#!/usr/bin/env python3
"""
NH농협 그룹 입찰공고 통합 크롤러
각 계열사 사이트를 크롤링하여 data/bids.json에 저장합니다.
"""

import asyncio
import json
import os
import re
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

# ─────────────────────────────────────────────
# 설정
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("nh-crawler")

DATA_DIR = Path(__file__).parent.parent / "data"
DATA_DIR.mkdir(exist_ok=True)
OUTPUT_PATH = DATA_DIR / "bids.json"

BROWSER_TIMEOUT = 30_000
PAGE_WAIT = 3_000
MAX_ITEMS = 30

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# ─────────────────────────────────────────────
# 유틸리티
# ─────────────────────────────────────────────
def normalize_date(raw: str) -> Optional[str]:
    if not raw:
        return None
    raw = raw.strip()
    patterns = [
        r"(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})",
        r"(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일",
    ]
    for p in patterns:
        m = re.search(p, raw)
        if m:
            y, mo, d = m.group(1), m.group(2).zfill(2), m.group(3).zfill(2)
            return f"{y}-{mo}-{d}"
    return None


def is_new(date_str: Optional[str], days: int = 7) -> bool:
    if not date_str:
        return False
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return (datetime.now() - dt).days <= days
    except ValueError:
        return False


def make_bid(source_id, source_name, title, date=None, deadline=None,
             link=None, category="입찰공고", note=None):
    return {
        "id": f"{source_id}_{hash(title + str(date)) & 0xFFFFFF:06x}",
        "source_id": source_id,
        "source_name": source_name,
        "title": title.strip(),
        "date": date,
        "deadline": deadline,
        "link": link,
        "category": category,
        "note": note,
        "is_new": is_new(date),
        "crawled_at": datetime.now().isoformat(),
    }


# ─────────────────────────────────────────────
# 1. 범농협 통합입찰 (nonghyup.com) ★ 핵심
# URL: https://www.nonghyup.com/ecenter/bid/bidList.do
# requests로 직접 파싱 가능 (JS 렌더링 불필요)
# ─────────────────────────────────────────────
class NonghyupBidCrawler:
    source_id = "nonghyup_bid"
    source_name = "범농협 통합입찰"
    url = "https://www.nonghyup.com/ecenter/bid/bidList.do"

    def crawl_static(self) -> list[dict]:
        """requests로 직접 HTML 파싱 - JS 렌더링 불필요"""
        items = []
        try:
            headers = {
                "User-Agent": USER_AGENT,
                "Accept-Language": "ko-KR,ko;q=0.9",
                "Referer": "https://www.nonghyup.com/",
            }
            resp = requests.get(self.url, headers=headers, timeout=20)
            resp.encoding = "utf-8"
            soup = BeautifulSoup(resp.text, "lxml")

            # 입찰공고 테이블 찾기
            # <table> 안의 tbody > tr 구조
            table = soup.find("table")
            if not table:
                logger.warning("[nonghyup_bid] 테이블을 찾을 수 없음")
                return []

            rows = table.find_all("tr")
            logger.info(f"[nonghyup_bid] 발견된 행: {len(rows)}")

            for row in rows:
                cells = row.find_all("td")
                if len(cells) < 4:
                    continue

                # 번호 셀이 숫자인지 확인 (헤더 행 제외)
                num_text = cells[0].get_text(strip=True)
                if not num_text.isdigit():
                    continue

                # 제목
                title_cell = cells[1]
                title_anchor = title_cell.find("a")
                title = title_anchor.get_text(strip=True) if title_anchor else title_cell.get_text(strip=True)
                # "새글" 텍스트 제거
                title = title.replace("새글", "").strip()
                if not title:
                    continue

                # 등록일 (4번째 셀)
                date_str = normalize_date(cells[3].get_text(strip=True))

                # 링크 구성
                # 클릭 이벤트 방식이라 직접 링크가 없음 → 목록 페이지 링크 사용
                link = self.url

                items.append(make_bid(
                    source_id=self.source_id,
                    source_name=self.source_name,
                    title=title,
                    date=date_str,
                    link=link,
                ))

                if len(items) >= MAX_ITEMS:
                    break

        except Exception as e:
            logger.error(f"[nonghyup_bid] 오류: {e}")

        logger.info(f"[nonghyup_bid] 수집 완료: {len(items)}건")
        return items


# ─────────────────────────────────────────────
# 2. NH농협생명 크롤러
# ─────────────────────────────────────────────
class NHLifeCrawler:
    source_id = "nhlife"
    source_name = "NH농협생명"
    url = "https://www.nhlife.co.kr/ho/ci/HOCI0008M00.nhl"

    async def crawl(self, page) -> list[dict]:
        items = []
        try:
            await page.goto(self.url, wait_until="networkidle", timeout=BROWSER_TIMEOUT)
            await page.wait_for_timeout(PAGE_WAIT)

            rows = await page.query_selector_all("table tbody tr")
            logger.info(f"[nhlife] 발견된 행: {len(rows)}")

            for row in rows[:MAX_ITEMS]:
                try:
                    text = await row.inner_text()
                    if not any(kw in text for kw in ["입찰", "공고", "조달", "구매", "용역", "컨설팅"]):
                        continue

                    title_el = await row.query_selector("td:nth-child(2) a, td:nth-child(2)")
                    if not title_el:
                        continue
                    title = (await title_el.inner_text()).strip()
                    if not title or len(title) < 3:
                        continue

                    date_el = await row.query_selector("td:last-child")
                    date_str = normalize_date(await date_el.inner_text()) if date_el else None

                    link = None
                    anchor = await row.query_selector("a")
                    if anchor:
                        href = await anchor.get_attribute("href")
                        if href:
                            link = href if href.startswith("http") else f"https://www.nhlife.co.kr{href}"

                    items.append(make_bid(
                        source_id=self.source_id,
                        source_name=self.source_name,
                        title=title, date=date_str, link=link
                    ))
                except Exception as e:
                    logger.debug(f"[nhlife] 행 파싱 오류: {e}")

        except Exception as e:
            logger.error(f"[nhlife] 오류: {e}")

        return items[:MAX_ITEMS]


# ─────────────────────────────────────────────
# 3. NH농협손해보험 크롤러
# URL: https://www.nhfire.co.kr/company/bbs/tenderList.nhfire
# ─────────────────────────────────────────────
class NHFireCrawler:
    source_id = "nhfire"
    source_name = "NH농협손해보험"
    url = "https://www.nhfire.co.kr/company/bbs/tenderList.nhfire"

    async def crawl(self, page) -> list[dict]:
        items = []
        try:
            await page.goto(self.url, wait_until="networkidle", timeout=BROWSER_TIMEOUT)
            await page.wait_for_timeout(PAGE_WAIT)

            # 정확한 테이블 선택: 번호/제목/첨부/등록일 구조
            rows = await page.query_selector_all("table tbody tr")
            logger.info(f"[nhfire] 발견된 행: {len(rows)}")

            for row in rows[:MAX_ITEMS]:
                try:
                    cells = await row.query_selector_all("td")
                    if len(cells) < 3:
                        continue

                    # 첫 번째 셀이 숫자(번호)인지 확인
                    num_text = (await cells[0].inner_text()).strip()
                    if not num_text.isdigit():
                        continue

                    # 제목 (두 번째 셀)
                    title_el = await cells[1].query_selector("a")
                    if not title_el:
                        title_el = cells[1]
                    title = (await title_el.inner_text()).strip()
                    if not title or len(title) < 3:
                        continue

                    # 등록일 (마지막 셀)
                    date_el = cells[-1]
                    date_str = normalize_date(await date_el.inner_text())

                    # 링크
                    link = None
                    anchor = await cells[1].query_selector("a")
                    if anchor:
                        href = await anchor.get_attribute("href")
                        if href and href != "#":
                            link = href if href.startswith("http") else f"https://www.nhfire.co.kr{href}"
                    if not link:
                        link = self.url

                    items.append(make_bid(
                        source_id=self.source_id,
                        source_name=self.source_name,
                        title=title, date=date_str, link=link
                    ))
                except Exception as e:
                    logger.debug(f"[nhfire] 행 파싱 오류: {e}")

        except Exception as e:
            logger.error(f"[nhfire] 오류: {e}")

        return items[:MAX_ITEMS]


# ─────────────────────────────────────────────
# 4. NH농협캐피탈 크롤러
# ─────────────────────────────────────────────
class NHCapitalCrawler:
    source_id = "nhcapital"
    source_name = "NH농협캐피탈"
    url = "https://www.nhcapital.co.kr"

    async def crawl(self, page) -> list[dict]:
        items = []
        try:
            candidate_urls = [
                "https://www.nhcapital.co.kr/introduce/notice/bid",
                "https://www.nhcapital.co.kr/introduce/notice",
                "https://www.nhcapital.co.kr",
            ]
            for candidate in candidate_urls:
                try:
                    await page.goto(candidate, wait_until="domcontentloaded", timeout=15_000)
                    await page.wait_for_timeout(2000)
                    content = await page.content()
                    if len(content) > 5000:
                        self.url = candidate
                        break
                except Exception:
                    continue

            await page.wait_for_timeout(PAGE_WAIT)
            rows = await page.query_selector_all("table tbody tr")
            logger.info(f"[nhcapital] 발견된 행: {len(rows)}")

            for row in rows[:MAX_ITEMS]:
                try:
                    cells = await row.query_selector_all("td")
                    if len(cells) < 2:
                        continue

                    num_text = (await cells[0].inner_text()).strip()
                    if not num_text.isdigit():
                        continue

                    title_el = await cells[1].query_selector("a") or cells[1]
                    title = (await title_el.inner_text()).strip()
                    if not title or len(title) < 3:
                        continue

                    date_str = normalize_date(await cells[-1].inner_text()) if cells else None

                    link = None
                    anchor = await cells[1].query_selector("a")
                    if anchor:
                        href = await anchor.get_attribute("href")
                        if href:
                            link = href if href.startswith("http") else f"https://www.nhcapital.co.kr{href}"

                    items.append(make_bid(
                        source_id=self.source_id,
                        source_name=self.source_name,
                        title=title, date=date_str, link=link
                    ))
                except Exception as e:
                    logger.debug(f"[nhcapital] 행 파싱 오류: {e}")

        except Exception as e:
            logger.error(f"[nhcapital] 오류: {e}")

        return items[:MAX_ITEMS]


# ─────────────────────────────────────────────
# 5. NH농협금융지주 크롤러
# ─────────────────────────────────────────────
class NHFnGroupCrawler:
    source_id = "nhfngroup"
    source_name = "NH농협금융지주"
    url = "https://www.nhfngroup.com"

    async def crawl(self, page) -> list[dict]:
        items = []
        try:
            candidate_urls = [
                "https://www.nhfngroup.com/bid/list.do",
                "https://www.nhfngroup.com/pr/bid/list.do",
                "https://www.nhfngroup.com",
            ]
            for candidate in candidate_urls:
                try:
                    await page.goto(candidate, wait_until="domcontentloaded", timeout=15_000)
                    await page.wait_for_timeout(2000)
                    content = await page.content()
                    if "입찰" in content or "공고" in content:
                        self.url = candidate
                        break
                except Exception:
                    continue

            await page.wait_for_timeout(PAGE_WAIT)
            rows = await page.query_selector_all("table tbody tr")
            logger.info(f"[nhfngroup] 발견된 행: {len(rows)}")

            for row in rows[:MAX_ITEMS]:
                try:
                    cells = await row.query_selector_all("td")
                    if len(cells) < 2:
                        continue

                    num_text = (await cells[0].inner_text()).strip()
                    if not num_text.isdigit():
                        continue

                    title_el = await cells[1].query_selector("a") or cells[1]
                    title = (await title_el.inner_text()).strip()
                    if not title or len(title) < 3:
                        continue

                    date_str = normalize_date(await cells[-1].inner_text()) if cells else None

                    link = None
                    anchor = await cells[1].query_selector("a")
                    if anchor:
                        href = await anchor.get_attribute("href")
                        if href:
                            link = href if href.startswith("http") else f"https://www.nhfngroup.com{href}"

                    items.append(make_bid(
                        source_id=self.source_id,
                        source_name=self.source_name,
                        title=title, date=date_str, link=link
                    ))
                except Exception as e:
                    logger.debug(f"[nhfngroup] 행 파싱 오류: {e}")

        except Exception as e:
            logger.error(f"[nhfngroup] 오류: {e}")

        return items[:MAX_ITEMS]


# ─────────────────────────────────────────────
# 6. NH농업지주 크롤러
# ─────────────────────────────────────────────
class NHAbGroupCrawler:
    source_id = "nhabgroup"
    source_name = "NH농업지주"
    url = "https://www.nhabgroup.com"

    async def crawl(self, page) -> list[dict]:
        items = []
        try:
            candidate_urls = [
                "https://www.nhabgroup.com/bid/list.do",
                "https://www.nhabgroup.com/pr/bid",
                "https://www.nhabgroup.com",
            ]
            for candidate in candidate_urls:
                try:
                    await page.goto(candidate, wait_until="domcontentloaded", timeout=15_000)
                    await page.wait_for_timeout(2000)
                    content = await page.content()
                    if len(content) > 3000:
                        self.url = candidate
                        break
                except Exception:
                    continue

            await page.wait_for_timeout(PAGE_WAIT)
            rows = await page.query_selector_all("table tbody tr")
            logger.info(f"[nhabgroup] 발견된 행: {len(rows)}")

            for row in rows[:MAX_ITEMS]:
                try:
                    cells = await row.query_selector_all("td")
                    if len(cells) < 2:
                        continue

                    num_text = (await cells[0].inner_text()).strip()
                    if not num_text.isdigit():
                        continue

                    title_el = await cells[1].query_selector("a") or cells[1]
                    title = (await title_el.inner_text()).strip()
                    if not title or len(title) < 3:
                        continue

                    date_str = normalize_date(await cells[-1].inner_text()) if cells else None

                    link = None
                    anchor = await cells[1].query_selector("a")
                    if anchor:
                        href = await anchor.get_attribute("href")
                        if href:
                            link = href if href.startswith("http") else f"https://www.nhabgroup.com{href}"

                    items.append(make_bid(
                        source_id=self.source_id,
                        source_name=self.source_name,
                        title=title, date=date_str, link=link
                    ))
                except Exception as e:
                    logger.debug(f"[nhabgroup] 행 파싱 오류: {e}")

        except Exception as e:
            logger.error(f"[nhabgroup] 오류: {e}")

        return items[:MAX_ITEMS]


# ─────────────────────────────────────────────
# 메인 실행
# ─────────────────────────────────────────────
async def run_all():
    all_items = []
    results_meta = {}

    # ── 1) requests로 처리 가능한 사이트 먼저 (빠름) ──
    static_crawlers = [
        NonghyupBidCrawler(),   # 범농협 통합입찰 ★
    ]

    for crawler in static_crawlers:
        logger.info(f"크롤링 시작 (정적): {crawler.source_name}")
        try:
            items = crawler.crawl_static()
            all_items.extend(items)
            results_meta[crawler.source_id] = {
                "name": crawler.source_name,
                "url": crawler.url,
                "count": len(items),
                "status": "ok" if items else "empty",
                "last_crawled": datetime.now().isoformat(),
            }
            logger.info(f"  → {len(items)}건 수집")
        except Exception as e:
            logger.error(f"  → 실패: {e}")
            results_meta[crawler.source_id] = {
                "name": crawler.source_name,
                "url": crawler.url,
                "count": 0,
                "status": "error",
                "error": str(e),
                "last_crawled": datetime.now().isoformat(),
            }

    # ── 2) Playwright 필요한 사이트 ──
    playwright_crawlers = [
        NHLifeCrawler(),
        NHFireCrawler(),
        NHCapitalCrawler(),
        NHFnGroupCrawler(),
        NHAbGroupCrawler(),
    ]

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
            ],
        )

        for crawler in playwright_crawlers:
            logger.info(f"크롤링 시작 (브라우저): {crawler.source_name}")
            page = await browser.new_page(
                user_agent=USER_AGENT,
                extra_http_headers={"Accept-Language": "ko-KR,ko;q=0.9"},
            )
            try:
                items = await crawler.crawl(page)
                all_items.extend(items)
                results_meta[crawler.source_id] = {
                    "name": crawler.source_name,
                    "url": crawler.url,
                    "count": len(items),
                    "status": "ok" if items else "empty",
                    "last_crawled": datetime.now().isoformat(),
                }
                logger.info(f"  → {len(items)}건 수집")
            except Exception as e:
                logger.error(f"  → 실패: {e}")
                results_meta[crawler.source_id] = {
                    "name": crawler.source_name,
                    "url": crawler.url,
                    "count": 0,
                    "status": "error",
                    "error": str(e),
                    "last_crawled": datetime.now().isoformat(),
                }
            finally:
                await page.close()

        await browser.close()

    # 날짜 내림차순 정렬
    all_items.sort(key=lambda x: x.get("date") or "0000-00-00", reverse=True)

    output = {
        "generated_at": datetime.now().isoformat(),
        "total": len(all_items),
        "sources": results_meta,
        "items": all_items,
    }

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    logger.info(f"\n✅ 크롤링 완료: 총 {len(all_items)}건 → {OUTPUT_PATH}")

    # Slack 알림
    slack_url = os.environ.get("SLACK_WEBHOOK")
    if slack_url:
        new_items = [i for i in all_items if i.get("is_new")]
        if new_items:
            try:
                text = f"*NH농협 신규 입찰공고 {len(new_items)}건*\n"
                for item in new_items[:10]:
                    text += f"• [{item['source_name']}] {item['title']} `{item.get('date','')}`\n"
                requests.post(slack_url, json={"text": text}, timeout=10)
            except Exception as e:
                logger.error(f"Slack 알림 실패: {e}")

    return output


if __name__ == "__main__":
    asyncio.run(run_all())
