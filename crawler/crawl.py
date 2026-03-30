#!/usr/bin/env python3
"""
NH농협 그룹 입찰공고 통합 크롤러
각 계열사 사이트를 Playwright(헤드리스 브라우저)로 크롤링하여
data/bids.json에 저장합니다.
"""

import asyncio
import json
import os
import re
import sys
import logging
from datetime import datetime, timedelta
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

BROWSER_TIMEOUT = 30_000   # ms
PAGE_WAIT = 3_000          # ms (동적 로딩 대기)
MAX_ITEMS = 30             # 사이트당 최대 수집 건수

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# ─────────────────────────────────────────────
# 유틸리티
# ─────────────────────────────────────────────
def normalize_date(raw: str) -> Optional[str]:
    """다양한 날짜 형식 → YYYY-MM-DD"""
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


def make_bid(
    source_id: str,
    source_name: str,
    title: str,
    date: Optional[str] = None,
    deadline: Optional[str] = None,
    link: Optional[str] = None,
    category: str = "입찰공고",
    note: Optional[str] = None,
) -> dict:
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
# 크롤러 베이스
# ─────────────────────────────────────────────
class BaseCrawler:
    source_id: str = ""
    source_name: str = ""
    url: str = ""

    async def crawl(self, page) -> list[dict]:
        raise NotImplementedError

    def make(self, **kwargs) -> dict:
        return make_bid(
            source_id=self.source_id,
            source_name=self.source_name,
            **kwargs,
        )


# ─────────────────────────────────────────────
# NH농협생명 크롤러
# 공지사항 목록에서 입찰공고 필터링
# URL: https://www.nhlife.co.kr/ho/ci/HOCI0008M00.nhl
# ─────────────────────────────────────────────
class NHLifeCrawler(BaseCrawler):
    source_id = "nhlife"
    source_name = "NH농협생명"
    url = "https://www.nhlife.co.kr/ho/ci/HOCI0008M00.nhl"

    async def crawl(self, page) -> list[dict]:
        items = []
        try:
            await page.goto(self.url, wait_until="networkidle", timeout=BROWSER_TIMEOUT)
            await page.wait_for_timeout(PAGE_WAIT)

            # 목록 행 파싱
            rows = await page.query_selector_all(".board_list li, .notice_list li, table tbody tr")
            logger.info(f"[nhlife] 발견된 행: {len(rows)}")

            for row in rows[:MAX_ITEMS]:
                try:
                    text = await row.inner_text()
                    # 입찰 키워드 필터
                    if not any(kw in text for kw in ["입찰", "공고", "조달", "구매", "용역", "컨설팅"]):
                        continue

                    # 제목
                    title_el = await row.query_selector("a, .title, td:nth-child(2)")
                    if not title_el:
                        continue
                    title = await title_el.inner_text()
                    title = title.strip()
                    if not title or len(title) < 3:
                        continue

                    # 날짜
                    date_el = await row.query_selector(".date, td:last-child, td:nth-child(3)")
                    date_str = None
                    if date_el:
                        date_str = normalize_date(await date_el.inner_text())

                    # 링크
                    link = None
                    anchor = await row.query_selector("a")
                    if anchor:
                        href = await anchor.get_attribute("href")
                        if href:
                            link = href if href.startswith("http") else f"https://www.nhlife.co.kr{href}"

                    items.append(self.make(title=title, date=date_str, link=link))
                except Exception as e:
                    logger.debug(f"[nhlife] 행 파싱 오류: {e}")
                    continue

        except PlaywrightTimeout:
            logger.warning(f"[nhlife] 타임아웃")
        except Exception as e:
            logger.error(f"[nhlife] 오류: {e}")

        return items[:MAX_ITEMS]


# ─────────────────────────────────────────────
# NH농협손해보험 크롤러
# URL: https://www.nhfire.co.kr/company/bbs/tenderList.nhfire
# ─────────────────────────────────────────────
class NHFireCrawler(BaseCrawler):
    source_id = "nhfire"
    source_name = "NH농협손해보험"
    url = "https://www.nhfire.co.kr/company/bbs/tenderList.nhfire"

    async def crawl(self, page) -> list[dict]:
        items = []
        try:
            await page.goto(self.url, wait_until="networkidle", timeout=BROWSER_TIMEOUT)
            await page.wait_for_timeout(PAGE_WAIT)

            rows = await page.query_selector_all("table tbody tr, .board_list li")
            logger.info(f"[nhfire] 발견된 행: {len(rows)}")

            for row in rows[:MAX_ITEMS]:
                try:
                    cells = await row.query_selector_all("td")
                    if len(cells) < 2:
                        continue

                    title_el = await row.query_selector("td:nth-child(2) a, td:nth-child(2)")
                    if not title_el:
                        continue
                    title = (await title_el.inner_text()).strip()
                    if not title or title in ["제목", "번호"]:
                        continue

                    date_el = await row.query_selector("td:last-child")
                    date_str = normalize_date(await date_el.inner_text()) if date_el else None

                    link = None
                    anchor = await row.query_selector("a")
                    if anchor:
                        href = await anchor.get_attribute("href")
                        if href:
                            link = href if href.startswith("http") else f"https://www.nhfire.co.kr{href}"

                    items.append(self.make(title=title, date=date_str, link=link))
                except Exception as e:
                    logger.debug(f"[nhfire] 행 파싱 오류: {e}")

        except PlaywrightTimeout:
            logger.warning(f"[nhfire] 타임아웃")
        except Exception as e:
            logger.error(f"[nhfire] 오류: {e}")

        return items[:MAX_ITEMS]


# ─────────────────────────────────────────────
# NH농협캐피탈 크롤러
# ─────────────────────────────────────────────
class NHCapitalCrawler(BaseCrawler):
    source_id = "nhcapital"
    source_name = "NH농협캐피탈"
    url = "https://www.nhcapital.co.kr/introduce/notice/bid"

    async def crawl(self, page) -> list[dict]:
        items = []
        try:
            # 공지사항 또는 입찰공고 페이지 탐색 시도
            candidate_urls = [
                "https://www.nhcapital.co.kr/introduce/notice/bid",
                "https://www.nhcapital.co.kr/introduce/notice",
                "https://www.nhcapital.co.kr/board/notice",
            ]
            loaded = False
            for candidate in candidate_urls:
                try:
                    await page.goto(candidate, wait_until="domcontentloaded", timeout=15_000)
                    await page.wait_for_timeout(2000)
                    content = await page.content()
                    if len(content) > 5000:
                        self.url = candidate
                        loaded = True
                        break
                except Exception:
                    continue

            if not loaded:
                logger.warning("[nhcapital] 페이지 로드 실패")
                return []

            await page.wait_for_timeout(PAGE_WAIT)
            rows = await page.query_selector_all("table tbody tr, .board-list li, .list-item")
            logger.info(f"[nhcapital] 발견된 행: {len(rows)}")

            for row in rows[:MAX_ITEMS]:
                try:
                    title_el = await row.query_selector("a, .title, td:nth-child(2)")
                    if not title_el:
                        continue
                    title = (await title_el.inner_text()).strip()
                    if not title or len(title) < 3:
                        continue

                    date_el = await row.query_selector("td:last-child, .date")
                    date_str = normalize_date(await date_el.inner_text()) if date_el else None

                    link = None
                    anchor = await row.query_selector("a")
                    if anchor:
                        href = await anchor.get_attribute("href")
                        if href:
                            link = href if href.startswith("http") else f"https://www.nhcapital.co.kr{href}"

                    items.append(self.make(title=title, date=date_str, link=link))
                except Exception as e:
                    logger.debug(f"[nhcapital] 행 파싱 오류: {e}")

        except Exception as e:
            logger.error(f"[nhcapital] 오류: {e}")

        return items[:MAX_ITEMS]


# ─────────────────────────────────────────────
# 범농협 통합구매 FIRSTePro 크롤러
# ─────────────────────────────────────────────
class FirstEproCrawler(BaseCrawler):
    source_id = "firstepro"
    source_name = "범농협 통합구매 FIRSTePro"
    url = "https://www.first-epro.com/bid/notice/list"

    async def crawl(self, page) -> list[dict]:
        items = []
        try:
            candidate_urls = [
                "https://www.first-epro.com/bid/notice/list",
                "https://www.first-epro.com/notice/list",
                "https://www.first-epro.com",
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
            rows = await page.query_selector_all("table tbody tr, .bid-list li, .notice-list li")
            logger.info(f"[firstepro] 발견된 행: {len(rows)}")

            for row in rows[:MAX_ITEMS]:
                try:
                    title_el = await row.query_selector("a, .title, td:nth-child(2)")
                    if not title_el:
                        continue
                    title = (await title_el.inner_text()).strip()
                    if not title or len(title) < 3:
                        continue

                    date_el = await row.query_selector("td:last-child, .date, .reg-date")
                    date_str = normalize_date(await date_el.inner_text()) if date_el else None

                    deadline_el = await row.query_selector(".deadline, td:nth-child(4)")
                    deadline_str = normalize_date(await deadline_el.inner_text()) if deadline_el else None

                    link = None
                    anchor = await row.query_selector("a")
                    if anchor:
                        href = await anchor.get_attribute("href")
                        if href:
                            link = href if href.startswith("http") else f"https://www.first-epro.com{href}"

                    items.append(self.make(title=title, date=date_str, deadline=deadline_str, link=link))
                except Exception as e:
                    logger.debug(f"[firstepro] 행 파싱 오류: {e}")

        except Exception as e:
            logger.error(f"[firstepro] 오류: {e}")

        return items[:MAX_ITEMS]


# ─────────────────────────────────────────────
# NH농협금융지주 크롤러 (requests 사용 가능 시도)
# ─────────────────────────────────────────────
class NHFnGroupCrawler(BaseCrawler):
    source_id = "nhfngroup"
    source_name = "NH농협금융지주"
    url = "https://www.nhfngroup.com/bid/list.do"

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

            # 입찰공고 링크 찾기
            links = await page.query_selector_all("a")
            bid_links = []
            for link_el in links:
                text = await link_el.inner_text()
                href = await link_el.get_attribute("href") or ""
                if "입찰" in text or "bid" in href.lower() or "공고" in text:
                    bid_links.append((text.strip(), href))

            # 클릭하여 목록 페이지 이동
            for text, href in bid_links[:3]:
                try:
                    full_url = href if href.startswith("http") else f"https://www.nhfngroup.com{href}"
                    await page.goto(full_url, wait_until="domcontentloaded", timeout=15_000)
                    await page.wait_for_timeout(PAGE_WAIT)
                    break
                except Exception:
                    continue

            rows = await page.query_selector_all("table tbody tr, .board-list li")
            logger.info(f"[nhfngroup] 발견된 행: {len(rows)}")

            for row in rows[:MAX_ITEMS]:
                try:
                    title_el = await row.query_selector("a, .subject, td:nth-child(2)")
                    if not title_el:
                        continue
                    title = (await title_el.inner_text()).strip()
                    if not title or len(title) < 3:
                        continue

                    date_el = await row.query_selector("td:last-child, .date")
                    date_str = normalize_date(await date_el.inner_text()) if date_el else None

                    link = None
                    anchor = await row.query_selector("a")
                    if anchor:
                        href = await anchor.get_attribute("href")
                        if href:
                            link = href if href.startswith("http") else f"https://www.nhfngroup.com{href}"

                    items.append(self.make(title=title, date=date_str, link=link))
                except Exception as e:
                    logger.debug(f"[nhfngroup] 행 파싱 오류: {e}")

        except Exception as e:
            logger.error(f"[nhfngroup] 오류: {e}")

        return items[:MAX_ITEMS]


# ─────────────────────────────────────────────
# 농협몰 크롤러 (nonghyup.com)
# ─────────────────────────────────────────────
class NonghyupMallCrawler(BaseCrawler):
    source_id = "nonghyup"
    source_name = "농협몰"
    url = "https://www.nonghyup.com"

    async def crawl(self, page) -> list[dict]:
        items = []
        try:
            candidate_urls = [
                "https://www.nonghyupmall.com/pr/bid/list.do",
                "https://www.nonghyup.com/pr/bid",
                "https://www.nonghyup.com",
            ]
            loaded = False
            for candidate in candidate_urls:
                try:
                    await page.goto(candidate, wait_until="domcontentloaded", timeout=15_000)
                    await page.wait_for_timeout(2000)
                    content = await page.content()
                    if len(content) > 3000:
                        self.url = candidate
                        loaded = True
                        break
                except Exception:
                    continue

            if not loaded:
                return []

            await page.wait_for_timeout(PAGE_WAIT)
            rows = await page.query_selector_all("table tbody tr, .board-list li")
            logger.info(f"[nonghyup] 발견된 행: {len(rows)}")

            for row in rows[:MAX_ITEMS]:
                try:
                    title_el = await row.query_selector("a, .subject, td:nth-child(2)")
                    if not title_el:
                        continue
                    title = (await title_el.inner_text()).strip()
                    if not title or len(title) < 3:
                        continue

                    date_el = await row.query_selector("td:last-child, .date")
                    date_str = normalize_date(await date_el.inner_text()) if date_el else None

                    link = None
                    anchor = await row.query_selector("a")
                    if anchor:
                        href = await anchor.get_attribute("href")
                        if href:
                            link = href if href.startswith("http") else f"https://www.nonghyup.com{href}"

                    items.append(self.make(title=title, date=date_str, link=link))
                except Exception as e:
                    logger.debug(f"[nonghyup] 행 파싱 오류: {e}")

        except Exception as e:
            logger.error(f"[nonghyup] 오류: {e}")

        return items[:MAX_ITEMS]


# ─────────────────────────────────────────────
# NH농업지주 크롤러 (nhabgroup.com)
# ─────────────────────────────────────────────
class NHAbGroupCrawler(BaseCrawler):
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
            rows = await page.query_selector_all("table tbody tr, .board-list li")
            logger.info(f"[nhabgroup] 발견된 행: {len(rows)}")

            for row in rows[:MAX_ITEMS]:
                try:
                    title_el = await row.query_selector("a, .subject, td:nth-child(2)")
                    if not title_el:
                        continue
                    title = (await title_el.inner_text()).strip()
                    if not title or len(title) < 3:
                        continue

                    date_el = await row.query_selector("td:last-child, .date")
                    date_str = normalize_date(await date_el.inner_text()) if date_el else None

                    link = None
                    anchor = await row.query_selector("a")
                    if anchor:
                        href = await anchor.get_attribute("href")
                        if href:
                            link = href if href.startswith("http") else f"https://www.nhabgroup.com{href}"

                    items.append(self.make(title=title, date=date_str, link=link))
                except Exception as e:
                    logger.debug(f"[nhabgroup] 행 파싱 오류: {e}")

        except Exception as e:
            logger.error(f"[nhabgroup] 오류: {e}")

        return items[:MAX_ITEMS]


# ─────────────────────────────────────────────
# 메인 실행
# ─────────────────────────────────────────────
async def run_all():
    crawlers = [
        NHLifeCrawler(),
        NHFireCrawler(),
        NHCapitalCrawler(),
        FirstEproCrawler(),
        NHFnGroupCrawler(),
        NonghyupMallCrawler(),
        NHAbGroupCrawler(),
    ]

    all_items = []
    results_meta = {}

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

        for crawler in crawlers:
            logger.info(f"크롤링 시작: {crawler.source_name} ({crawler.url})")
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

    # 신규 공고 슬랙 알림 (SLACK_WEBHOOK 환경변수 설정 시)
    slack_url = os.environ.get("SLACK_WEBHOOK")
    if slack_url:
        await send_slack_notification(slack_url, all_items, results_meta)

    return output


async def send_slack_notification(webhook_url: str, items: list, meta: dict):
    """신규 공고(7일 이내)를 Slack으로 알림"""
    try:
        new_items = [i for i in items if i.get("is_new")]
        if not new_items:
            return

        text = f"*NH농협 신규 입찰공고 {len(new_items)}건*\n"
        for item in new_items[:10]:
            link = item.get("link", "")
            title = item.get("title", "")
            date = item.get("date", "")
            src = item.get("source_name", "")
            text += f"• [{src}] <{link}|{title}> `{date}`\n"

        payload = {"text": text}
        resp = requests.post(webhook_url, json=payload, timeout=10)
        logger.info(f"Slack 알림 전송: {resp.status_code}")
    except Exception as e:
        logger.error(f"Slack 알림 실패: {e}")


if __name__ == "__main__":
    asyncio.run(run_all())
