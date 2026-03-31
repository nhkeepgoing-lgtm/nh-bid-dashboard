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

HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept-Language": "ko-KR,ko;q=0.9",
}

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
            y = m.group(1)
            mo = m.group(2).zfill(2)
            d = m.group(3).zfill(2)
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


def parse_table_static(html, source_id, source_name, base_url, list_url,
                        num_col=0, title_col=1, date_col=2) -> list:
    """정적 HTML 테이블 파싱 공통 함수"""
    skip_titles = ["번호", "제목", "작성일", "등록일", "첨부파일", "조회", "조회수", "파일"]
    items = []
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table")
    if not table:
        return []

    rows = table.find_all("tr")
    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 2:
            continue

        # 번호가 숫자인지 확인
        if len(cells) > num_col:
            num_text = cells[num_col].get_text(strip=True)
            if not num_text.isdigit():
                continue

        # 제목
        if len(cells) <= title_col:
            continue
        title_cell = cells[title_col]
        anchor = title_cell.find("a")
        title = anchor.get_text(strip=True) if anchor else title_cell.get_text(strip=True)
        title = title.replace("새글 표시 아이콘", "").replace("새글", "").strip()
        if not title or len(title) < 3 or title in skip_titles:
            continue

        # 날짜
        date_idx = date_col if date_col >= 0 else len(cells) - 1
        date_str = None
        if len(cells) > date_idx:
            date_str = normalize_date(cells[date_idx].get_text(strip=True))

        # 링크
        link = list_url
        if anchor:
            href = anchor.get("href", "")
            if href and href not in ["#", "javascript:void(0)"] and "javascript" not in href:
                link = href if href.startswith("http") else base_url + href

        items.append(make_bid(
            source_id=source_id,
            source_name=source_name,
            title=title,
            date=date_str,
            link=link,
        ))

        if len(items) >= MAX_ITEMS:
            break

    return items


# ─────────────────────────────────────────────
# 1. 범농협 통합구매 (nonghyup.com)
# URL: https://www.nonghyup.com/ecenter/bid/bidList.do
# ─────────────────────────────────────────────
class NonghyupBidCrawler:
    source_id = "nonghyup_bid"
    source_name = "범농협 통합구매"
    url = "https://www.nonghyup.com/ecenter/bid/bidList.do"
    base_url = "https://www.nonghyup.com"

    def crawl_static(self) -> list:
        try:
            resp = requests.get(self.url, headers=HEADERS, timeout=20)
            resp.encoding = "utf-8"
            items = parse_table_static(
                html=resp.text,
                source_id=self.source_id,
                source_name=self.source_name,
                base_url=self.base_url,
                list_url=self.url,
                num_col=0, title_col=1, date_col=3,
            )
            logger.info(f"[nonghyup_bid] 수집 완료: {len(items)}건")
            return items
        except Exception as e:
            logger.error(f"[nonghyup_bid] 오류: {e}")
            return []


# ─────────────────────────────────────────────
# 2. NH농협금융지주 (nhfngroup.com)
# URL: https://www.nhfngroup.com/user/indexSub.do?codyMenuSeq=1102093359&siteId=nhfngroup
# ─────────────────────────────────────────────
class NHFnGroupCrawler:
    source_id = "nhfngroup"
    source_name = "NH농협금융지주"
    url = "https://www.nhfngroup.com/user/indexSub.do?codyMenuSeq=1102093359&siteId=nhfngroup"
    base_url = "https://www.nhfngroup.com"

    def crawl_static(self) -> list:
        try:
            resp = requests.get(self.url, headers=HEADERS, timeout=20)
            resp.encoding = "UTF-8"
            items = parse_table_static(
                html=resp.text,
                source_id=self.source_id,
                source_name=self.source_name,
                base_url=self.base_url,
                list_url=self.url,
                num_col=0, title_col=1, date_col=2,
            )
            logger.info(f"[nhfngroup] 수집 완료: {len(items)}건")
            return items
        except Exception as e:
            logger.error(f"[nhfngroup] 오류: {e}")
            return []


# ─────────────────────────────────────────────
# 3. NH농협경제지주 (nhabgroup.com)
# URL: http://www.nhabgroup.com/user/indexSub.do?codyMenuSeq=602309274&siteId=nhabgroup
# ─────────────────────────────────────────────
class NHAbGroupCrawler:
    source_id = "nhabgroup"
    source_name = "NH농협경제지주"
    url = "http://www.nhabgroup.com/user/indexSub.do?codyMenuSeq=602309274&siteId=nhabgroup"
    base_url = "http://www.nhabgroup.com"

    def crawl_static(self) -> list:
        try:
            resp = requests.get(self.url, headers=HEADERS, timeout=20)
            resp.encoding = "UTF-8"
            items = parse_table_static(
                html=resp.text,
                source_id=self.source_id,
                source_name=self.source_name,
                base_url=self.base_url,
                list_url=self.url,
                num_col=0, title_col=1, date_col=2,
            )
            logger.info(f"[nhabgroup] 수집 완료: {len(items)}건")
            return items
        except Exception as e:
            logger.error(f"[nhabgroup] 오류: {e}")
            return []


# ─────────────────────────────────────────────
# 4. NH농협캐피탈 (nhcapital.co.kr)
# URL: https://www.nhcapital.co.kr/customer/customer/customerManageposts/selectManagepostsNoticeList.nh
# 공지사항에서 입찰공고 키워드 필터링
# ─────────────────────────────────────────────
class NHCapitalCrawler:
    source_id = "nhcapital"
    source_name = "NH농협캐피탈"
    url = "https://www.nhcapital.co.kr/customer/customer/customerManageposts/selectManagepostsNoticeList.nh"
    base_url = "https://www.nhcapital.co.kr"

    def crawl_static(self) -> list:
        items = []
        try:
            resp = requests.get(self.url, headers=HEADERS, timeout=20)
            resp.encoding = "utf-8"
            soup = BeautifulSoup(resp.text, "lxml")
            table = soup.find("table")
            if not table:
                logger.warning("[nhcapital] 테이블 없음")
                return []

            rows = table.find_all("tr")
            for row in rows:
                cells = row.find_all("td")
                if len(cells) < 4:
                    continue

                num_text = cells[0].get_text(strip=True)
                if not num_text.isdigit():
                    continue

                title_cell = cells[1]
                anchor = title_cell.find("a")
                title = anchor.get_text(strip=True) if anchor else title_cell.get_text(strip=True)
                title = title.replace("신규", "").replace("첨부파일", "").strip()
                if not title or len(title) < 3:
                    continue

                # 입찰공고 키워드 필터
                if not any(kw in title for kw in ["입찰", "공고", "용역", "구매", "선정", "조달", "매각"]):
                    continue

                date_str = normalize_date(cells[3].get_text(strip=True))

                link = self.url
                if anchor:
                    href = anchor.get("href", "")
                    if href and "javascript" not in href and href != "#":
                        link = href if href.startswith("http") else self.base_url + href

                items.append(make_bid(
                    source_id=self.source_id,
                    source_name=self.source_name,
                    title=title, date=date_str, link=link,
                ))

                if len(items) >= MAX_ITEMS:
                    break

        except Exception as e:
            logger.error(f"[nhcapital] 오류: {e}")

        logger.info(f"[nhcapital] 수집 완료: {len(items)}건")
        return items


# ─────────────────────────────────────────────
# 5. NH농협생명 (nhlife.co.kr) - Playwright
# URL: https://www.nhlife.co.kr/ho/ci/HOCI0008M00.nhl
# ─────────────────────────────────────────────
class NHLifeCrawler:
    source_id = "nhlife"
    source_name = "NH농협생명"
    url = "https://www.nhlife.co.kr/ho/ci/HOCI0008M00.nhl"

    async def crawl(self, page) -> list:
        items = []
        try:
            await page.goto(self.url, wait_until="networkidle", timeout=BROWSER_TIMEOUT)
            await page.wait_for_timeout(PAGE_WAIT)

            rows = await page.query_selector_all("table tbody tr")
            logger.info(f"[nhlife] 발견된 행: {len(rows)}")

            for row in rows:
                try:
                    text = await row.inner_text()
                    if not any(kw in text for kw in ["입찰", "공고", "조달", "구매", "용역", "컨설팅"]):
                        continue

                    cells = await row.query_selector_all("td")
                    if len(cells) < 2:
                        continue

                    title_el = await cells[1].query_selector("a")
                    if not title_el:
                        title_el = cells[1]
                    title = (await title_el.inner_text()).strip()
                    if not title or len(title) < 3:
                        continue

                    date_str = normalize_date(await cells[-1].inner_text())

                    link = self.url
                    anchor = await cells[1].query_selector("a")
                    if anchor:
                        href = await anchor.get_attribute("href")
                        if href and "javascript" not in href:
                            link = href if href.startswith("http") else f"https://www.nhlife.co.kr{href}"

                    items.append(make_bid(
                        source_id=self.source_id,
                        source_name=self.source_name,
                        title=title, date=date_str, link=link
                    ))

                    if len(items) >= MAX_ITEMS:
                        break
                except Exception as e:
                    logger.debug(f"[nhlife] 행 파싱 오류: {e}")

        except Exception as e:
            logger.error(f"[nhlife] 오류: {e}")

        return items


# ─────────────────────────────────────────────
# 6. NH농협손해보험 (nhfire.co.kr) - Playwright
# URL: https://www.nhfire.co.kr/company/bbs/tenderList.nhfire
# ─────────────────────────────────────────────
class NHFireCrawler:
    source_id = "nhfire"
    source_name = "NH농협손해보험"
    url = "https://www.nhfire.co.kr/company/bbs/tenderList.nhfire"

    async def crawl(self, page) -> list:
        items = []
        try:
            await page.goto(self.url, wait_until="networkidle", timeout=BROWSER_TIMEOUT)
            await page.wait_for_timeout(PAGE_WAIT)

            rows = await page.query_selector_all("table tbody tr")
            logger.info(f"[nhfire] 발견된 행: {len(rows)}")

            for row in rows[:MAX_ITEMS]:
                try:
                    cells = await row.query_selector_all("td")
                    if len(cells) < 2:
                        continue

                    title_el = await cells[1].query_selector("a")
                    if not title_el:
                        title_el = cells[1]
                    title = (await title_el.inner_text()).strip()

                    if not title or len(title) < 3:
                        continue
                    if title in ["제목", "번호", "첨부파일", "등록일", "조회수"]:
                        continue

                    date_str = normalize_date(await cells[-1].inner_text())

                    link = self.url
                    anchor = await cells[1].query_selector("a")
                    if anchor:
                        href = await anchor.get_attribute("href")
                        if href and href != "#" and "javascript" not in href:
                            link = href if href.startswith("http") else f"https://www.nhfire.co.kr{href}"

                    items.append(make_bid(
                        source_id=self.source_id,
                        source_name=self.source_name,
                        title=title, date=date_str, link=link
                    ))
                except Exception as e:
                    logger.debug(f"[nhfire] 행 파싱 오류: {e}")

        except Exception as e:
            logger.error(f"[nhfire] 오류: {e}")

        return items


# ─────────────────────────────────────────────
# 메인 실행
# ─────────────────────────────────────────────
async def run_all():
    all_items = []
    results_meta = {}

    # ── 1) requests 정적 파싱 (빠름) ──
    static_crawlers = [
        NonghyupBidCrawler(),   # 범농협 통합구매
        NHFnGroupCrawler(),     # NH농협금융지주
        NHAbGroupCrawler(),     # NH농협경제지주
        NHCapitalCrawler(),     # NH농협캐피탈
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

    # ── 2) Playwright 브라우저 필요 ──
    playwright_crawlers = [
        NHLifeCrawler(),    # NH농협생명
        NHFireCrawler(),    # NH농협손해보험
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

    # Slack 알림 (선택사항)
    slack_url = os.environ.get("SLACK_WEBHOOK")
    if slack_url:
        new_items = [i for i in all_items if i.get("is_new")]
        if new_items:
            try:
                text = f"*NH농협 신규 입찰공고 {len(new_items)}건*\n"
                for item in new_items[:10]:
                    text += f"• [{item['source_name']}] {item['title']} `{item.get('date', '')}`\n"
                requests.post(slack_url, json={"text": text}, timeout=10)
            except Exception as e:
                logger.error(f"Slack 알림 실패: {e}")

    return output


if __name__ == "__main__":
    asyncio.run(run_all())
