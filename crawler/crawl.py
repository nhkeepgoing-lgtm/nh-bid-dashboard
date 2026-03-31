#!/usr/bin/env python3
"""
NH농협 그룹 입찰공고 통합 크롤러
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
from playwright.async_api import async_playwright

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
HEADERS = {"User-Agent": USER_AGENT, "Accept-Language": "ko-KR,ko;q=0.9"}


def normalize_date(raw: str) -> Optional[str]:
    if not raw:
        return None
    raw = raw.strip()
    for p in [r"(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})", r"(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일"]:
        m = re.search(p, raw)
        if m:
            return f"{m.group(1)}-{m.group(2).zfill(2)}-{m.group(3).zfill(2)}"
    return None


def is_new(date_str: Optional[str], days: int = 7) -> bool:
    if not date_str:
        return False
    try:
        return (datetime.now() - datetime.strptime(date_str, "%Y-%m-%d")).days <= days
    except ValueError:
        return False


def make_bid(source_id, source_name, title, date=None, deadline=None, link=None):
    return {
        "id": f"{source_id}_{hash(title + str(date)) & 0xFFFFFF:06x}",
        "source_id": source_id,
        "source_name": source_name,
        "title": title.strip(),
        "date": date,
        "deadline": deadline,
        "link": link,
        "category": "입찰공고",
        "is_new": is_new(date),
        "crawled_at": datetime.now().isoformat(),
    }


SKIP_TITLES = {"번호", "제목", "작성일", "등록일", "첨부파일", "조회", "조회수", "파일", "이용안내", "공지사항", "입찰공고"}


def parse_table(html, source_id, source_name, base_url, list_url,
                num_col=0, title_col=1, date_col=2) -> list:
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table")
    if not table:
        return []
    items = []
    for row in table.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 2:
            continue
        if len(cells) > num_col:
            if not cells[num_col].get_text(strip=True).isdigit():
                continue
        if len(cells) <= title_col:
            continue
        anchor = cells[title_col].find("a")
        title = (anchor.get_text(strip=True) if anchor else cells[title_col].get_text(strip=True))
        title = title.replace("새글 표시 아이콘", "").replace("새글", "").strip()
        if not title or len(title) < 3 or title in SKIP_TITLES:
            continue
        date_idx = date_col if date_col >= 0 else len(cells) - 1
        date_str = normalize_date(cells[date_idx].get_text(strip=True)) if len(cells) > date_idx else None
        link = list_url
        if anchor:
            href = anchor.get("href", "")
            if href and "javascript" not in href and href != "#":
                link = href if href.startswith("http") else base_url + href
        items.append(make_bid(source_id, source_name, title, date_str, link=link))
        if len(items) >= MAX_ITEMS:
            break
    return items


# ── 1. 범농협 통합구매 (nonghyup.com) ──────────────────────
class NonghyupBidCrawler:
    source_id = "nonghyup_bid"
    source_name = "범농협 통합구매"
    url = "https://www.nonghyup.com/ecenter/bid/bidList.do"

    def crawl_static(self):
        try:
            resp = requests.get(self.url, headers=HEADERS, timeout=20)
            resp.encoding = "utf-8"
            items = parse_table(resp.text, self.source_id, self.source_name,
                                "https://www.nonghyup.com", self.url,
                                num_col=0, title_col=1, date_col=3)
            logger.info(f"[nonghyup_bid] {len(items)}건 수집")
            return items
        except Exception as e:
            logger.error(f"[nonghyup_bid] {e}")
            return []


# ── 2. NH농협금융지주 (nhfngroup.com) ──────────────────────
class NHFnGroupCrawler:
    source_id = "nhfngroup"
    source_name = "NH농협금융지주"
    url = "https://www.nhfngroup.com/user/indexSub.do?codyMenuSeq=1102093359&siteId=nhfngroup"

    def crawl_static(self):
        try:
            resp = requests.get(self.url, headers=HEADERS, timeout=20)
            resp.encoding = "UTF-8"
            items = parse_table(resp.text, self.source_id, self.source_name,
                                "https://www.nhfngroup.com", self.url,
                                num_col=0, title_col=1, date_col=2)
            logger.info(f"[nhfngroup] {len(items)}건 수집")
            return items
        except Exception as e:
            logger.error(f"[nhfngroup] {e}")
            return []


# ── 3. NH농협경제지주 (nhabgroup.com) ──────────────────────
class NHAbGroupCrawler:
    source_id = "nhabgroup"
    source_name = "농협경제지주"
    url = "http://www.nhabgroup.com/user/indexSub.do?codyMenuSeq=602309274&siteId=nhabgroup"

    def crawl_static(self):
        try:
            resp = requests.get(self.url, headers=HEADERS, timeout=20)
            resp.encoding = "UTF-8"
            items = parse_table(resp.text, self.source_id, self.source_name,
                                "http://www.nhabgroup.com", self.url,
                                num_col=0, title_col=1, date_col=2)
            logger.info(f"[nhabgroup] {len(items)}건 수집")
            return items
        except Exception as e:
            logger.error(f"[nhabgroup] {e}")
            return []


# ── 4. NH농협캐피탈 (nhcapital.co.kr) ──────────────────────
class NHCapitalCrawler:
    source_id = "nhcapital"
    source_name = "NH농협캐피탈"
    url = "https://www.nhcapital.co.kr/customer/customer/customerManageposts/selectManagepostsNoticeList.nh"

    def crawl_static(self):
        items = []
        try:
            resp = requests.get(self.url, headers=HEADERS, timeout=20)
            resp.encoding = "utf-8"
            soup = BeautifulSoup(resp.text, "lxml")
            table = soup.find("table")
            if not table:
                return []
            for row in table.find_all("tr"):
                cells = row.find_all("td")
                if len(cells) < 4:
                    continue
                if not cells[0].get_text(strip=True).isdigit():
                    continue
                anchor = cells[1].find("a")
                title = (anchor.get_text(strip=True) if anchor else cells[1].get_text(strip=True))
                title = title.replace("신규", "").strip()
                if not title or len(title) < 3:
                    continue
                # 입찰 관련 키워드만
                if not any(kw in title for kw in ["입찰", "공고", "용역", "구매", "선정", "조달", "매각"]):
                    continue
                date_str = normalize_date(cells[3].get_text(strip=True))
                link = self.url
                if anchor:
                    href = anchor.get("href", "")
                    if href and "javascript" not in href and href != "#":
                        link = href if href.startswith("http") else "https://www.nhcapital.co.kr" + href
                items.append(make_bid(self.source_id, self.source_name, title, date_str, link=link))
                if len(items) >= MAX_ITEMS:
                    break
        except Exception as e:
            logger.error(f"[nhcapital] {e}")
        logger.info(f"[nhcapital] {len(items)}건 수집")
        return items


# ── 5. NH농협손해보험 (nhfire.co.kr) - Playwright ──────────
class NHFireCrawler:
    source_id = "nhfire"
    source_name = "NH농협손해보험"
    url = "https://www.nhfire.co.kr/company/bbs/tenderList.nhfire"

    async def crawl(self, page):
        items = []
        try:
            await page.goto(self.url, wait_until="networkidle", timeout=BROWSER_TIMEOUT)
            await page.wait_for_timeout(PAGE_WAIT)

            # 팝업 닫기 (있으면)
            try:
                confirm_btn = await page.query_selector("a[href*='nhlife']:not([href='#'])")
                if confirm_btn:
                    await confirm_btn.click()
                    await page.wait_for_timeout(500)
            except Exception:
                pass

            # id="content" 영역 안의 테이블만 선택
            # nhfire 입찰공고 테이블은 #content 또는 .board_list 안에 있음
            rows = await page.query_selector_all("#content table tbody tr, .board_list table tbody tr, table.bdList tbody tr")

            # 위 선택자로 못 찾으면 전체 테이블에서 번호가 숫자인 행만
            if not rows:
                rows = await page.query_selector_all("table tbody tr")

            logger.info(f"[nhfire] 발견된 행: {len(rows)}")

            for row in rows[:MAX_ITEMS * 2]:
                try:
                    cells = await row.query_selector_all("td")
                    if len(cells) < 2:
                        continue

                    # 첫 번째 셀이 숫자(번호)인지 확인 → 팝업 테이블 제외
                    num_text = (await cells[0].inner_text()).strip()
                    if not num_text.isdigit():
                        continue

                    anchor = await cells[1].query_selector("a")
                    title_el = anchor if anchor else cells[1]
                    title = (await title_el.inner_text()).strip()
                    if not title or len(title) < 3 or title in SKIP_TITLES:
                        continue

                    date_str = normalize_date(await cells[-1].inner_text())

                    link = self.url
                    if anchor:
                        href = await anchor.get_attribute("href")
                        if href and href != "#" and "javascript" not in href:
                            link = href if href.startswith("http") else f"https://www.nhfire.co.kr{href}"

                    items.append(make_bid(self.source_id, self.source_name, title, date_str, link=link))
                    if len(items) >= MAX_ITEMS:
                        break
                except Exception as e:
                    logger.debug(f"[nhfire] 행 오류: {e}")

        except Exception as e:
            logger.error(f"[nhfire] {e}")
        logger.info(f"[nhfire] {len(items)}건 수집")
        return items


# ── 6. NH농협생명보험 (nhlife.co.kr) - Playwright ──────────
class NHLifeCrawler:
    source_id = "nhlife"
    source_name = "NH농협생명보험"
    url = "https://www.nhlife.co.kr/ho/ci/HOCI0008M00.nhl"

    async def crawl(self, page):
        items = []
        try:
            await page.goto(self.url, wait_until="networkidle", timeout=BROWSER_TIMEOUT)
            await page.wait_for_timeout(PAGE_WAIT)

            rows = await page.query_selector_all("table tbody tr")
            logger.info(f"[nhlife] 발견된 행: {len(rows)}")

            for row in rows:
                try:
                    cells = await row.query_selector_all("td")
                    if len(cells) < 2:
                        continue

                    # 전체 행 텍스트에서 입찰 키워드 확인
                    full_text = await row.inner_text()
                    if not any(kw in full_text for kw in ["입찰", "공고", "조달", "구매", "용역", "컨설팅"]):
                        continue

                    # 제목은 두 번째 셀의 a 태그
                    anchor = await cells[1].query_selector("a")
                    title_el = anchor if anchor else cells[1]
                    title = (await title_el.inner_text()).strip()
                    if not title or len(title) < 3 or title in SKIP_TITLES:
                        continue

                    # 날짜는 마지막 셀
                    date_str = normalize_date(await cells[-1].inner_text())

                    link = self.url
                    if anchor:
                        href = await anchor.get_attribute("href")
                        if href and "javascript" not in href:
                            link = href if href.startswith("http") else f"https://www.nhlife.co.kr{href}"

                    items.append(make_bid(self.source_id, self.source_name, title, date_str, link=link))
                    if len(items) >= MAX_ITEMS:
                        break
                except Exception as e:
                    logger.debug(f"[nhlife] 행 오류: {e}")

        except Exception as e:
            logger.error(f"[nhlife] {e}")
        logger.info(f"[nhlife] {len(items)}건 수집")
        return items


# ── 메인 실행 ──────────────────────────────────────────────
async def run_all():
    all_items = []
    results_meta = {}

    # 정적 파싱 (빠름)
    for crawler in [NonghyupBidCrawler(), NHFnGroupCrawler(), NHAbGroupCrawler(), NHCapitalCrawler()]:
        logger.info(f"크롤링 (정적): {crawler.source_name}")
        try:
            items = crawler.crawl_static()
            all_items.extend(items)
            results_meta[crawler.source_id] = {
                "name": crawler.source_name, "url": crawler.url,
                "count": len(items), "status": "ok" if items else "empty",
                "last_crawled": datetime.now().isoformat(),
            }
        except Exception as e:
            logger.error(f"{crawler.source_name} 실패: {e}")
            results_meta[crawler.source_id] = {
                "name": crawler.source_name, "url": crawler.url,
                "count": 0, "status": "error", "error": str(e),
                "last_crawled": datetime.now().isoformat(),
            }

    # Playwright 필요 사이트
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
        )
        for crawler in [NHFireCrawler(), NHLifeCrawler()]:
            logger.info(f"크롤링 (브라우저): {crawler.source_name}")
            page = await browser.new_page(
                user_agent=USER_AGENT,
                extra_http_headers={"Accept-Language": "ko-KR,ko;q=0.9"},
            )
            try:
                items = await crawler.crawl(page)
                all_items.extend(items)
                results_meta[crawler.source_id] = {
                    "name": crawler.source_name, "url": crawler.url,
                    "count": len(items), "status": "ok" if items else "empty",
                    "last_crawled": datetime.now().isoformat(),
                }
            except Exception as e:
                logger.error(f"{crawler.source_name} 실패: {e}")
                results_meta[crawler.source_id] = {
                    "name": crawler.source_name, "url": crawler.url,
                    "count": 0, "status": "error", "error": str(e),
                    "last_crawled": datetime.now().isoformat(),
                }
            finally:
                await page.close()
        await browser.close()

    all_items.sort(key=lambda x: x.get("date") or "0000-00-00", reverse=True)

    output = {
        "generated_at": datetime.now().isoformat(),
        "total": len(all_items),
        "sources": results_meta,
        "items": all_items,
    }
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    logger.info(f"✅ 완료: 총 {len(all_items)}건")

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
                logger.error(f"Slack 오류: {e}")

    return output


if __name__ == "__main__":
    asyncio.run(run_all())
