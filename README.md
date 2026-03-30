# NH농협 그룹 입찰공고 통합 대시보드

7개 NH농협 계열사 입찰공고를 자동 수집하여 하나의 대시보드에서 조회하는 시스템입니다.

## 아키텍처

```
┌─────────────────────────────────────────────────────┐
│  GitHub Actions (매일 오전 9시, 오후 2시 자동 실행)     │
└────────────────────┬────────────────────────────────┘
                     │
         ┌───────────▼───────────┐
         │  Python Playwright     │  ← JS 렌더링 크롤러
         │  크롤러 (crawler/)     │    (Headless Chromium)
         └───────────┬───────────┘
                     │ data/bids.json 생성
         ┌───────────▼───────────┐
         │  Node.js Express API   │  ← REST API 서버
         │  (api/)               │    로컬 또는 Railway 배포
         └───────────┬───────────┘
                     │ /api/bids
         ┌───────────▼───────────┐
         │  정적 HTML 대시보드    │  ← GitHub Pages 배포
         │  (dashboard/)         │    또는 로컬 실행
         └───────────────────────┘
```

## 수집 대상

| 기관 | URL | 방식 |
|------|-----|------|
| NH농협생명 | nhlife.co.kr | Playwright (JS렌더링) |
| NH농협손해보험 | nhfire.co.kr | Playwright (JS렌더링) |
| NH농협캐피탈 | nhcapital.co.kr | Playwright (JS렌더링) |
| 범농협 통합구매 FIRSTePro | first-epro.com | Playwright (JS렌더링) |
| NH농협금융지주 | nhfngroup.com | requests (정적) |
| 농협몰 | nonghyup.com | Playwright (JS렌더링) |
| NH농업지주 | nhabgroup.com | requests (정적) |

## 빠른 시작

### 1. 크롤러 실행 (Python)
```bash
cd crawler
pip install -r requirements.txt
playwright install chromium
python crawl.py
# → data/bids.json 생성됨
```

### 2. API 서버 실행 (Node.js)
```bash
cd api
npm install
node server.js
# → http://localhost:3000
```

### 3. 대시보드 실행 (브라우저)
```bash
cd dashboard
npx serve public/
# → http://localhost:5000
```

## GitHub Actions 자동화

`.github/workflows/crawl.yml` 에서 스케줄 설정.  
크롤링 결과는 `data/bids.json` 으로 GitHub에 커밋됩니다.  
GitHub Pages를 켜면 `dashboard/public/` 이 자동 배포됩니다.

## 환경 변수

| 변수 | 설명 | 기본값 |
|------|------|--------|
| `PORT` | API 서버 포트 | 3000 |
| `DATA_PATH` | bids.json 경로 | ../data/bids.json |
| `SLACK_WEBHOOK` | 신규 공고 알림 URL | 없음 |
