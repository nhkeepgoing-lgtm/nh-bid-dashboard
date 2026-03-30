#!/usr/bin/env bash
# setup.sh - NH농협 대시보드 로컬 환경 셋업
set -e

echo "═══════════════════════════════════════════"
echo "  NH농협 입찰공고 대시보드 셋업"
echo "═══════════════════════════════════════════"

# Python 가상환경
echo ""
echo "[1/4] Python 가상환경 & 의존성 설치..."
cd crawler
python3 -m venv .venv
source .venv/bin/activate
pip install -q -r requirements.txt
playwright install chromium
cd ..

# Node.js API
echo ""
echo "[2/4] Node.js API 의존성 설치..."
cd api
npm install --silent
cd ..

# data 디렉토리
echo ""
echo "[3/4] 데이터 디렉토리 확인..."
mkdir -p data
if [ ! -f data/bids.json ] || [ "$(cat data/bids.json | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d["total"])')" = "0" ]; then
  echo "  → 초기 크롤링 실행 중..."
  cd crawler
  source .venv/bin/activate
  python crawl.py
  cd ..
else
  echo "  → 기존 데이터 사용"
fi

echo ""
echo "[4/4] 완료!"
echo ""
echo "═══════════════════════════════════════════"
echo "  실행 방법:"
echo ""
echo "  [터미널 1] API 서버:"
echo "    cd api && node server.js"
echo ""
echo "  [터미널 2] 대시보드:"
echo "    cd dashboard && npx serve public/"
echo "    → http://localhost:5000"
echo ""
echo "  [크롤링 재실행]:"
echo "    cd crawler && source .venv/bin/activate"
echo "    python crawl.py"
echo "═══════════════════════════════════════════"
