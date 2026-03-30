'use strict';

/**
 * NH농협 입찰공고 REST API 서버
 * 
 * 엔드포인트:
 *   GET /api/bids              전체 목록 (필터/정렬/페이지 지원)
 *   GET /api/bids/:id          단건 조회
 *   GET /api/sources           수집 소스 메타 정보
 *   GET /api/stats             통계 요약
 *   GET /health                헬스체크
 */

const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const fs = require('fs');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;
const DATA_PATH = process.env.DATA_PATH || path.join(__dirname, '../data/bids.json');

// ─────────────────────────────────────────────
// 미들웨어
// ─────────────────────────────────────────────
app.use(helmet({ contentSecurityPolicy: false }));
app.use(cors({
  origin: '*',
  methods: ['GET'],
}));
app.use(express.json());

// Rate limiting
const limiter = rateLimit({
  windowMs: 60 * 1000, // 1분
  max: 120,
  standardHeaders: true,
  legacyHeaders: false,
});
app.use('/api', limiter);

// ─────────────────────────────────────────────
// 데이터 로더 (메모리 캐시 + 파일 감시)
// ─────────────────────────────────────────────
let cachedData = null;
let lastLoaded = null;
const CACHE_TTL_MS = 5 * 60 * 1000; // 5분

function loadData() {
  const now = Date.now();
  if (cachedData && lastLoaded && (now - lastLoaded) < CACHE_TTL_MS) {
    return cachedData;
  }
  try {
    if (!fs.existsSync(DATA_PATH)) {
      console.warn(`[warn] 데이터 파일 없음: ${DATA_PATH}`);
      return { generated_at: null, total: 0, sources: {}, items: [] };
    }
    const raw = fs.readFileSync(DATA_PATH, 'utf-8');
    cachedData = JSON.parse(raw);
    lastLoaded = now;
    console.log(`[info] 데이터 로드: ${cachedData.total}건 (${cachedData.generated_at})`);
    return cachedData;
  } catch (e) {
    console.error('[error] 데이터 파일 파싱 오류:', e.message);
    return { generated_at: null, total: 0, sources: {}, items: [] };
  }
}

// 파일 변경 감지 → 캐시 무효화
if (fs.existsSync(path.dirname(DATA_PATH))) {
  fs.watch(path.dirname(DATA_PATH), (evt, filename) => {
    if (filename === 'bids.json') {
      cachedData = null;
      console.log('[info] bids.json 변경 감지 → 캐시 무효화');
    }
  });
}

// ─────────────────────────────────────────────
// 헬퍼
// ─────────────────────────────────────────────
function paginate(arr, page, limit) {
  const p = Math.max(1, parseInt(page) || 1);
  const l = Math.min(100, Math.max(1, parseInt(limit) || 20));
  const start = (p - 1) * l;
  return {
    items: arr.slice(start, start + l),
    pagination: {
      page: p,
      limit: l,
      total: arr.length,
      total_pages: Math.ceil(arr.length / l),
      has_next: start + l < arr.length,
      has_prev: p > 1,
    },
  };
}

function filterItems(items, query) {
  let result = [...items];

  // 소스 필터
  if (query.source) {
    const sources = query.source.split(',').map(s => s.trim());
    result = result.filter(i => sources.includes(i.source_id));
  }

  // 키워드 검색
  if (query.q) {
    const kw = query.q.toLowerCase();
    result = result.filter(i =>
      (i.title || '').toLowerCase().includes(kw) ||
      (i.note || '').toLowerCase().includes(kw)
    );
  }

  // 신규만
  if (query.new === 'true') {
    result = result.filter(i => i.is_new);
  }

  // 날짜 범위
  if (query.date_from) {
    result = result.filter(i => i.date && i.date >= query.date_from);
  }
  if (query.date_to) {
    result = result.filter(i => i.date && i.date <= query.date_to);
  }

  // 정렬
  const sort = query.sort || 'date_desc';
  if (sort === 'date_asc') {
    result.sort((a, b) => (a.date || '').localeCompare(b.date || ''));
  } else if (sort === 'source') {
    result.sort((a, b) => a.source_id.localeCompare(b.source_id));
  } else {
    result.sort((a, b) => (b.date || '').localeCompare(a.date || ''));
  }

  return result;
}

// ─────────────────────────────────────────────
// 라우터
// ─────────────────────────────────────────────

// 헬스체크
app.get('/health', (req, res) => {
  const data = loadData();
  res.json({
    status: 'ok',
    generated_at: data.generated_at,
    total: data.total,
    uptime: process.uptime(),
  });
});

// 전체 목록
app.get('/api/bids', (req, res) => {
  const data = loadData();
  const filtered = filterItems(data.items || [], req.query);
  const { items, pagination } = paginate(filtered, req.query.page, req.query.limit);

  res.json({
    success: true,
    generated_at: data.generated_at,
    pagination,
    items,
  });
});

// 단건 조회
app.get('/api/bids/:id', (req, res) => {
  const data = loadData();
  const item = (data.items || []).find(i => i.id === req.params.id);
  if (!item) {
    return res.status(404).json({ success: false, error: '공고를 찾을 수 없습니다.' });
  }
  res.json({ success: true, item });
});

// 소스 메타
app.get('/api/sources', (req, res) => {
  const data = loadData();
  res.json({
    success: true,
    generated_at: data.generated_at,
    sources: data.sources || {},
  });
});

// 통계
app.get('/api/stats', (req, res) => {
  const data = loadData();
  const items = data.items || [];
  const now = new Date();
  const sevenDaysAgo = new Date(now - 7 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);

  // 소스별 집계
  const bySource = {};
  for (const item of items) {
    bySource[item.source_id] = bySource[item.source_id] || { count: 0, new_count: 0, name: item.source_name };
    bySource[item.source_id].count++;
    if (item.date && item.date >= sevenDaysAgo) {
      bySource[item.source_id].new_count++;
    }
  }

  res.json({
    success: true,
    generated_at: data.generated_at,
    stats: {
      total: items.length,
      new_count: items.filter(i => i.date && i.date >= sevenDaysAgo).length,
      sources_ok: Object.values(data.sources || {}).filter(s => s.status === 'ok').length,
      sources_total: Object.keys(data.sources || {}).length,
      by_source: bySource,
    },
  });
});

// 404
app.use((req, res) => {
  res.status(404).json({ success: false, error: 'Not Found' });
});

// 에러 핸들러
app.use((err, req, res, next) => {
  console.error('[error]', err);
  res.status(500).json({ success: false, error: '서버 오류가 발생했습니다.' });
});

// ─────────────────────────────────────────────
// 시작
// ─────────────────────────────────────────────
app.listen(PORT, () => {
  console.log(`
  ╔══════════════════════════════════════════╗
  ║  NH농협 입찰공고 API 서버 실행 중         ║
  ║  http://localhost:${PORT}                   ║
  ║                                          ║
  ║  GET /api/bids          전체 목록         ║
  ║  GET /api/bids?q=시스템  키워드 검색      ║
  ║  GET /api/bids?new=true  신규 공고만      ║
  ║  GET /api/stats          통계            ║
  ║  GET /api/sources        소스 정보       ║
  ╚══════════════════════════════════════════╝
  `);
});

module.exports = app;
