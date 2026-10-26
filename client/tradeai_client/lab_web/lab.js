/* TradeAI Create Strategy: полигон обучения бота (окно pywebview, Lightweight Charts 4.1).
 * Трейдер открывает любой отрезок любой монеты (или случайный), рисует и подписывает разметку понятиями словаря + причиной, ставит сделки (L/S),
 * гоняет реплей (будущее скрыто, свечи открываются по одной). Бот рисует своё (детекторы Python), подсказывает «ищу вход» и даёт сигналы на закрытой свече,
 * сравнение показывает совпало / лишнее / не увидел. Урок (свечи + разметка + причины) сохраняется на ПК и отправляется на сервер.
 * Координаты рисунков - (время UTC в секундах, цена): переживают смену таймфрейма. */
(function () {
  'use strict';
  const CFG = window.__CFG__ || {};
  const $ = (id) => document.getElementById(id);
  const TFS = ['1m', '5m', '15m', '1h', '4h', '1d'];
  const IV = { '1m': 60, '5m': 300, '15m': 900, '1h': 3600, '4h': 14400, '1d': 86400 };
  const pad = (n) => String(n).padStart(2, '0');
  const S = {
    symbol: 'BTCUSDT', tf: '15m', all: [], cursor: -1, digits: 2,
    tool: 'cursor', drawings: [], selected: null, draft: null, drag: null,
    vocab: [], terms: {}, strategy: '', strategies: [], author: CFG.author || '',
    bot: { objects: [], events: [], trend: 0 }, botLayer: true, mineLayer: true, events: [], seenEv: {},
    // по умолчанию бот показывает «каркас» (уровни, зоны, наклонки, проторговки, ликвидность, тренд); метки - отдельными слоями, иначе график тонет в подписях
    hidden: { high: true, low: true, volume: true, reversal: true, breakout: true, zone_exit: true, gap: true, consolidation: true },
    compare: null, playing: false, timer: 0, busy: false, lessonId: null, lessonCreated: null, notes: '', complete: null,
    botTrades: [], raf: 0, loadingOlder: false, fetchingFuture: false, futureEnd: false,
  };
  let chart, series, vol;

  /* ------------------------------------------------------------------ мост Python */
  let ready = null;
  function bridge() {
    if (ready) return ready;
    ready = new Promise((resolve) => {
      if (window.pywebview && window.pywebview.api) return resolve(window.pywebview.api);
      window.addEventListener('pywebviewready', () => resolve(window.pywebview.api));
      setTimeout(() => resolve(window.pywebview && window.pywebview.api ? window.pywebview.api : null), 8000);
    });
    return ready;
  }
  async function call(name, ...args) {
    const api = await bridge();
    if (!api || !api[name]) throw new Error('нет связи с приложением');
    return api[name](...args);
  }
  let toastTimer = null;
  function toast(text, ms) {
    const t = $('toast'); t.textContent = text; t.style.display = 'block';
    clearTimeout(toastTimer); toastTimer = setTimeout(() => { t.style.display = 'none'; }, ms || 4000);
  }
  function setStatus(kind, text) { $('dot').className = kind; $('stext').textContent = text; }
  const esc = (s) => String(s == null ? '' : s).replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
  const fmtLocal = (sec) => { const d = new Date(sec * 1000); return pad(d.getDate()) + '.' + pad(d.getMonth() + 1) + '.' + String(d.getFullYear()).slice(2) + ' ' + pad(d.getHours()) + ':' + pad(d.getMinutes()); };
  const fp = (p) => (p == null || !isFinite(p) ? '—' : (+p).toFixed(S.digits));
  const newId = () => 'd' + Date.now().toString(36) + Math.random().toString(36).slice(2, 6);
  const termOf = (id) => S.terms[id] || null;
  const iv = () => IV[S.tf];
  const visible = () => S.all.slice(0, S.cursor + 1);
  const atrNow = () => { const b = visible().slice(-15); if (b.length < 2) return 0; let s = 0; for (let i = 1; i < b.length; i++) s += Math.max(b[i].high - b[i].low, Math.abs(b[i].high - b[i - 1].close), Math.abs(b[i].low - b[i - 1].close)); return s / (b.length - 1); };

  /* ------------------------------------------------------------------ время <-> пиксели (как в окне сделки: середина свечи, дробные позиции считаем сами) */
  function rawToLogical(t) {
    const b = visible(), n = b.length, w = iv();
    if (!n) return null;
    if (t <= b[0].time) return (t - b[0].time) / w;
    if (t >= b[n - 1].time) return n - 1 + (t - b[n - 1].time) / w;
    let lo = 0, hi = n - 1;
    while (hi - lo > 1) { const m = (lo + hi) >> 1; if (b[m].time <= t) lo = m; else hi = m; }
    return lo + (t - b[lo].time) / (b[hi].time - b[lo].time);
  }
  function rawToTime(L) {
    const b = visible(), n = b.length, w = iv();
    if (!n) return null;
    if (L <= 0) return b[0].time + L * w;
    if (L >= n - 1) return b[n - 1].time + (L - (n - 1)) * w;
    const i = Math.floor(L);
    return b[i].time + (L - i) * (b[i + 1].time - b[i].time);
  }
  const timeToLogical = (t) => rawToLogical(t - iv() / 2);
  const logicalToTime = (L) => { const r = rawToTime(L); return r == null ? null : r + iv() / 2; };
  function logicalX(ts, L) {
    const a = Math.floor(L);
    if (a === L) return ts.logicalToCoordinate(L);
    const xa = ts.logicalToCoordinate(a), xb = ts.logicalToCoordinate(a + 1);
    return xa == null || xb == null ? null : xa + (xb - xa) * (L - a);
  }
  const X = (sec) => { const L = timeToLogical(sec); return L == null ? null : logicalX(chart.timeScale(), L); };
  const Y = (price) => series.priceToCoordinate(price);
  const xToTime = (x) => { const L = chart.timeScale().coordinateToLogical(x); return L == null ? null : logicalToTime(L); };
  const yToPrice = (y) => series.coordinateToPrice(y);
  const tsec = (t) => (t == null || !isFinite(t) ? null : Math.round(t));
  const Xbar = (t) => X(t + iv() / 2);  // центр свечи по времени её открытия (объекты бота)

  /* ------------------------------------------------------------------ свечи */
  const toBar = (r) => ({ time: r[0], open: r[1], high: r[2], low: r[3], close: r[4], volume: r[5], bv: r[6] });
  const toRow = (b) => [b.time, b.open, b.high, b.low, b.close, b.volume, b.bv];
  function digitsFor(p) { return p >= 1000 ? 2 : p >= 10 ? 3 : p >= 1 ? 4 : p >= 0.1 ? 5 : p >= 0.01 ? 6 : 7; }
  function render(keepRange) {
    const vb = visible();
    const range = keepRange ? chart.timeScale().getVisibleLogicalRange() : null;
    series.setData(vb.map((b) => ({ time: b.time, open: b.open, high: b.high, low: b.low, close: b.close })));
    vol.setData(vb.map((b) => ({ time: b.time, value: b.volume, color: b.close >= b.open ? 'rgba(38,166,154,.35)' : 'rgba(239,83,80,.35)' })));
    if (range) chart.timeScale().setVisibleLogicalRange(range);
    updateClock(); scheduleDraw();
  }
  function revealNext() {
    const b = S.all[S.cursor + 1]; if (!b) return null;
    S.cursor += 1;
    series.update({ time: b.time, open: b.open, high: b.high, low: b.low, close: b.close });
    vol.update({ time: b.time, value: b.volume, color: b.close >= b.open ? 'rgba(38,166,154,.35)' : 'rgba(239,83,80,.35)' });
    updateClock();
    return b;
  }
  function updateClock() { const b = S.all[S.cursor]; $('clock').textContent = b ? 'сейчас: ' + fmtLocal(b.time + iv()) : ''; }

  async function openSpot(symbol, tf, atS, opts) {
    stop();
    setStatus('busy', 'загрузка свечей…');
    const sym = symbol.toUpperCase().replace(/[^A-Z0-9]/g, '');
    const h = await call('history', sym, tf, atS, 800);
    if (!h.ok) { setStatus('off', h.error); toast('Свечи: ' + h.error, 7000); return false; }
    if (!h.bars.length) { setStatus('off', 'нет свечей'); toast('У ' + sym + ' нет свечей ' + tf + ' до этой даты', 6000); return false; }
    const hist = h.bars.map(toBar);
    const last = hist[hist.length - 1];
    S.symbol = sym; S.tf = tf; S.all = hist; S.cursor = hist.length - 1; S.futureEnd = false;
    S.digits = digitsFor(last.close);
    series.applyOptions({ priceFormat: { type: 'price', precision: S.digits, minMove: Math.pow(10, -S.digits) } });
    if (!opts || !opts.keepMarks) { S.bot = { objects: [], events: [], trend: 0 }; S.events = []; S.seenEv = {}; S.compare = null; S.botTrades = []; }
    $('coin').value = sym;
    document.querySelectorAll('#tfs .btn').forEach((b) => b.classList.toggle('on', b.dataset.tf === tf));
    $('date').value = toLocalInput(last.time + iv());
    render(false);
    chart.timeScale().setVisibleLogicalRange({ from: Math.max(0, S.cursor - 180), to: S.cursor + 25 });
    setStatus('live', sym + ' ' + tf);
    await ensureFuture();
    renderPanels();
    return true;
  }
  async function ensureFuture() {
    if (S.futureEnd || S.fetchingFuture || S.all.length - 1 - S.cursor > 50) return;
    S.fetchingFuture = true;
    try {
      const lastT = S.all.length ? S.all[S.all.length - 1].time : 0;
      const r = await call('future', S.symbol, S.tf, lastT + iv(), 1000);
      if (r.ok) {
        const add = r.bars.map(toBar).filter((b) => b.time > lastT);
        if (!add.length) S.futureEnd = true; else S.all = S.all.concat(add);
      }
    } finally { S.fetchingFuture = false; }
  }
  async function loadOlder() {
    if (S.loadingOlder || !S.all.length) return;
    S.loadingOlder = true;
    try {
      const r = await call('history', S.symbol, S.tf, S.all[0].time, 800);
      if (r.ok) {
        const add = r.bars.map(toBar).filter((b) => b.time < S.all[0].time);
        if (add.length) {
          const range = chart.timeScale().getVisibleLogicalRange();
          S.all = add.concat(S.all); S.cursor += add.length;
          render(false);
          if (range) chart.timeScale().setVisibleLogicalRange({ from: range.from + add.length, to: range.to + add.length });
        }
      }
    } finally { S.loadingOlder = false; }
  }
  function toLocalInput(sec) { const d = new Date(sec * 1000); return d.getFullYear() + '-' + pad(d.getMonth() + 1) + '-' + pad(d.getDate()) + 'T' + pad(d.getHours()) + ':' + pad(d.getMinutes()); }

  async function switchTf(tf) {
    if (tf === S.tf || !S.all.length) return;
    const nowS = S.all[S.cursor].time + iv();  // текущий момент реплея: закрытие последней видимой свечи
    await openSpot(S.symbol, tf, nowS, { keepMarks: true });
    analyzeNow(false);
  }

  /* ------------------------------------------------------------------ реплей */
  function stop() { S.playing = false; clearTimeout(S.timer); $('b-play').textContent = '▶ Play'; $('b-play').classList.remove('on'); }
  function play() {
    if (S.playing) { stop(); return; }
    S.playing = true; $('b-play').textContent = '⏸ Пауза'; $('b-play').classList.add('on');
    const loop = async () => {
      if (!S.playing) return;
      const ok = await step();
      if (!ok) { stop(); toast('Дальше свечей нет', 4000); return; }
      S.timer = setTimeout(loop, 1000 / (+$('speed').value || 1));
    };
    loop();
  }
  async function step() {
    await ensureFuture();
    const b = revealNext(); if (!b) return false;
    const ts = chart.timeScale(), r = ts.getVisibleLogicalRange();
    if (r && r.to < S.cursor + 3) ts.scrollToPosition(Math.max(5, ts.scrollPosition()), false);
    evalTrades(b);
    if ($('bot-live').checked) await analyzeNow(true); else scheduleDraw();
    renderTradesPane();
    return true;
  }

  /* ------------------------------------------------------------------ бот */
  async function analyzeNow(withSetups) {
    if (S.busy || !S.all.length) return;
    S.busy = true;
    try {
      const rows = visible().slice(-1500).map(toRow);
      const r = await call('analyze', rows, S.tf, S.strategy, !!withSetups);
      if (!r.ok) { toast('Бот: ' + r.error, 6000); return; }
      S.bot = r;
      for (const ev of r.events || []) {
        const key = ev.t + '|' + ev.text;
        if (S.seenEv[key]) continue;
        S.seenEv[key] = 1; S.events.unshift(ev);
        if (ev.kind === 'signal') { toast('🤖 ' + ev.text, 7000); openBotTrade(ev); }
      }
      renderBadges(); renderBotPane(); scheduleDraw();
    } finally { S.busy = false; }
  }
  function renderBadges() {
    const el = $('badges'); el.textContent = '';
    const st = { 1: ['тренд вверх', 'good'], '-1': ['тренд вниз', 'bad'], 0: ['боковик', 'amber'] }[S.bot.trend || 0];
    if (S.bot.objects.length) { const b = document.createElement('span'); b.className = 'badge ' + st[1]; b.textContent = 'Бот: ' + st[0]; el.appendChild(b); }
    const last = S.events[0];
    if (last && S.all[S.cursor] && last.t === S.all[S.cursor].time) { const b = document.createElement('span'); b.className = 'badge ' + (last.kind === 'signal' ? 'good' : 'amber'); b.textContent = '🤖 ' + last.text; el.appendChild(b); }
  }

  /* ------------------------------------------------------------------ сделки (свои и бота) */
  function tradeFromDrawing(d) { return d.tool === 'long' || d.tool === 'short'; }
  function evalOne(tr, b) {
    if (tr.status === 'win' || tr.status === 'loss' || tr.status === 'closed') return;
    const long = tr.side === 'long';
    if (tr.status === 'pending') {
      if (b.time <= tr.placed) return;
      if (tr.mode === 'market') { tr.fill = b.open; }
      else if (tr.mode === 'limit') { if (long ? b.low <= tr.entry : b.high >= tr.entry) tr.fill = tr.entry; }
      else if (long ? b.high >= tr.entry : b.low <= tr.entry) tr.fill = tr.entry;  // стоп-вход на пробой
      if (tr.fill == null) return;
      tr.status = 'open'; tr.fill_t = b.time;
    }
    const hitSl = long ? b.low <= tr.sl : b.high >= tr.sl, hitTp = long ? b.high >= tr.tp : b.low <= tr.tp;
    if (hitSl) { tr.status = 'loss'; tr.exit = tr.sl; tr.exit_t = b.time; }  // стоп и тейк в одной свече - считаем стоп (пессимистично)
    else if (hitTp) { tr.status = 'win'; tr.exit = tr.tp; tr.exit_t = b.time; }
    if (tr.exit != null) tr.r = (long ? tr.exit - tr.fill : tr.fill - tr.exit) / Math.abs(tr.fill - tr.sl);
  }
  function evalTrades(b) {
    for (const d of S.drawings) if (tradeFromDrawing(d)) { syncTrade(d); evalOne(d.trade, b); }
    for (const t of S.botTrades) evalOne(t, b);
  }
  function syncTrade(d) {  // пока сделка не исполнена, уровни можно двигать мышью
    if (!d.trade) d.trade = { side: d.tool, status: 'pending', placed: d.placed };
    const tr = d.trade;
    if (!d.mode) setEntryMode(d);
    if (tr.status === 'pending') { tr.entry = d.p1.p; tr.sl = d.sl; tr.tp = d.tp; tr.mode = d.mode; }
    else { tr.sl = d.sl; tr.tp = d.tp; }  // после входа можно двигать стоп и тейк
  }
  function setEntryMode(d) {  // тип входа фиксируется при постановке (и при переносе цены входа), а не пересчитывается на каждой свече
    const close = S.all[S.cursor] ? S.all[S.cursor].close : d.p1.p, tol = 0.1 * (atrNow() || 0);
    d.mode = Math.abs(d.p1.p - close) <= tol ? 'market' : ((d.tool === 'long') === (d.p1.p < close) ? 'limit' : 'stop');
  }
  function openBotTrade(ev) {
    if (!$('bot-live').checked) return;
    if (S.botTrades.some((t) => t.status === 'pending' || t.status === 'open')) return;  // одна сделка бота за раз
    S.botTrades.push({ side: ev.side, entry: ev.entry, sl: ev.stop, tp: ev.target, mode: 'market', status: 'pending', placed: ev.t, why: ev.text, bot: true });
  }
  function closeTradeNow(d) {
    const tr = d.trade, b = S.all[S.cursor];
    if (!tr || tr.status !== 'open' || !b) return;
    tr.status = 'closed'; tr.exit = b.close; tr.exit_t = b.time;
    tr.r = (tr.side === 'long' ? tr.exit - tr.fill : tr.fill - tr.exit) / Math.abs(tr.fill - tr.sl);
    renderTradesPane(); scheduleDraw();
  }

  /* ------------------------------------------------------------------ отрисовка */
  function scheduleDraw() {
    if (S.raf) return;
    const run = () => { if (!S.raf) return; S.raf = 0; draw(); };
    S.raf = requestAnimationFrame(run); setTimeout(run, 60);
  }
  function rgba(hex, a) {
    const m = /^#?([0-9a-f]{6})$/i.exec(hex || ''); if (!m) return hex;
    const n = parseInt(m[1], 16); return 'rgba(' + (n >> 16) + ',' + ((n >> 8) & 255) + ',' + (n & 255) + ',' + a + ')';
  }
  function plot() { const box = $('chartbox').getBoundingClientRect(); return { w: chart.timeScale().width(), h: box.height - chart.timeScale().height(), box }; }
  function label(ctx, text, x, y, color, bg) {
    if (!text) return;
    ctx.font = '12px "Segoe UI",sans-serif';
    const w = ctx.measureText(text).width + 8;
    ctx.fillStyle = bg || 'rgba(11,14,20,.82)'; ctx.fillRect(x, y - 9, w, 18);
    ctx.fillStyle = color; ctx.textBaseline = 'middle'; ctx.fillText(text, x + 4, y);
  }
  function seg(ctx, a, b, color, width, dash) {
    ctx.beginPath(); ctx.setLineDash(dash || []); ctx.strokeStyle = color; ctx.lineWidth = width;
    ctx.moveTo(a.x, a.y); ctx.lineTo(b.x, b.y); ctx.stroke(); ctx.setLineDash([]);
  }
  const pt = (p) => { const x = X(p.t), y = Y(p.p); return (x == null || y == null) ? null : { x, y }; };
  function farPoint(a, b) { const dx = b.x - a.x, dy = b.y - a.y, len = Math.hypot(dx, dy) || 1, k = 20000 / len; return { x: a.x + dx * k, y: a.y + dy * k }; }

  /* Подписи горизонталей (уровни, ликвидность - ваши и бота) собираются за кадр и рисуются справа без наложения друг на друга.
   * Горизонталь за верхним/нижним краем экрана не пропадает: у края стоит стрелка с ценой (▲ выше экрана, ▼ ниже). */
  let hLabels = [];
  function hlineLabel(text, y, P, color) {
    const out = y < 0 ? -1 : y > P.h ? 1 : 0;
    hLabels.push({ text: (out < 0 ? '▲ ' : out > 0 ? '▼ ' : '') + text, y: out < 0 ? 0 : out > 0 ? P.h : y, color, edge: out });
  }
  function flushHLabels(ctx, P) {
    const GAP = 17, list = hLabels; hLabels = [];
    if (!list.length) return;
    list.sort((a, b) => a.y - b.y);
    let prev = -1e9;
    for (const l of list) { l.yy = Math.min(Math.max(l.y, 10, prev + GAP), P.h - 10); prev = l.yy; }
    let next = 1e9;
    for (let i = list.length - 1; i >= 0; i--) { list[i].yy = Math.min(list[i].yy, next - GAP); next = list[i].yy; }
    ctx.font = '12px "Segoe UI",sans-serif';
    for (const l of list) {
      const w = ctx.measureText(l.text).width + 8, x = Math.max(0, P.w - w - 6);
      if (l.edge === 0 && Math.abs(l.yy - l.y) > 2) { ctx.strokeStyle = rgba(l.color, 0.5); ctx.lineWidth = 1; ctx.beginPath(); ctx.moveTo(P.w - 4, l.y); ctx.lineTo(P.w - 4, l.yy); ctx.stroke(); }
      label(ctx, l.text, x, l.yy, l.color, l.edge ? 'rgba(11,14,20,.95)' : null);
    }
  }
  function drawBotObj(ctx, o, P, mark) {
    const t = termOf(o.type); const col = mark === 'extra' ? '#ff9800' : (t ? t.color : '#c9d1d9');
    const dash = mark === 'extra' ? [6, 4] : null;
    if (o.kind === 'hline') {
      const x1 = Xbar(o.t1), x2 = Xbar(o.t2), y = Y(o.p1); if (x1 == null || x2 == null || y == null) return;
      const txt = (mark === 'extra' ? 'лишнее: ' : '') + o.label + ' · ' + fp(o.p1);
      if (y < 0 || y > P.h) { hlineLabel(txt, y, P, col); return; }  // цена вне экрана: линию не видно, но уровень на месте - стрелка у края
      if (x2 < 0) return;  // уровень закончился левее экрана
      seg(ctx, { x: Math.max(x1, -50), y }, { x: Math.max(x2, x1 + 4), y }, rgba(col, 0.85), mark ? 2.5 : 1.5, dash || [8, 4]);
      hlineLabel(txt, y, P, col);
    } else if (o.kind === 'box') {
      const x1 = X(o.t1), x2 = X(o.t2 + iv()), y1 = Y(o.p1), y2 = Y(o.p2);
      if (x1 == null || x2 == null || y1 == null || y2 == null) return;
      ctx.fillStyle = rgba(col, 0.1); ctx.fillRect(Math.min(x1, x2), Math.min(y1, y2), Math.abs(x2 - x1), Math.max(2, Math.abs(y2 - y1)));
      ctx.setLineDash(dash || [4, 3]); ctx.strokeStyle = rgba(col, 0.75); ctx.lineWidth = mark ? 2 : 1;
      ctx.strokeRect(Math.min(x1, x2), Math.min(y1, y2), Math.abs(x2 - x1), Math.max(2, Math.abs(y2 - y1))); ctx.setLineDash([]);
      label(ctx, (mark === 'extra' ? 'лишнее: ' : '') + o.label, Math.min(x1, x2) + 2, Math.min(y1, y2) - 10, col);
    } else if (o.kind === 'line') {
      const a = { x: Xbar(o.t1), y: Y(o.p1) }, b = { x: Xbar(o.t2), y: Y(o.p2) };
      if (a.x == null || b.x == null || a.y == null || b.y == null) return;
      const isTrend = o.type === 'trend';
      seg(ctx, a, b, rgba(isTrend ? (o.side === 'long' ? '#2bb673' : '#e5534b') : col, isTrend ? 0.45 : 0.9), isTrend ? 1 : (mark ? 2.5 : 1.8), dash || (isTrend ? [2, 3] : null));
      if (!isTrend) label(ctx, (mark === 'extra' ? 'лишнее: ' : '') + o.label, b.x + 6, b.y, col);
    } else if (o.kind === 'point') {
      const x = Xbar(o.t1), y = Y(o.p1); if (x == null || y == null) return;
      ctx.beginPath(); ctx.arc(x, y, mark ? 5 : 3.5, 0, Math.PI * 2); ctx.fillStyle = col; ctx.fill();
      const many = (S.pointCount[o.type] || 0) > 25 && !mark;  // много меток одного вида - только точки (подпись - у выбранных при сравнении)
      if (o.type === 'high' || o.type === 'low' || (o.type === 'trend')) label(ctx, o.label, x - 6, y + (o.type === 'low' || o.label === 'HL' || o.label === 'LL' ? 13 : -13), col, 'rgba(0,0,0,0)');
      else if (!many) label(ctx, (mark === 'extra' ? 'лишнее: ' : '') + o.label, x + 6, y + (o.side === 'short' ? -12 : 12), col);
    }
  }
  function drawTradeBox(ctx, P, tr, entryT, selected, isBot) {
    const x1 = X(entryT); if (x1 == null) return;
    const endT = tr.exit_t ? tr.exit_t + iv() : (S.all[S.cursor] ? S.all[S.cursor].time + iv() * 12 : entryT + iv() * 30);
    const x2 = Math.max(X(endT) ?? x1 + 120, x1 + 40);
    const ye = Y(tr.fill != null ? tr.fill : tr.entry), ys = Y(tr.sl), yt = Y(tr.tp); if (ye == null || ys == null || yt == null) return;
    ctx.fillStyle = 'rgba(43,182,115,' + (isBot ? 0.1 : 0.18) + ')'; ctx.fillRect(x1, Math.min(ye, yt), x2 - x1, Math.abs(yt - ye));
    ctx.fillStyle = 'rgba(229,83,75,' + (isBot ? 0.1 : 0.18) + ')'; ctx.fillRect(x1, Math.min(ye, ys), x2 - x1, Math.abs(ys - ye));
    const dash = isBot ? [5, 4] : null;
    seg(ctx, { x: x1, y: ye }, { x: x2, y: ye }, '#4aa3ff', 1.5, dash);
    seg(ctx, { x: x1, y: ys }, { x: x2, y: ys }, '#e5534b', 1.5, dash);
    seg(ctx, { x: x1, y: yt }, { x: x2, y: yt }, '#2bb673', 1.5, dash);
    const risk = Math.abs((tr.fill ?? tr.entry) - tr.sl) || 1e-12, rr = Math.abs(tr.tp - (tr.fill ?? tr.entry)) / risk;
    const res = tr.r != null ? (tr.r >= 0 ? '+' : '') + tr.r.toFixed(2) + 'R' : { pending: 'ждёт входа', open: 'в позиции' }[tr.status] || '';
    label(ctx, (isBot ? '🤖 ' : '') + (tr.side === 'long' ? 'ЛОНГ' : 'ШОРТ') + '  1:' + rr.toFixed(1) + '  ' + res, x1 + 2, (tr.side === 'long' ? Math.min(yt, ye) : Math.max(yt, ye)) + (tr.side === 'long' ? -11 : 11),
      tr.r == null ? '#c9d1d9' : tr.r >= 0 ? '#2bb673' : '#e5534b');
    if (tr.exit_t) { const xe = Xbar(tr.exit_t), yx = Y(tr.exit); if (xe != null && yx != null) { ctx.beginPath(); ctx.arc(xe, yx, 4, 0, 7); ctx.fillStyle = tr.r >= 0 ? '#2bb673' : '#e5534b'; ctx.fill(); } }
    if (selected) for (const [y] of [[ye], [ys], [yt]]) { ctx.fillStyle = '#fff'; ctx.strokeStyle = '#4aa3ff'; ctx.lineWidth = 2; ctx.beginPath(); ctx.rect(x1 + 20 - 5, y - 5, 10, 10); ctx.fill(); ctx.stroke(); }
  }
  function drawMine(ctx, d, P, selected, draft) {
    const t = termOf(d.term), col = rgba(d.color || (t ? t.color : '#ffb300'), d.opacity || 0.9), w = d.width || 2;
    const cm = S.compare && S.compare.byUser[d.id];
    const a = pt(d.p1), b = d.p2 ? pt(d.p2) : null;
    let anchor = a;
    if (d.tool === 'long' || d.tool === 'short') { syncTrade(d); drawTradeBox(ctx, P, d.trade, d.p1.t, selected, false); return; }
    if (d.tool === 'hline') {
      const y = Y(d.p1.p); if (y == null) return;
      const x0 = Math.max(0, X(d.p1.t) ?? 0);
      if (y < 0 || y > P.h) { hlineLabel((t ? t.name : 'без подписи') + ' · ' + fp(d.p1.p), y, P, t ? t.color : '#c9d1d9'); return; }  // ваш уровень за краем экрана - стрелка с ценой
      seg(ctx, { x: x0, y }, { x: P.w, y }, col, w); anchor = { x: Math.min(P.w - 200, x0 + 4), y };
    } else if (d.tool === 'point') {
      if (!a) return; ctx.beginPath(); ctx.arc(a.x, a.y, 6, 0, Math.PI * 2); ctx.fillStyle = col; ctx.fill(); ctx.lineWidth = 2; ctx.strokeStyle = '#0e1117'; ctx.stroke();
    } else if (a && b) {
      if (d.tool === 'trend') seg(ctx, a, b, col, w);
      else if (d.tool === 'ray') seg(ctx, a, farPoint(a, b), col, w);
      else if (d.tool === 'rect') {
        ctx.fillStyle = rgba(d.color || (t ? t.color : '#ffb300'), 0.22); ctx.strokeStyle = col; ctx.lineWidth = w;
        ctx.fillRect(Math.min(a.x, b.x), Math.min(a.y, b.y), Math.abs(b.x - a.x), Math.abs(b.y - a.y));
        ctx.strokeRect(Math.min(a.x, b.x), Math.min(a.y, b.y), Math.abs(b.x - a.x), Math.abs(b.y - a.y));
        anchor = { x: Math.min(a.x, b.x), y: Math.min(a.y, b.y) };
      }
    } else return;
    if (!draft) {
      const txt = (t ? t.name : 'без подписи') + (cm === 'matched' ? '  ✓ бот видит' : cm === 'missed' ? '  ✗ бот не увидел' : '');
      label(ctx, txt, (anchor ? anchor.x : 0) + 4, (anchor ? anchor.y : 0) - 12, cm === 'matched' ? '#2bb673' : cm === 'missed' ? '#e5534b' : (t ? t.color : '#c9d1d9'));
      if (cm === 'missed' && a) { ctx.setLineDash([3, 3]); ctx.strokeStyle = '#e5534b'; ctx.lineWidth = 1.5; ctx.strokeRect((anchor || a).x - 6, (anchor || a).y - 6, 12, 12); ctx.setLineDash([]); }
    }
    if (selected) for (const h of handles(d)) { ctx.fillStyle = '#fff'; ctx.strokeStyle = '#4aa3ff'; ctx.lineWidth = 2; ctx.beginPath(); ctx.rect(h.x - 5, h.y - 5, 10, 10); ctx.fill(); ctx.stroke(); }
  }
  function draw() {
    if (!chart) return;
    const P = plot(), cv = $('ov'), dpr = window.devicePixelRatio || 1;
    const bw = Math.round(P.box.width * dpr), bh = Math.round(P.box.height * dpr);
    if (cv.width !== bw || cv.height !== bh) { cv.width = bw; cv.height = bh; cv.style.width = P.box.width + 'px'; cv.style.height = P.box.height + 'px'; }
    const ctx = cv.getContext('2d');
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0); ctx.clearRect(0, 0, P.box.width, P.box.height);
    ctx.save(); ctx.beginPath(); ctx.rect(0, 0, P.w, P.h); ctx.clip();
    const objs = S.compare ? S.compare.objects : S.bot.objects;
    S.pointCount = {}; for (const o of objs) if (o.kind === 'point') S.pointCount[o.type] = (S.pointCount[o.type] || 0) + 1;
    if (S.botLayer) objs.forEach((o, i) => { if (!S.hidden[o.type] && o.kind !== 'label') drawBotObj(ctx, o, P, S.compare ? S.compare.byBot[i] : null); });
    for (const t of S.botTrades) drawTradeBox(ctx, P, t, t.placed + iv(), false, true);
    if (S.mineLayer) for (const d of S.drawings) drawMine(ctx, d, P, d.id === S.selected, false);
    if (S.draft) drawMine(ctx, S.draft.d, P, false, true);
    flushHLabels(ctx, P);
    ctx.restore();
  }

  /* ------------------------------------------------------------------ рисование, выбор, правка */
  function handles(d) {
    if (d.tool === 'long' || d.tool === 'short') {
      const x = (X(d.p1.t) ?? 0) + 20; return [{ x, y: Y(d.p1.p), k: 'entry' }, { x, y: Y(d.sl), k: 'sl' }, { x, y: Y(d.tp), k: 'tp' }].filter((h) => h.y != null);
    }
    const a = pt(d.p1), b = d.p2 ? pt(d.p2) : null, out = [];
    if (d.tool === 'hline') { const y = Y(d.p1.p); if (y != null) out.push({ x: Math.max(24, X(d.p1.t) ?? 24), y, k: 'p1' }); return out; }
    if (a) out.push({ ...a, k: 'p1' }); if (b) out.push({ ...b, k: 'p2' });
    return out;
  }
  function distSeg(px, py, a, b) {
    const dx = b.x - a.x, dy = b.y - a.y, l2 = dx * dx + dy * dy;
    const t = l2 ? Math.max(0, Math.min(1, ((px - a.x) * dx + (py - a.y) * dy) / l2)) : 0;
    return Math.hypot(px - (a.x + t * dx), py - (a.y + t * dy));
  }
  function hitTest(d, x, y) {
    const a = pt(d.p1), b = d.p2 ? pt(d.p2) : null, tol = 7;
    if (d.tool === 'hline') { const yy = Y(d.p1.p); return yy != null && Math.abs(y - yy) <= tol; }
    if (d.tool === 'point') return !!a && Math.hypot(a.x - x, a.y - y) <= 9;
    if (d.tool === 'long' || d.tool === 'short') {
      const x1 = X(d.p1.t); if (x1 == null) return false;
      const ys = [Y(d.p1.p), Y(d.sl), Y(d.tp)].filter((v) => v != null);
      return x >= x1 && x <= x1 + 200 && y >= Math.min(...ys) - tol && y <= Math.max(...ys) + tol;
    }
    if (!a || !b) return false;
    if (d.tool === 'trend') return distSeg(x, y, a, b) <= tol;
    if (d.tool === 'ray') return distSeg(x, y, a, farPoint(a, b)) <= tol;
    if (d.tool === 'rect') { const x1 = Math.min(a.x, b.x), x2 = Math.max(a.x, b.x), y1 = Math.min(a.y, b.y), y2 = Math.max(a.y, b.y); return x >= x1 - tol && x <= x2 + tol && y >= y1 - tol && y <= y2 + tol; }
    return false;
  }
  const byId = (id) => S.drawings.find((d) => d.id === id) || null;
  function pick(x, y) { for (let i = S.drawings.length - 1; i >= 0; i--) if (hitTest(S.drawings[i], x, y)) return S.drawings[i]; return null; }
  function pickHandle(d, x, y) { return handles(d).find((h) => Math.hypot(h.x - x, h.y - y) <= 9) || null; }
  function select(id) { S.selected = id; renderMarkPane(); scheduleDraw(); }
  function setTool(t) {
    S.tool = t; S.draft = null;
    document.querySelectorAll('#tools [data-tool]').forEach((b) => b.classList.toggle('on', b.dataset.tool === t));
    $('chartbox').style.cursor = t === 'cursor' ? '' : 'crosshair';
    if (t !== 'cursor') select(null);
    scheduleDraw();
  }
  function mouse(e) {
    const r = $('chartbox').getBoundingClientRect(), P = plot();
    const x = e.clientX - r.left, y = e.clientY - r.top;
    return { x, y, inPlot: x >= 0 && y >= 0 && x <= P.w && y <= P.h, t: xToTime(x), p: yToPrice(y) };
  }
  function defaultTerm(tool) {
    const kind = { trend: 'line', ray: 'line', hline: 'hline', rect: 'box', point: 'point' }[tool];
    const t = S.vocab.find((v) => v.kind === kind); return t ? t.id : null;
  }
  function addDrawing(d) {
    S.drawings.push(d); S.compare = null;
    setTool('cursor'); select(d.id);
    if (d.tool === 'long' || d.tool === 'short') { renderTradesPane(); return; }
    askTerm(d, true);
  }
  function onDown(e) {
    if (e.button !== 0) return;
    const m = mouse(e); if (!m.inPlot || m.t == null || m.p == null) return;
    if (S.tool === 'cursor') {
      const cur = byId(S.selected);
      if (cur) { const h = pickHandle(cur, m.x, m.y); if (h) { e.stopPropagation(); e.preventDefault(); S.drag = { id: cur.id, mode: h.k }; return; } }
      const d = pick(m.x, m.y);
      if (d) { e.stopPropagation(); e.preventDefault(); select(d.id); S.drag = { id: d.id, mode: 'move', L0: chart.timeScale().coordinateToLogical(m.x), P0: m.p, p1: { ...d.p1 }, p2: d.p2 ? { ...d.p2 } : null, sl: d.sl, tp: d.tp }; }
      else if (S.selected) select(null);
      return;
    }
    e.stopPropagation(); e.preventDefault();
    const base = { id: newId(), tool: S.tool, term: null, note: '', width: 2, opacity: 0.9 };
    if (S.tool === 'hline' || S.tool === 'point') { addDrawing({ ...base, term: defaultTerm(S.tool), p1: { t: tsec(m.t), p: m.p } }); return; }
    if (S.tool === 'long' || S.tool === 'short') {
      const a = atrNow() || Math.abs(m.p) * 0.005, long = S.tool === 'long';
      const now = S.all[S.cursor];
      const d = { ...base, term: 'entry', p1: { t: now ? now.time + iv() : tsec(m.t), p: m.p }, sl: long ? m.p - a : m.p + a, tp: long ? m.p + 2 * a : m.p - 2 * a, placed: now ? now.time : 0 };
      setEntryMode(d); addDrawing(d);
      return;
    }
    if (!S.draft) S.draft = { d: { ...base, term: defaultTerm(S.tool), p1: { t: tsec(m.t), p: m.p }, p2: { t: tsec(m.t), p: m.p } }, down: { x: m.x, y: m.y }, moved: false };
    else { const d = S.draft.d; d.p2 = { t: tsec(m.t), p: m.p }; S.draft = null; addDrawing(d); }
    scheduleDraw();
  }
  function onMove(e) {
    const m = mouse(e);
    if (S.draft && m.t != null && m.p != null) { S.draft.d.p2 = { t: tsec(m.t), p: m.p }; if (Math.hypot(m.x - S.draft.down.x, m.y - S.draft.down.y) > 5) S.draft.moved = true; scheduleDraw(); return; }
    if (!S.drag) return;
    const d = byId(S.drag.id); if (!d || m.t == null || m.p == null) return;
    e.preventDefault();
    const locked = d.trade && d.trade.status !== 'pending';
    if (S.drag.mode === 'move') {
      if (d.tool === 'long' || d.tool === 'short') { if (locked) return; const dp = m.p - S.drag.P0; d.p1 = { ...d.p1, p: S.drag.p1.p + dp }; d.sl = S.drag.sl + dp; d.tp = S.drag.tp + dp; setEntryMode(d); }
      else {
        const dL = chart.timeScale().coordinateToLogical(m.x) - S.drag.L0, dP = m.p - S.drag.P0;
        const shift = (p) => ({ t: tsec(logicalToTime(timeToLogical(p.t) + dL)), p: p.p + dP });
        d.p1 = shift(S.drag.p1); if (S.drag.p2) d.p2 = shift(S.drag.p2);
      }
    } else if (S.drag.mode === 'p1') d.p1 = { t: tsec(m.t), p: m.p };
    else if (S.drag.mode === 'p2') d.p2 = { t: tsec(m.t), p: m.p };
    else if (S.drag.mode === 'entry') { if (!locked) { d.p1 = { ...d.p1, p: m.p }; setEntryMode(d); } }
    else if (S.drag.mode === 'sl') d.sl = m.p;
    else if (S.drag.mode === 'tp') d.tp = m.p;
    S.compare = null; scheduleDraw();
  }
  function onUp(e) {
    if (S.draft) { if (S.draft.moved) { const m = mouse(e); if (m.t != null && m.p != null) { const d = S.draft.d; d.p2 = { t: tsec(m.t), p: m.p }; S.draft = null; addDrawing(d); } } return; }
    if (S.drag) { S.drag = null; renderMarkPane(); renderTradesPane(); }
  }
  function deleteSelected() {
    if (!S.selected) { toast('Выберите рисунок курсором (↖)'); return; }
    S.drawings = S.drawings.filter((d) => d.id !== S.selected); S.selected = null; S.compare = null;
    renderMarkPane(); renderTradesPane(); scheduleDraw();
  }
  let clearArmed = 0;
  function clearAll() {
    if (!S.drawings.length) return;
    if (Date.now() - clearArmed > 4000) { clearArmed = Date.now(); toast('Нажмите «✕» ещё раз, чтобы удалить ВСЮ разметку'); return; }
    S.drawings = []; S.selected = null; S.compare = null; renderMarkPane(); renderTradesPane(); scheduleDraw();
  }
  function onKey(e) {
    if (e.key === 'Escape' && S.modalCancel) { e.preventDefault(); S.modalCancel(); return; }
    if (e.key === 'Escape' && !$('snapmodal').classList.contains('hidden')) { e.preventDefault(); closeSnap(); return; }
    if (/INPUT|TEXTAREA|SELECT/.test((e.target && e.target.tagName) || '')) return;
    if (e.key === 'Escape') { S.draft = null; setTool('cursor'); select(null); }
    else if ((e.key === 'Delete' || e.key === 'Backspace') && S.selected) deleteSelected();
    else if (e.key === ' ') { e.preventDefault(); play(); }
    else if (e.key === 'ArrowRight') { e.preventDefault(); step(); }
  }

  /* ------------------------------------------------------------------ подпись рисунка */
  function askTerm(d, isNew) {
    const box = $('m-terms'); box.textContent = '';
    let chosen = d.term;
    const kindOf = { trend: 'line', ray: 'line', hline: 'hline', rect: 'box', point: 'point' }[d.tool];
    const sorted = S.vocab.filter((t) => t.kind !== 'trade').slice().sort((a, b) => (b.kind === kindOf) - (a.kind === kindOf));
    for (const t of sorted) {
      const el = document.createElement('span'); el.className = 'term' + (t.id === chosen ? ' on' : '') + (t.detector ? '' : ' nodet');
      el.innerHTML = '<span class="sw" style="background:' + t.color + '"></span>' + esc(t.name);
      el.title = t.hint + (t.detector ? '' : ' (своё понятие: бот пока его не рисует)');
      el.onclick = () => { chosen = t.id; box.querySelectorAll('.term').forEach((x) => x.classList.remove('on')); el.classList.add('on'); };
      box.appendChild(el);
    }
    $('m-note').value = d.note || '';
    $('modal').classList.remove('hidden'); setTimeout(() => $('m-note').focus(), 30);
    const close = () => { $('modal').classList.add('hidden'); S.modalCancel = null; };
    $('m-ok').onclick = () => { d.term = chosen; d.note = $('m-note').value.trim(); close(); renderMarkPane(); scheduleDraw(); };
    // «Отмена»: новый рисунок удаляется с графика, у существующего правка просто не применяется
    S.modalCancel = () => {
      close();
      if (isNew) { S.drawings = S.drawings.filter((x) => x.id !== d.id); if (S.selected === d.id) S.selected = null; S.compare = null; }
      renderMarkPane(); renderTradesPane(); scheduleDraw();
    };
    $('m-cancel').onclick = S.modalCancel;
  }

  /* ------------------------------------------------------------------ слепок рынка + комментарий */
  function snapshotImage() {
    // свечи (через встроенный снимок графика) + холст с линиями: ваши и бота, уровни, зоны, сделки - ровно то, что вы видите
    const shot = chart.takeScreenshot();
    const out = document.createElement('canvas'); out.width = shot.width; out.height = shot.height;
    const g = out.getContext('2d'); g.drawImage(shot, 0, 0);
    g.drawImage($('ov'), 0, 0, shot.width, shot.height);
    return out.toDataURL('image/png');
  }
  function closeSnap() { $('snapmodal').classList.add('hidden'); S.snapDraft = null; $('s-note').readOnly = false; $('s-ok').style.display = ''; }
  function openSnap() {
    if (!S.all.length) return;
    stop();
    draw();  // свежий кадр с линиями
    let image;
    try { image = snapshotImage(); } catch (e) { toast('Не удалось снять слепок: ' + e.message, 6000); return; }
    const vb = visible(), r = chart.timeScale().getVisibleLogicalRange();
    const from = r ? logicalToTime(Math.max(0, r.from)) : vb[0].time, to = Math.min(r ? logicalToTime(r.to) : vb[vb.length - 1].time, vb[vb.length - 1].time);
    const objs = (S.compare ? S.compare.objects : S.bot.objects).filter((o) => !S.hidden[o.type] && o.kind !== 'label');
    S.snapDraft = { image, symbol: S.symbol, tf: S.tf, t: vb[vb.length - 1].time, bars: vb.slice(-400).map(toRow), view: { from_t: Math.round(from), to_t: Math.round(to) },
      drawings: S.drawings.map((d) => { const { trade, ...rest } = d; return { ...rest, ...(trade ? { result: { status: trade.status, r: trade.r } } : {}) }; }),
      bot_objects: objs, lesson_id: S.lessonId || '', strategy: S.strategy };
    $('s-img').src = image; $('s-note').value = '';
    $('s-info').textContent = S.symbol + ' ' + S.tf + ' · момент ' + fmtLocal(S.snapDraft.t + iv()) + ' · ваших рисунков ' + S.drawings.length + ' · объектов бота ' + objs.length;
    $('snapmodal').classList.remove('hidden'); setTimeout(() => $('s-note').focus(), 30);
  }
  async function saveSnap() {
    const d = S.snapDraft; if (!d) return;
    const comment = $('s-note').value.trim();
    if (!comment) { toast('Напишите комментарий: что здесь важно и почему'); return; }
    $('s-ok').disabled = true;
    const r = await call('save_snapshot', { ...d, comment });
    $('s-ok').disabled = false;
    if (!r.ok) { toast('Слепок не сохранён: ' + r.error, 7000); return; }
    closeSnap();
    toast('Слепок сохранён' + (r.sent === true ? ' и отправлен на сервер' : r.sent ? ' на ПК (сервер: ' + r.sent + ')' : ''), 6000);
    renderSnapshots();
  }
  async function renderSnapshots() {
    const box = $('snaps-list'); if (!box) return;
    const r = await call('list_snapshots');
    box.textContent = '';
    if (!r.ok) { el('div', 'bad', esc(r.error), box); return; }
    if (!r.items.length) { el('div', 'muted', 'Слепков пока нет. «💬 Комментарий» вверху: картинка графика со всеми линиями + ваш комментарий к этому моменту.', box); return; }
    for (const x of r.items.slice(0, 40)) {
      const it = el('div', 'item', '<div class="grow"><div class="what">' + esc(x.symbol + ' ' + x.tf + ' · ' + fmtLocal(x.t + (IV[x.tf] || 0))) + '</div><div class="note">' + esc((x.comment || '').slice(0, 160)) + '</div></div>', box);
      it.title = 'Открыть слепок';
      it.onclick = async () => {
        const g = await call('get_snapshot', x.id); if (!g.ok) { toast(g.error, 6000); return; }
        S.snapDraft = null; $('s-img').src = g.snapshot.image; $('s-note').value = g.snapshot.comment; $('s-note').readOnly = true;
        $('s-info').textContent = g.snapshot.symbol + ' ' + g.snapshot.tf + ' · ' + fmtLocal(g.snapshot.t + (IV[g.snapshot.tf] || 0)) + (g.snapshot.author ? ' · ' + g.snapshot.author : '');
        $('s-ok').style.display = 'none'; $('snapmodal').classList.remove('hidden');
      };
    }
  }

  /* ------------------------------------------------------------------ панели */
  function el(tag, cls, html, parent) { const e = document.createElement(tag); if (cls) e.className = cls; if (html != null) e.innerHTML = html; if (parent) parent.appendChild(e); return e; }
  function renderPanels() { renderMarkPane(); renderBotPane(); renderTradesPane(); }

  function renderMarkPane() {
    const root = $('pane-mark'); root.textContent = '';
    const c = el('div', 'card', '<h3>Моя разметка</h3>', root);
    const marks = S.drawings.filter((d) => !(d.tool === 'long' || d.tool === 'short'));
    if (!marks.length) el('div', 'muted', 'Рисуйте инструментами слева: линия, уровень, луч, зона, метка. После каждого рисунка выберите понятие и напишите, почему вы так видите.', c);
    for (const d of marks) {
      const t = termOf(d.term);
      const it = el('div', 'item' + (d.id === S.selected ? ' sel' : ''), '<span class="sw" style="margin-top:5px;background:' + (t ? t.color : '#666') + '"></span><div class="grow"><div class="what">' + esc(t ? t.name : 'без подписи') +
        '</div><div class="note">' + esc(d.note || '') + '</div></div>', c);
      it.onclick = () => select(d.id);
      it.ondblclick = () => askTerm(d);
    }
    if (S.selected && byId(S.selected) && !tradeFromDrawing(byId(S.selected))) {
      const d = byId(S.selected);
      const b = el('div', 'btns', null, c);
      const e1 = el('button', 'btn small', '✎ Понятие и причина', b); e1.onclick = () => askTerm(d);
      const e2 = el('button', 'btn small', '🗑 Удалить', b); e2.onclick = deleteSelected;
    }
    const l = el('div', 'card', '<h3>Урок</h3>', root);
    const ta = el('textarea', null, null, l); ta.rows = 4; ta.placeholder = 'Общий разбор: что происходит на рынке, кто контролирует, где и почему искать вход…'; ta.value = S.notes;
    ta.oninput = () => { S.notes = ta.value; };
    const used = [...new Set(marks.map((d) => d.term).filter(Boolean))];
    if (used.length) {
      el('div', 'muted', 'Размечено ПОЛНОСТЬЮ (бот получит «лишнее» за то, чего вы не нарисовали):', l);
      const box = el('div', 'btns', null, l);
      const comp = S.complete || used;
      for (const id of used) {
        const t = termOf(id); const lab = el('label', 'chk', null, box);
        const cb = el('input', null, null, lab); cb.type = 'checkbox'; cb.checked = comp.includes(id);
        cb.onchange = () => { const cur = new Set(S.complete || used); cb.checked ? cur.add(id) : cur.delete(id); S.complete = [...cur]; };
        lab.appendChild(document.createTextNode(t ? t.name : id));
      }
    }
    const btns = el('div', 'btns', null, l);
    const sv = el('button', 'btn on', S.lessonId ? '💾 Сохранить урок' : '💾 Сохранить как урок', btns); sv.onclick = saveLesson;
    const nw = el('button', 'btn', 'Новый урок', btns); nw.onclick = () => { S.lessonId = null; S.lessonCreated = null; S.drawings = []; S.notes = ''; S.complete = null; S.compare = null; renderPanels(); scheduleDraw(); };
    const ls = el('button', 'btn', '📂 Мои уроки', btns); ls.onclick = showLessons;
    if (S.lessonId) el('div', 'muted', 'Урок: ' + esc(S.lessonId), l);
    el('div', null, null, l).id = 'lessons-list';
    const sc = el('div', 'card', '<h3>Слепки рынка с комментариями</h3>', root);
    el('div', null, null, sc).id = 'snaps-list';
    renderSnapshots();
  }

  async function showLessons() {
    const box = $('lessons-list'); box.innerHTML = '<div class="muted">загрузка…</div>';
    const r = await call('list_lessons');
    if (!r.ok) { box.innerHTML = '<div class="bad">' + esc(r.error) + '</div>'; return; }
    box.textContent = '';
    if (!r.items.length) { el('div', 'muted', 'Уроков пока нет', box); return; }
    for (const x of r.items) {
      const it = el('div', 'item', '<div class="grow"><div class="what">' + esc(x.symbol + ' ' + x.tf + ' · ' + fmtLocal(x.to_t)) + '</div><div class="note">' +
        esc(Object.entries(x.terms).map(([k, v]) => (termOf(k) ? termOf(k).name : k) + ' ' + v).join(', ') || 'без разметки') + (x.author ? ' · ' + esc(x.author) : '') + '</div></div>', box);
      it.onclick = () => openLesson(x.id);
    }
  }
  async function openLesson(id) {
    const r = await call('load_lesson', id);
    if (!r.ok) { toast(r.error, 6000); return; }
    const ls = r.lesson;
    stop();
    S.symbol = ls.symbol; S.tf = ls.tf; S.all = ls.bars.map((b) => ({ time: b.t, open: b.o, high: b.h, low: b.l, close: b.c, volume: b.v, bv: b.bv }));
    S.cursor = S.all.length - 1; S.futureEnd = false; S.digits = digitsFor(S.all[S.cursor].close);
    series.applyOptions({ priceFormat: { type: 'price', precision: S.digits, minMove: Math.pow(10, -S.digits) } });
    S.drawings = ls.drawings || []; S.notes = ls.notes || ''; S.complete = ls.complete_types && ls.complete_types.length ? ls.complete_types : null;
    S.lessonId = ls.id; S.lessonCreated = ls.created; S.compare = null; S.bot = { objects: [], events: [], trend: 0 }; S.events = []; S.seenEv = {}; S.botTrades = [];
    $('coin').value = ls.symbol; document.querySelectorAll('#tfs .btn').forEach((b) => b.classList.toggle('on', b.dataset.tf === ls.tf));
    render(false);
    const v = ls.view || {}; const L1 = timeToLogical(v.from_t || S.all[0].time), L2 = timeToLogical(v.to_t || S.all[S.cursor].time);
    chart.timeScale().setVisibleLogicalRange({ from: L1 ?? 0, to: (L2 ?? S.cursor) + 5 });
    renderPanels(); setStatus('live', 'урок ' + ls.symbol + ' ' + ls.tf); toast('Урок открыт: можно сравнить с ботом или продолжить реплей');
  }

  function lessonPayload() {
    const vb = visible(), r = chart.timeScale().getVisibleLogicalRange();
    const from = r ? logicalToTime(Math.max(0, r.from)) : vb[0].time, to = Math.min(r ? logicalToTime(r.to) : vb[vb.length - 1].time, vb[vb.length - 1].time);
    return {
      id: S.lessonId || undefined, created: S.lessonCreated || undefined, symbol: S.symbol, tf: S.tf, bars: vb.slice(-2500).map(toRow),
      drawings: S.drawings.map((d) => { const { trade, ...rest } = d; return { ...rest, ...(trade ? { result: { status: trade.status, r: trade.r } } : {}) }; }),
      notes: S.notes, view: { from_t: Math.round(from), to_t: Math.round(to) }, complete_types: S.complete || [], strategy: S.strategy, author: S.author,
      trades: S.drawings.filter(tradeFromDrawing).map((d) => ({ side: d.tool, entry: d.p1.p, sl: d.sl, tp: d.tp, ...(d.trade || {}) })),
      title: S.symbol + ' ' + S.tf + ' ' + fmtLocal(vb[vb.length - 1].time),
    };
  }
  async function saveLesson() {
    if (!S.all.length) return;
    const r = await call('save_lesson', lessonPayload());
    if (!r.ok) { toast('Урок не сохранён: ' + r.error, 7000); return; }
    S.lessonId = r.id;
    toast('Урок сохранён' + (r.sent === true ? ' и отправлен на сервер' : r.sent ? ' на ПК (сервер: ' + r.sent + ')' : ''), 6000);
    renderMarkPane();
  }

  function renderBotPane() {
    const root = $('pane-bot'); root.textContent = '';
    const c = el('div', 'card', '<h3>Как видит бот</h3>', root);
    const b = el('div', 'btns', null, c);
    const a1 = el('button', 'btn on', '👁 Показать', b); a1.title = 'Бот размечает график по свечам до текущего момента'; a1.onclick = () => { S.compare = null; analyzeNow(false); };
    const a2 = el('button', 'btn', '⚖ Сравнить с моей разметкой', b); a2.onclick = runCompare;
    const a3 = el('button', 'btn', S.botLayer ? 'Скрыть бота' : 'Показать бота', b); a3.onclick = () => { S.botLayer = !S.botLayer; renderBotPane(); scheduleDraw(); };
    const a4 = el('button', 'btn', S.mineLayer ? 'Скрыть мою разметку' : 'Показать мою разметку', b); a4.onclick = () => { S.mineLayer = !S.mineLayer; renderBotPane(); scheduleDraw(); };
    if (S.compare) {
      const rep = S.compare.report, o = rep.overall;
      const sc = el('div', 'card', '<h3>Совпадение с вашей разметкой</h3>', root);
      el('div', 'big ' + (o.f1 >= 0.7 ? 'good' : o.f1 >= 0.4 ? 'amber' : 'bad'), o.f1 == null ? '—' : Math.round(o.f1 * 100) + '%', sc);
      el('div', 'muted', '✓ совпало ' + o.tp + ' · ✗ бот не увидел ' + o.fn + ' · лишнее у бота ' + o.fp, sc);
      const tb = el('table', null, '<tr><th>Понятие</th><th>✓</th><th>✗ не увидел</th><th>лишнее</th><th>F1</th></tr>', sc);
      for (const [k, v] of Object.entries(rep.by_type)) {
        const t = termOf(k); el('tr', null, '<td>' + esc(t ? t.name : k) + '</td><td>' + v.tp + '</td><td>' + v.fn + '</td><td>' + v.fp + '</td><td>' + (v.f1 == null ? '—' : Math.round(v.f1 * 100) + '%') + '</td>', tb);
      }
      el('div', 'muted', 'На графике: ваши рисунки - «✓ бот видит» / «✗ бот не увидел», оранжевым - лишнее у бота.', sc);
    }
    const ly = el('div', 'card', '<h3>Слои бота</h3>', root);
    const counts = {}; for (const ob of (S.compare ? S.compare.objects : S.bot.objects)) counts[ob.type] = (counts[ob.type] || 0) + 1;
    for (const t of S.vocab.filter((v) => v.detector)) {
      const r = el('label', 'row chk', null, ly);
      const cb = el('input', null, null, r); cb.type = 'checkbox'; cb.checked = !S.hidden[t.id]; cb.onchange = () => { S.hidden[t.id] = !cb.checked; scheduleDraw(); };
      el('span', 'sw', null, r).style.background = t.color;
      el('span', 'grow', esc(t.name) + (counts[t.id] ? ' <span class="muted">(' + counts[t.id] + ')</span>' : ''), r);
    }
    const lg = el('div', 'card', '<h3>Бот говорит</h3>', root);
    if (!S.events.length) el('div', 'muted', 'Запустите реплей (▶) с включённым «бот в реплее»: на каждой закрытой свече бот пишет, где ищет вход, и даёт сигналы - без знания будущего.', lg);
    for (const ev of S.events.slice(0, 80)) {
      const m = el('div', 'msg ' + ev.kind + (ev.side === 'short' ? ' short' : ''), '<span class="when">' + fmtLocal(ev.t + iv()) + '</span>' + esc(ev.text) + (ev.why ? '<div class="muted">' + esc(ev.why.join(' · ')) + '</div>' : ''), lg);
      m.onclick = () => { const L = timeToLogical(ev.t + iv() / 2); if (L != null) chart.timeScale().setVisibleLogicalRange({ from: L - 80, to: L + 20 }); };
    }
  }
  async function runCompare() {
    const marks = S.drawings.filter((d) => d.term && !tradeFromDrawing(d));
    if (!marks.length) { toast('Сначала разметьте график и подпишите рисунки понятиями'); return; }
    setStatus('busy', 'сравниваю…');
    const r = await call('compare', lessonPayload(), S.strategy);
    setStatus('live', S.symbol + ' ' + S.tf);
    if (!r.ok) { toast(r.error, 6000); return; }
    const byUser = {}, byBot = {};
    for (const m of r.report.marks) { if (m.user_id) byUser[m.user_id] = m.status; if (m.bot_index != null && m.status === 'extra') byBot[m.bot_index] = 'extra'; }
    S.compare = { report: r.report, objects: r.objects, byUser, byBot };
    S.botLayer = true; renderBotPane(); scheduleDraw();
  }

  function renderTradesPane() {
    const root = $('pane-trades'); root.textContent = '';
    const mine = S.drawings.filter(tradeFromDrawing);
    const c = el('div', 'card', '<h3>Мои сделки</h3>', root);
    if (!mine.length) el('div', 'muted', 'Инструменты L / S слева: клик - цена входа (по рынку, если рядом с ценой; иначе лимит или стоп-вход), затем тяните стоп и тейк. Play - и смотрите, что будет.', c);
    let sum = 0, n = 0;
    for (const d of mine) {
      syncTrade(d); const tr = d.trade; if (tr.r != null) { sum += tr.r; n += 1; }
      const st = { pending: 'ждёт входа (' + { market: 'по рынку', limit: 'лимит', stop: 'стоп-вход' }[tr.mode] + ')', open: 'в позиции', win: 'тейк', loss: 'стоп', closed: 'закрыта' }[tr.status];
      const it = el('div', 'item' + (d.id === S.selected ? ' sel' : ''), '<div class="grow"><div class="what ' + (d.tool === 'long' ? 'good' : 'bad') + '">' + (d.tool === 'long' ? 'ЛОНГ' : 'ШОРТ') + ' · ' + st +
        (tr.r != null ? ' · <b class="' + (tr.r >= 0 ? 'good' : 'bad') + '">' + (tr.r >= 0 ? '+' : '') + tr.r.toFixed(2) + 'R</b>' : '') + '</div><div class="note">вход ' + fp(tr.fill ?? tr.entry) + ' · стоп ' + fp(tr.sl) + ' · тейк ' + fp(tr.tp) + '</div></div>', c);
      it.onclick = () => select(d.id);
      if (tr.status === 'open') { const bx = el('button', 'btn small', 'Закрыть по рынку', it); bx.onclick = (e) => { e.stopPropagation(); closeTradeNow(d); }; }
    }
    if (n) el('div', 'big ' + (sum >= 0 ? 'good' : 'bad'), 'Итого: ' + (sum >= 0 ? '+' : '') + sum.toFixed(2) + 'R за ' + n, c);
    const b = el('div', 'card', '<h3>Сделки бота в реплее</h3>', root);
    let bs = 0, bn = 0;
    if (!S.botTrades.length) el('div', 'muted', 'Бот входит по своим сигналам (по рынку на следующей свече), одна сделка за раз.', b);
    for (const tr of S.botTrades) {
      if (tr.r != null) { bs += tr.r; bn += 1; }
      el('div', 'item', '<div class="grow"><div class="what ' + (tr.side === 'long' ? 'good' : 'bad') + '">🤖 ' + (tr.side === 'long' ? 'ЛОНГ' : 'ШОРТ') + ' · ' + ({ pending: 'ждёт', open: 'в позиции', win: 'тейк', loss: 'стоп' }[tr.status] || tr.status) +
        (tr.r != null ? ' · <b class="' + (tr.r >= 0 ? 'good' : 'bad') + '">' + (tr.r >= 0 ? '+' : '') + tr.r.toFixed(2) + 'R</b>' : '') + '</div><div class="note">' + esc(tr.why || '') + '</div></div>', b);
    }
    if (bn) el('div', 'big ' + (bs >= 0 ? 'good' : 'bad'), 'Бот: ' + (bs >= 0 ? '+' : '') + bs.toFixed(2) + 'R за ' + bn, b);
  }

  async function renderStrategyPane() {
    const root = $('pane-strategy'); root.textContent = '';
    const r = await call('get_strategy', S.strategy);
    if (!r.ok) { el('div', 'bad', esc(r.error), root); return; }
    const st = r.strategy;
    const c = el('div', 'card', '<h3>Стратегия</h3>', root);
    const sel = el('select', null, null, c);
    for (const n of S.strategies) { const o = el('option', null, esc(n), sel); o.value = n; o.selected = n === st.name; }
    sel.onchange = async () => { S.strategy = sel.value; await call('set_active', sel.value); renderStrategyPane(); };
    el('div', 'muted', 'Понятия, которые бот рисует и использует (выключенные - не рисуются и не сравниваются):', c);
    for (const t of S.vocab.filter((v) => v.detector)) {
      const row = el('label', 'row chk', null, c);
      const cb = el('input', null, null, row); cb.type = 'checkbox'; cb.checked = (st.enabled || []).includes(t.id);
      cb.onchange = async () => { const en = new Set(st.enabled || []); cb.checked ? en.add(t.id) : en.delete(t.id); await call('save_strategy', { ...st, enabled: [...en] }, (cb.checked ? 'включено ' : 'выключено ') + t.name); renderStrategyPane(); };
      el('span', 'sw', null, row).style.background = t.color; el('span', 'grow', esc(t.name), row);
    }
    const tn = el('div', 'card', '<h3>Обучение на моих уроках</h3>', root);
    el('div', 'muted', 'Бот перебирает свои настройки по каждому понятию и берёт те, при которых его разметка больше всего совпадает с вашей во ВСЕХ уроках сразу. Старые уроки при этом проверяются - согласованное не ломается.', tn);
    const bt = el('div', 'btns', null, tn);
    const go = el('button', 'btn on', '🧠 Подобрать под мои уроки', bt);
    const out = el('div', null, null, tn);
    go.onclick = async () => {
      go.disabled = true; out.innerHTML = '<div class="muted">подбираю…</div>';
      const res = await call('tune', st.name); go.disabled = false;
      if (!res.ok) { out.innerHTML = '<div class="bad">' + esc(res.error) + '</div>'; return; }
      const tb = el('table', null, '<tr><th>Понятие</th><th>уроков</th><th>было</th><th>стало</th></tr>', out.firstChild ? (out.textContent = '', out) : out);
      for (const [k, v] of Object.entries(res.report)) {
        const t = termOf(k);
        el('tr', null, '<td>' + esc(t ? t.name : k) + '</td><td>' + v.lessons + '</td><td>' + Math.round(v.before.f1 * 100) + '%</td><td class="' + (v.after.f1 > v.before.f1 ? 'good' : '') + '">' + Math.round(v.after.f1 * 100) + '%</td>', tb);
      }
      if (res.skipped_terms && res.skipped_terms.length) el('div', 'muted', 'Без детектора (нужен код): ' + esc(res.skipped_terms.map((k) => (termOf(k) ? termOf(k).name : k)).join(', ')), out);
      el('div', 'muted', 'Уроков: ' + res.lessons + ' · ' + res.seconds + ' с', out);
      const ap = el('button', 'btn on', '✅ Применить подбор', el('div', 'btns', null, out));
      ap.onclick = async () => { const a = await call('apply_tune', st.name, res.params); toast(a.ok ? 'Настройки применены (прежние - в истории для отката)' : a.error, 6000); renderStrategyPane(); };
    };
    const nt = el('div', 'card', '<h3>Новое понятие</h3>', root);
    el('div', 'muted', 'Если в словаре нет нужного слова - добавьте. Разметка с ним копится в уроках; чтобы бот начал его рисовать, нужен новый детектор (это делает разработчик по вашим урокам).', nt);
    const nm = el('input', null, null, nt); nm.placeholder = 'Название (например «имбаланс»)';
    const kd = el('select', null, null, nt); [['box', 'область'], ['hline', 'горизонталь'], ['line', 'линия'], ['point', 'метка']].forEach(([v, n]) => { const o = el('option', null, n, kd); o.value = v; });
    const cl = el('input', null, null, nt); cl.type = 'color'; cl.value = '#c9d1d9';
    const ad = el('button', 'btn', 'Добавить', nt);
    ad.onclick = async () => { const res = await call('add_term', nm.value, kd.value, cl.value, ''); if (!res.ok) { toast(res.error, 5000); return; } setVocab(res.vocab); toast('Понятие добавлено'); renderStrategyPane(); };
    if (st.history && st.history.length) el('div', 'muted', 'Изменений в истории: ' + st.history.length + ' (откат - во вкладке Create Strategy приложения)', root);
  }

  function setVocab(list) { S.vocab = list; S.terms = {}; for (const t of list) S.terms[t.id] = t; }

  /* ------------------------------------------------------------------ запуск */
  async function init() {
    const tfs = $('tfs');
    TFS.forEach((tf) => { const b = document.createElement('button'); b.className = 'btn'; b.textContent = tf; b.dataset.tf = tf; b.onclick = () => switchTf(tf); tfs.appendChild(b); tfs.appendChild(document.createTextNode(' ')); });
    chart = LightweightCharts.createChart($('chart'), {
      layout: { background: { color: '#0e1117' }, textColor: '#e6e9ef', fontSize: 13, fontFamily: 'Segoe UI, Arial, sans-serif' },
      grid: { vertLines: { color: 'rgba(255,255,255,0.035)' }, horzLines: { color: 'rgba(255,255,255,0.035)' } },
      rightPriceScale: { borderColor: '#2a313c', scaleMargins: { top: 0.08, bottom: 0.2 } },
      timeScale: { borderColor: '#2a313c', timeVisible: true, secondsVisible: false, rightOffset: 12, minBarSpacing: 1.5,
        tickMarkFormatter: (t, type) => { const d = new Date(t * 1000); if (type === 0) return String(d.getFullYear()); if (type === 1) return pad(d.getMonth() + 1) + '.' + d.getFullYear(); if (type === 2) return pad(d.getDate()) + '.' + pad(d.getMonth() + 1); return pad(d.getHours()) + ':' + pad(d.getMinutes()); } },
      localization: { timeFormatter: (t) => fmtLocal(t), priceFormatter: (p) => p.toFixed(S.digits) },
      crosshair: { mode: 0 },
    });
    series = chart.addCandlestickSeries({ upColor: '#26a69a', downColor: '#ef5350', borderVisible: false, wickUpColor: '#26a69a', wickDownColor: '#ef5350' });
    vol = chart.addHistogramSeries({ priceFormat: { type: 'volume' }, priceScaleId: 'vol', lastValueVisible: false, priceLineVisible: false });
    chart.priceScale('vol').applyOptions({ scaleMargins: { top: 0.84, bottom: 0 } });
    chart.timeScale().subscribeVisibleLogicalRangeChange((r) => { scheduleDraw(); if (r && r.from < 20 && S.all.length && !S.playing) loadOlder(); });
    chart.subscribeCrosshairMove(() => scheduleDraw());
    new ResizeObserver(() => { chart.resize($('chartbox').clientWidth, $('chartbox').clientHeight); scheduleDraw(); }).observe($('chartbox'));
    document.querySelectorAll('#tools [data-tool]').forEach((b) => { b.onclick = () => setTool(b.dataset.tool); });
    $('b-del').onclick = deleteSelected; $('b-clear').onclick = clearAll;
    $('b-snap').onclick = openSnap; $('s-ok').onclick = saveSnap; $('s-cancel').onclick = closeSnap;
    const box = $('chartbox');
    box.addEventListener('mousedown', onDown, true);
    box.addEventListener('pointerdown', (e) => { const m = mouse(e); if (e.button === 0 && m.inPlot && (S.tool !== 'cursor' || pick(m.x, m.y) || (byId(S.selected) && pickHandle(byId(S.selected), m.x, m.y)))) e.stopPropagation(); }, true);
    window.addEventListener('mousemove', onMove, true); window.addEventListener('mouseup', onUp, true); window.addEventListener('keydown', onKey);
    document.querySelectorAll('.tab').forEach((t) => { t.onclick = () => {
      document.querySelectorAll('.tab').forEach((x) => x.classList.toggle('on', x === t));
      document.querySelectorAll('.pane').forEach((p) => p.classList.toggle('on', p.id === 'pane-' + t.dataset.tab));
      if (t.dataset.tab === 'strategy') renderStrategyPane();
    }; });
    $('b-play').onclick = play; $('b-step').onclick = () => { stop(); step(); };
    $('b-go').onclick = () => { const v = $('date').value; const at = v ? Math.floor(new Date(v).getTime() / 1000) : Math.floor(Date.now() / 1000); openSpot($('coin').value || S.symbol, S.tf, at).then(() => analyzeNow(false)); };
    $('b-rand').onclick = async () => { setStatus('busy', 'выбираю случайный график…'); const r = await call('random_spot'); if (!r.ok) { setStatus('off', r.error); toast(r.error, 6000); return; } S.drawings = []; S.lessonId = null; S.notes = ''; S.complete = null; await openSpot(r.symbol, r.tf, r.at_s); analyzeNow(false); };
    $('coin').onkeydown = (e) => { if (e.key === 'Enter') $('b-go').click(); };

    const boot = await call('boot').catch((e) => ({ ok: false, error: String(e) }));
    if (!boot.ok) { setStatus('off', boot.error); toast(boot.error, 8000); return; }
    setVocab(boot.vocab); S.strategies = boot.strategies; S.strategy = CFG.strategy || boot.active; S.author = boot.author || S.author;
    call('top_coins').then((r) => { if (!r.ok) return; const dl = $('coins'); dl.textContent = ''; for (const c of r.items) { const o = document.createElement('option'); o.value = c.symbol; o.label = (c.volume_usdt / 1e9).toFixed(2) + ' млрд $/сутки'; dl.appendChild(o); } });
    renderPanels();
    if (CFG.lesson_id) { await openLesson(CFG.lesson_id); return; }
    const o = CFG.open || {};
    if (o.random) { $('b-rand').click(); return; }
    await openSpot(o.symbol || 'BTCUSDT', o.tf || '15m', o.at_s || Math.floor(Date.now() / 1000));
    analyzeNow(false);
  }
  window.__lab = { S, step, analyzeNow, runCompare, openSpot, draw, plot, Y, X, get chart() { return chart; } };
  init();
})();
