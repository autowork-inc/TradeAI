/* TradeAI: интерактивный график сделки на TradingView Lightweight Charts 4.1 (окно pywebview, отдельный процесс).
 * Данные: свечи Binance Futures (история - через мост Python, реальное время - WebSocket kline с резервным опросом) либо сервер мониторинга (MT5-символы).
 * Рисунки пользователя - холст поверх графика; координаты (время в секундах UTC, цена) переживают смену таймфрейма; хранение - JSON рядом с .exe (мост Python).
 * Разметка бота (CHR) и геометрия сигнала рисуются на том же холсте. Время на осях - в поясе ПК. */
(function () {
  'use strict';
  const CFG = window.__CFG__;
  const spec = CFG.spec;
  const $ = (id) => document.getElementById(id);
  const IV = { '1m': 60, '5m': 300, '15m': 900, '1h': 3600, '4h': 14400 };
  const SWATCHES = ['#ffb300', '#4aa3ff', '#2bb673', '#e5534b', '#b07cf0', '#ffffff', '#4cd1c7', '#f08a4b'];
  const WIDTHS = [1, 2, 3, 4, 6];
  const pad = (n) => String(n).padStart(2, '0');
  const S = {
    interval: CFG.interval, bars: [], gen: 0, ws: null, wsTimer: null, pollTimer: null, lastMsg: 0, loadingOlder: false, oldestReached: false,
    tool: 'cursor', style: { color: '#ffb300', width: 2, opacity: 0.9 }, drawings: [], selected: null, draft: null, drag: null,
    showBot: true, fav: !!(CFG.favorite && CFG.favorite.is_favorite), saveTimer: null, raf: 0, markers: [], priceLines: [],
  };
  let chart, series, vol, ready = null;

  /* ------------------------------------------------------------------ мост Python */
  function bridge() {
    if (ready) return ready;
    ready = new Promise((resolve) => {
      if (window.pywebview && window.pywebview.api) return resolve(window.pywebview.api);
      window.addEventListener('pywebviewready', () => resolve(window.pywebview.api));
      setTimeout(() => resolve(null), 8000);
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
    const t = $('toast');
    t.textContent = text; t.style.display = 'block';
    clearTimeout(toastTimer); toastTimer = setTimeout(() => { t.style.display = 'none'; }, ms || 4000);
  }
  function setStatus(kind, text) {
    $('dot').className = kind; $('stext').textContent = text;
  }

  /* ------------------------------------------------------------------ время и цены */
  const fmtLocal = (sec) => { const d = new Date(sec * 1000); return pad(d.getDate()) + '.' + pad(d.getMonth() + 1) + ' ' + pad(d.getHours()) + ':' + pad(d.getMinutes()); };
  const ivSec = () => IV[S.interval] || 900;
  /* Время <-> логический индекс свечи. Свеча занимает интервал [open, open + iv), её тело нарисовано в центре (open + iv/2), поэтому индекс считается от СЕРЕДИНЫ свечи:
     иначе точка рисунка, привязанная к абсолютному времени, при смене таймфрейма сдвигалась бы на полсвечи нового таймфрейма. Рисунки хранят только (время UTC, цена). */
  // <time-mapping>
  const barIv = () => S.barsIv || ivSec();
  const specIv = () => IV[CFG.interval] || ivSec();  // длина свечи таймфрейма СДЕЛКИ: правый край зон/линий сигнала не зависит от выбранного сейчас таймфрейма
  function rawToLogical(t) {
    const b = S.bars, n = b.length, iv = barIv();
    if (!n) return null;
    if (t <= b[0].time) return (t - b[0].time) / iv;
    if (t >= b[n - 1].time) return n - 1 + (t - b[n - 1].time) / iv;
    let lo = 0, hi = n - 1;
    while (hi - lo > 1) { const m = (lo + hi) >> 1; if (b[m].time <= t) lo = m; else hi = m; }
    return lo + (t - b[lo].time) / (b[hi].time - b[lo].time);
  }
  function rawToTime(L) {
    const b = S.bars, n = b.length, iv = barIv();
    if (!n) return null;
    if (L <= 0) return b[0].time + L * iv;
    if (L >= n - 1) return b[n - 1].time + (L - (n - 1)) * iv;
    const i = Math.floor(L);
    return b[i].time + (L - i) * (b[i + 1].time - b[i].time);
  }
  const timeToLogical = (t) => rawToLogical(t - barIv() / 2);
  const logicalToTime = (L) => { const r = rawToTime(L); return r == null ? null : r + barIv() / 2; };
  // </time-mapping>
  /* Координата времени рисунка - СТРОГИЙ Unix-timestamp в секундах UTC (целое число): не индекс свечи, не строка, не миллисекунды.
     Индексы свечей живут только внутри одного вызова отрисовки (logical <-> пиксели) и никогда не сохраняются. */
  const tsec = (t) => (t == null || !isFinite(t) ? null : Math.round(Math.abs(t) > 1e11 ? t / 1000 : t));
  const normPoint = (p) => (p && isFinite(p.p) && tsec(p.t) > 1e8 ? { t: tsec(p.t), p: +p.p } : null);
  function normDrawings(list) { // загруженные рисунки -> секунды UTC; записи без валидных точек (старые «индексы свечей» < 1e8) отбрасываются
    const out = [];
    for (const d of list || []) {
      const p1 = normPoint(d.p1), p2 = d.p2 ? normPoint(d.p2) : null;
      if (!p1 || (d.type !== 'hline' && !p2)) continue;
      out.push(d.type === 'hline' ? { ...d, p1 } : { ...d, p1, p2 });
    }
    return out;
  }
  const X = (sec) => { const L = timeToLogical(sec); return L == null ? null : chart.timeScale().logicalToCoordinate(L); };
  const Y = (price) => series.priceToCoordinate(price);
  const xToTime = (x) => { const L = chart.timeScale().coordinateToLogical(x); return L == null ? null : logicalToTime(L); };
  const yToPrice = (y) => series.coordinateToPrice(y);
  function snapIndex(sec) { // последняя свеча не позже sec (для маркеров)
    const b = S.bars; if (!b.length || sec < b[0].time) return -1;
    let lo = 0, hi = b.length - 1;
    while (lo < hi) { const m = (lo + hi + 1) >> 1; if (b[m].time <= sec) lo = m; else hi = m - 1; }
    return lo;
  }

  /* ------------------------------------------------------------------ данные */
  const toBar = (r) => ({ time: Math.floor(r[0] / 1000), open: +r[1], high: +r[2], low: +r[3], close: +r[4], volume: +r[5] });
  function setBars(arr) {
    S.bars = arr; S.barsIv = ivSec();  // интервал, в котором лежат свечи (S.interval меняется до их загрузки)
    series.setData(arr.map((b) => ({ time: b.time, open: b.open, high: b.high, low: b.low, close: b.close })));
    vol.setData(arr.map((b) => ({ time: b.time, value: b.volume, color: b.close >= b.open ? 'rgba(38,166,154,.35)' : 'rgba(239,83,80,.35)' })));
    applyMarkers(); scheduleDraw();
  }
  function applyKline(row) {
    const bar = toBar(row), n = S.bars.length, last = S.bars[n - 1];
    if (!last || bar.time > last.time) S.bars.push(bar);
    else if (bar.time === last.time) S.bars[n - 1] = bar;
    else return;
    series.update({ time: bar.time, open: bar.open, high: bar.high, low: bar.low, close: bar.close });
    vol.update({ time: bar.time, value: bar.volume, color: bar.close >= bar.open ? 'rgba(38,166,154,.35)' : 'rgba(239,83,80,.35)' });
    if (!last || bar.time > last.time) { applyMarkers(); }
    scheduleDraw();
  }
  function snapshotBars() { // свечи снапшота и достроенные после сигнала: показываем сразу, пока идёт загрузка (только на таймфрейме сделки)
    if (S.interval !== CFG.interval) return [];
    return spec.bars.map((b) => ({ time: Math.floor(b.t / 1000), open: b.o, high: b.h, low: b.l, close: b.c, volume: b.v || 0 })).filter((b, i, a) => i === 0 || b.time > a[i - 1].time);
  }
  async function loadInitial(keepRange) {
    const gen = ++S.gen;
    stopLive(); S.oldestReached = false; S.quiet = true;
    setStatus('', 'загрузка…');
    const wanted = keepRange;
    try {
      let rows = await call('klines', S.interval, null, 1000);
      let pages = 0;
      // история должна доходить до нужного места: до сделки (с запасом 150 свечей) или до левого края прежнего вида при смене таймфрейма (запас 50 свечей)
      const need = wanted ? wanted.from * 1000 - 50 * ivSec() * 1000 : (CFG.focus_ms && S.interval === CFG.interval ? CFG.focus_ms - 150 * ivSec() * 1000 : 0);
      while (need && rows.length && rows[0][0] > need && pages < 40) {
        const older = await call('klines', S.interval, rows[0][0] - 1, 1000);
        if (!older.length) break;
        rows = older.concat(rows); pages++;
      }
      if (gen !== S.gen) return;
      if (!rows.length) throw new Error('источник не вернул свечей');
      setBars(rows.map(toBar));
      setStatus('stale', 'история загружена');
      startLive();
    } catch (e) {
      if (gen !== S.gen) return;
      const snap = snapshotBars();
      if (snap.length) setBars(snap);
      setStatus('off', 'нет живых данных' + (snap.length ? ' · свечи снапшота' : ''));
      toast('Живые свечи недоступны: ' + (e && e.message ? e.message : e), 7000);
    }
    S.view = { keep: wanted || null };
    S.quiet = true;
    applyView();
  }
  function applyView() { // диапазон можно задать только когда у графика есть ширина (до первой раскладки WebView она нулевая)
    if (!S.view) return;
    if (chart.timeScale().width() <= 0 || !S.bars.length) { if ((S.viewTries = (S.viewTries || 0) + 1) < 600) setTimeout(applyView, 50); return; }
    S.viewTries = 0;
    const v = S.view; S.view = null;
    if (v.keep) restoreRange(v.keep); else focusTrade();
    setTimeout(() => { S.quiet = false; }, 600);
  }
  async function loadOlder() {
    if (S.loadingOlder || S.oldestReached || !S.bars.length || CFG.source.kind === 'none') return;
    S.loadingOlder = true; const gen = S.gen;
    try {
      const rows = await call('klines', S.interval, S.bars[0].time * 1000 - 1, 1000);
      if (gen !== S.gen) return;
      if (!rows.length) { S.oldestReached = true; return; }
      const older = rows.map(toBar).filter((b) => b.time < S.bars[0].time);
      if (!older.length) { S.oldestReached = true; return; }
      const r = chart.timeScale().getVisibleLogicalRange();
      setBars(older.concat(S.bars));
      if (r) chart.timeScale().setVisibleLogicalRange({ from: r.from + older.length, to: r.to + older.length });
    } catch (e) { toast('История не загружена: ' + (e && e.message ? e.message : e)); }
    finally { S.loadingOlder = false; }
  }

  /* ------------------------------------------------------------------ реальное время */
  function stopLive() {
    if (S.ws) { const w = S.ws; S.ws = null; try { w.close(); } catch (e) { /* уже закрыт */ } }
    clearTimeout(S.wsTimer); clearInterval(S.pollTimer); S.pollTimer = null;
  }
  function startLive() {
    if (CFG.source.kind === 'none') return;
    const gen = S.gen;
    if (CFG.source.kind === 'binance') openWs(gen, 1000);
    S.pollTimer = setInterval(async () => {
      if (gen !== S.gen) return;
      const wsAlive = CFG.source.kind === 'binance' && Date.now() - S.lastMsg < 8000;
      if (wsAlive) return;
      try {
        const rows = await call('klines', S.interval, null, 3);
        if (gen !== S.gen) return;
        rows.forEach(applyKline); S.lastMsg = Date.now();
      } catch (e) { /* связь: статус ниже покажет устаревание */ }
    }, 3000);
  }
  function openWs(gen, delay) {
    const sym = CFG.source.symbol.toLowerCase();
    const ws = new WebSocket('wss://fstream.binance.com/market/stream?streams=' + sym + '@kline_' + S.interval);
    S.ws = ws;
    ws.onmessage = (ev) => {
      if (gen !== S.gen) return;
      const m = JSON.parse(ev.data), k = (m.data || m).k;
      if (!k) return;
      S.lastMsg = Date.now();
      applyKline([k.t, k.o, k.h, k.l, k.c, k.v]);
    };
    ws.onclose = () => { if (S.ws === ws && gen === S.gen) { S.ws = null; S.wsTimer = setTimeout(() => openWs(gen, Math.min(delay * 2, 15000)), delay); } };
    ws.onerror = () => { try { ws.close(); } catch (e) { /* закрывается */ } };
  }
  function statusTick() {
    if (!S.lastMsg) return;
    const age = Date.now() - S.lastMsg, d = new Date(S.lastMsg);
    const clock = pad(d.getHours()) + ':' + pad(d.getMinutes()) + ':' + pad(d.getSeconds());
    if (age < 10000) setStatus('live', 'LIVE · ' + clock);
    else if (age < 60000) setStatus('stale', 'данные ' + Math.round(age / 1000) + ' с назад');
    else setStatus('off', 'нет данных ' + Math.round(age / 1000) + ' с');
  }

  /* ------------------------------------------------------------------ вид */
  function currentRangeTimes() {
    const r = chart.timeScale().getVisibleLogicalRange();
    return r ? { from: logicalToTime(r.from), to: logicalToTime(r.to) } : null;
  }
  function restoreRange(t) {
    if (!t) return;
    const a = timeToLogical(t.from), b = timeToLogical(t.to);
    if (a != null && b != null) chart.timeScale().setVisibleLogicalRange({ from: a, to: b });
  }
  function focusTrade() {
    if (S.interval === CFG.interval && spec.visible) {
      const a = timeToLogical(spec.visible[0] / 1000), b = timeToLogical(spec.visible[1] / 1000);
      if (a != null && b != null) { chart.timeScale().setVisibleLogicalRange({ from: a, to: b + 3 }); return; }
    }
    const n = S.bars.length; chart.timeScale().setVisibleLogicalRange({ from: Math.max(0, n - 120), to: n + 6 });
  }
  function jumpToTrade() {
    const t = (CFG.focus_ms || (spec.visible && spec.visible[0]) || 0) / 1000;
    if (!t) return;
    const L = timeToLogical(t), r = chart.timeScale().getVisibleLogicalRange(), w = r ? r.to - r.from : 120;
    if (L != null) chart.timeScale().setVisibleLogicalRange({ from: L - w * 0.35, to: L + w * 0.65 });
  }
  function toRealtime() { chart.timeScale().scrollToRealTime(); }

  function applyMarkers() {
    const mk = [];
    const shape = { arrow_up: 'arrowUp', arrow_down: 'arrowDown', circle: 'circle', square: 'square' };
    for (const m of (spec.trade_markers || []).concat(spec.setup_markers || [])) {
      const i = snapIndex(m.t / 1000);
      if (i < 0) continue;
      mk.push({ time: S.bars[i].time, position: m.position === 'above' ? 'aboveBar' : 'belowBar', color: m.color, shape: shape[m.shape] || 'circle', text: m.text || '' });
    }
    mk.sort((a, b) => a.time - b.time);
    series.setMarkers(mk);
  }
  function applyPriceLines() {
    const style = { solid: 0, dotted: 1, dashed: 2 };
    for (const h of spec.hlines || []) {
      series.createPriceLine({ price: h.price, color: h.color, lineWidth: Math.min(4, h.width || 2), lineStyle: style[h.style] || 0, axisLabelVisible: true, title: h.title || '' });
    }
  }

  /* ------------------------------------------------------------------ холст: разметка сделки, бота и рисунки */
  function scheduleDraw() { // rAF не вызывается в скрытом окне: страховка таймером, чтобы холст не оставался устаревшим
    if (S.raf) return;
    const run = () => { if (!S.raf) return; S.raf = 0; draw(); };
    S.raf = requestAnimationFrame(run); setTimeout(run, 60);
  }
  function rgba(hex, a) {
    const m = /^#?([0-9a-f]{6})$/i.exec(hex || ''); if (!m) return hex;
    const n = parseInt(m[1], 16); return 'rgba(' + (n >> 16) + ',' + ((n >> 8) & 255) + ',' + (n & 255) + ',' + a + ')';
  }
  function plot() { const box = $('chartbox').getBoundingClientRect(); return { w: chart.timeScale().width(), h: box.height - chart.timeScale().height(), box }; }
  function label(ctx, text, x, y, color) {
    ctx.font = '12px "Segoe UI",sans-serif';
    const w = ctx.measureText(text).width + 8;
    ctx.fillStyle = 'rgba(11,14,20,.82)'; ctx.fillRect(x, y - 9, w, 18);
    ctx.fillStyle = color; ctx.textBaseline = 'middle'; ctx.fillText(text, x + 4, y);
  }
  function lineBetween(ctx, a, b, color, width, dash) {
    ctx.beginPath(); ctx.setLineDash(dash || []); ctx.strokeStyle = color; ctx.lineWidth = width;
    ctx.moveTo(a.x, a.y); ctx.lineTo(b.x, b.y); ctx.stroke(); ctx.setLineDash([]);
  }
  function pt(p) { const x = X(p.t), y = Y(p.p); return (x == null || y == null) ? null : { x, y }; }
  function farPoint(a, b) { const dx = b.x - a.x, dy = b.y - a.y, len = Math.hypot(dx, dy) || 1, k = 20000 / len; return { x: a.x + dx * k, y: a.y + dy * k }; }

  function drawSpecLayer(ctx, P) {
    for (const b of spec.boxes || []) {
      const x1 = X(b.t1 / 1000), x2 = X(b.t2 / 1000 + specIv()), y1 = Y(b.p1), y2 = Y(b.p2);
      if (x1 == null || x2 == null || y1 == null || y2 == null) continue;
      ctx.fillStyle = b.fill; ctx.strokeStyle = b.color; ctx.lineWidth = 1;
      ctx.fillRect(Math.min(x1, x2), Math.min(y1, y2), Math.abs(x2 - x1), Math.abs(y2 - y1));
      ctx.strokeRect(Math.min(x1, x2), Math.min(y1, y2), Math.abs(x2 - x1), Math.abs(y2 - y1));
      if (b.label) label(ctx, b.label, Math.min(x1, x2) + 2, Math.min(y1, y2) - 10, b.color);
    }
    for (const g of CFG.chr ? [] : spec.segments || []) {
      // линия геометрии сигнала - ровно от (t1, p1) до (t2, p2): наклонные линии (трендовые) рисуются с настоящим наклоном, горизонтальные - p1 == p2
      const x1 = X(g.t1 / 1000), x2 = X(g.t2 / 1000 + specIv()), y1 = Y(g.price), y2 = Y(g.price2 == null ? g.price : g.price2);
      if (x1 == null || x2 == null || y1 == null || y2 == null) continue;
      lineBetween(ctx, { x: x1, y: y1 }, { x: x2, y: y2 }, g.color, 1.5, [5, 4]);
      if (g.label) label(ctx, g.label, x1 + 2, y1 - 10, g.color);
    }
    for (const v of spec.vlines || []) {
      const x = X(v.t / 1000); if (x == null) continue;
      lineBetween(ctx, { x, y: 0 }, { x, y: P.h }, v.color, 1, [4, 4]);
      if (v.text) label(ctx, v.text, x + 3, 12, v.color);
    }
  }
  function drawBot(ctx) {
    if (!S.showBot || !CFG.bot) return;
    for (const it of CFG.bot.items) {
      if (it.kind === 'zone') {
        const x1 = X(it.t1 / 1000), x2 = X(it.t2 / 1000 + specIv()), y1 = Y(it.p1), y2 = Y(it.p2);
        if (x1 == null || x2 == null || y1 == null || y2 == null) continue;
        ctx.fillStyle = it.fill; ctx.fillRect(Math.min(x1, x2), Math.min(y1, y2), Math.abs(x2 - x1), Math.max(2, Math.abs(y2 - y1)));
      } else if (it.kind === 'line') {
        const a = pt({ t: it.t1 / 1000, p: it.p1 }), b = pt({ t: it.t2 / 1000, p: it.p2 });
        if (!a || !b) continue;
        lineBetween(ctx, a, b, it.color, it.width || 2, it.dash);
        if (it.label) label(ctx, it.label, b.x + 6, b.y, it.color);
      } else if (it.kind === 'point') {
        const a = pt({ t: it.t / 1000, p: it.p }); if (!a) continue;
        ctx.beginPath(); ctx.arc(a.x, a.y, it.r || 5, 0, Math.PI * 2); ctx.fillStyle = it.color; ctx.fill();
        ctx.lineWidth = 2; ctx.strokeStyle = '#0e1117'; ctx.stroke();
        if (it.label) label(ctx, it.label, a.x + 9, a.y - 12, it.color);
      }
    }
  }
  function styleOf(d) { return { c: d.color, w: d.width, o: d.opacity }; }
  function drawOne(ctx, d, P, selected, draft) {
    const a = pt(d.p1), b = d.p2 ? pt(d.p2) : null;
    const col = rgba(d.color, d.opacity);
    if (d.type === 'hline') {
      const y = Y(d.p1.p); if (y == null) return;
      lineBetween(ctx, { x: 0, y }, { x: P.w, y }, col, d.width);
      label(ctx, d.p1.p.toFixed(CFG.digits), P.w - 64, y - 11, d.color);
    } else if (a && b) {
      if (d.type === 'trend') lineBetween(ctx, a, b, col, d.width);
      else if (d.type === 'ray') lineBetween(ctx, a, farPoint(a, b), col, d.width);
      else if (d.type === 'rect') {
        ctx.fillStyle = rgba(d.color, d.opacity * 0.35); ctx.strokeStyle = col; ctx.lineWidth = d.width;
        ctx.fillRect(Math.min(a.x, b.x), Math.min(a.y, b.y), Math.abs(b.x - a.x), Math.abs(b.y - a.y));
        ctx.strokeRect(Math.min(a.x, b.x), Math.min(a.y, b.y), Math.abs(b.x - a.x), Math.abs(b.y - a.y));
      }
    }
    if (selected) for (const h of handles(d)) {
      ctx.fillStyle = '#fff'; ctx.strokeStyle = '#4aa3ff'; ctx.lineWidth = 2;
      ctx.beginPath(); ctx.rect(h.x - 5, h.y - 5, 10, 10); ctx.fill(); ctx.stroke();
    }
  }
  function draw() {
    const P = plot(), cv = $('ov'), dpr = window.devicePixelRatio || 1;
    const bw = Math.round(P.box.width * dpr), bh = Math.round(P.box.height * dpr);
    if (cv.width !== bw || cv.height !== bh) { cv.width = bw; cv.height = bh; cv.style.width = P.box.width + 'px'; cv.style.height = P.box.height + 'px'; }
    const ctx = cv.getContext('2d');
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0); ctx.clearRect(0, 0, P.box.width, P.box.height);
    ctx.save(); ctx.beginPath(); ctx.rect(0, 0, P.w, P.h); ctx.clip();
    drawSpecLayer(ctx, P); drawBot(ctx);
    for (const d of S.drawings) drawOne(ctx, d, P, d.id === S.selected, false);
    if (S.draft) drawOne(ctx, S.draft.d, P, false, true);
    ctx.restore();
  }

  /* ------------------------------------------------------------------ рисунки: геометрия, выбор, правка */
  function handles(d) {
    const a = pt(d.p1), b = d.p2 ? pt(d.p2) : null, out = [];
    if (d.type === 'hline') { const y = Y(d.p1.p); if (y != null) out.push({ x: 24, y, k: 'p1' }); return out; }
    if (a) out.push({ ...a, k: 'p1' }); if (b) out.push({ ...b, k: 'p2' });
    if (d.type === 'rect' && a && b) { out.push({ x: a.x, y: b.y, k: 'c1' }); out.push({ x: b.x, y: a.y, k: 'c2' }); }
    return out;
  }
  function distSeg(px, py, a, b) {
    const dx = b.x - a.x, dy = b.y - a.y, l2 = dx * dx + dy * dy;
    const t = l2 ? Math.max(0, Math.min(1, ((px - a.x) * dx + (py - a.y) * dy) / l2)) : 0;
    return Math.hypot(px - (a.x + t * dx), py - (a.y + t * dy));
  }
  function hitTest(d, x, y) {
    const a = pt(d.p1), b = d.p2 ? pt(d.p2) : null, tol = 7;
    if (d.type === 'hline') { const yy = Y(d.p1.p); return yy != null && Math.abs(y - yy) <= tol; }
    if (!a || !b) return false;
    if (d.type === 'trend') return distSeg(x, y, a, b) <= tol;
    if (d.type === 'ray') return distSeg(x, y, a, farPoint(a, b)) <= tol;
    if (d.type === 'rect') {
      const x1 = Math.min(a.x, b.x), x2 = Math.max(a.x, b.x), y1 = Math.min(a.y, b.y), y2 = Math.max(a.y, b.y);
      return x >= x1 - tol && x <= x2 + tol && y >= y1 - tol && y <= y2 + tol;
    }
    return false;
  }
  function pick(x, y) { for (let i = S.drawings.length - 1; i >= 0; i--) if (hitTest(S.drawings[i], x, y)) return S.drawings[i]; return null; }
  function pickHandle(d, x, y) { return handles(d).find((h) => Math.hypot(h.x - x, h.y - y) <= 9) || null; }
  const byId = (id) => S.drawings.find((d) => d.id === id) || null;

  function persist() {
    clearTimeout(S.saveTimer);
    S.saveTimer = setTimeout(async () => {
      try { await call('save_drawings', JSON.stringify(S.drawings)); } catch (e) { toast('Рисунки не сохранены: ' + (e && e.message ? e.message : e), 6000); }
    }, 300);
  }
  function select(id) {
    S.selected = id;
    const d = byId(id);
    if (d) { S.style = { color: d.color, width: d.width, opacity: d.opacity }; }
    syncStyleUi(); scheduleDraw();
  }
  function setTool(t) {
    S.tool = t; S.draft = null;
    document.querySelectorAll('#tools [data-tool]').forEach((b) => b.classList.toggle('on', b.dataset.tool === t));
    $('chartbox').style.cursor = t === 'cursor' ? '' : 'crosshair';
    if (t !== 'cursor') select(null);
    syncStyleUi(); scheduleDraw();
  }
  function syncStyleUi() {
    $('style').style.display = (S.tool !== 'cursor' || S.selected) ? 'flex' : 'none';
    document.querySelectorAll('#swatches .sw').forEach((s) => s.classList.toggle('on', s.dataset.c.toLowerCase() === S.style.color.toLowerCase()));
    document.querySelectorAll('#widths .wbtn').forEach((b) => b.classList.toggle('on', +b.dataset.w === S.style.width));
    $('color').value = /^#[0-9a-f]{6}$/i.test(S.style.color) ? S.style.color : '#ffb300';
    $('opacity').value = Math.round(S.style.opacity * 100);
  }
  function applyStyle(patch) {
    S.style = { ...S.style, ...patch };
    const d = byId(S.selected);
    if (d) { Object.assign(d, patch); persist(); }
    syncStyleUi(); scheduleDraw();
  }
  function deleteSelected() {
    if (!S.selected) { toast('Выберите рисунок курсором (↖), затем удалите'); return; }
    S.drawings = S.drawings.filter((d) => d.id !== S.selected); S.selected = null;
    persist(); syncStyleUi(); scheduleDraw();
  }
  let clearArmed = 0;
  function clearAll() {
    if (!S.drawings.length) { toast('Рисунков нет'); return; }
    if (Date.now() - clearArmed > 4000) { clearArmed = Date.now(); toast('Нажмите «✕» ещё раз, чтобы удалить ВСЕ рисунки этой монеты'); return; }
    S.drawings = []; S.selected = null; clearArmed = 0; persist(); syncStyleUi(); scheduleDraw(); toast('Рисунки удалены');
  }
  const newId = () => 'd' + Date.now().toString(36) + Math.random().toString(36).slice(2, 6);
  function mouse(e) {
    const r = $('chartbox').getBoundingClientRect(), P = plot();
    const x = e.clientX - r.left, y = e.clientY - r.top;
    return { x, y, inPlot: x >= 0 && y >= 0 && x <= P.w && y <= P.h, t: xToTime(x), p: yToPrice(y) };
  }
  function onDown(e) {
    if (e.button !== 0) return;
    const m = mouse(e); if (!m.inPlot || m.t == null || m.p == null) return;
    if (S.tool === 'cursor') {
      const cur = byId(S.selected);
      if (cur) { const h = pickHandle(cur, m.x, m.y); if (h) { e.stopPropagation(); e.preventDefault(); S.drag = { id: cur.id, mode: h.k }; return; } }
      const d = pick(m.x, m.y);
      if (d) {
        e.stopPropagation(); e.preventDefault(); select(d.id);
        S.drag = { id: d.id, mode: 'move', L0: chart.timeScale().coordinateToLogical(m.x), P0: m.p, p1: { ...d.p1 }, p2: d.p2 ? { ...d.p2 } : null };
      } else if (S.selected) select(null);
      return;
    }
    e.stopPropagation(); e.preventDefault();
    const base = { id: newId(), type: S.tool, color: S.style.color, width: S.style.width, opacity: S.style.opacity };
    if (S.tool === 'hline') {
      S.drawings.push({ ...base, p1: { t: tsec(m.t), p: m.p } }); persist(); setTool('cursor'); return;
    }
    if (!S.draft) { S.draft = { d: { ...base, p1: { t: tsec(m.t), p: m.p }, p2: { t: tsec(m.t), p: m.p } }, down: { x: m.x, y: m.y }, moved: false }; }
    else { finishDraft(m); }
    scheduleDraw();
  }
  function finishDraft(m) {
    const d = S.draft.d; d.p2 = { t: tsec(m.t), p: m.p };
    S.drawings.push(d); S.draft = null; persist(); setTool('cursor'); select(d.id);
  }
  function onMove(e) {
    const m = mouse(e);
    if (S.draft && m.t != null && m.p != null) { S.draft.d.p2 = { t: tsec(m.t), p: m.p }; if (Math.hypot(m.x - S.draft.down.x, m.y - S.draft.down.y) > 5) S.draft.moved = true; scheduleDraw(); return; }
    if (!S.drag) return;
    const d = byId(S.drag.id); if (!d || m.t == null || m.p == null) return;
    e.preventDefault();
    if (S.drag.mode === 'move') {
      const dL = chart.timeScale().coordinateToLogical(m.x) - S.drag.L0, dP = m.p - S.drag.P0;
      const shift = (p) => ({ t: tsec(logicalToTime(timeToLogical(p.t) + dL)), p: p.p + dP });
      d.p1 = shift(S.drag.p1); if (S.drag.p2) d.p2 = shift(S.drag.p2);
    } else if (S.drag.mode === 'p1') { d.p1 = { t: tsec(m.t), p: m.p }; }
    else if (S.drag.mode === 'p2') { d.p2 = { t: tsec(m.t), p: m.p }; }
    else if (S.drag.mode === 'c1') { d.p1 = { t: d.p1.t, p: d.p1.p }; d.p2 = { t: d.p2.t, p: d.p2.p }; d.p1.t = tsec(m.t); d.p2.p = m.p; }
    else if (S.drag.mode === 'c2') { d.p2 = { t: tsec(m.t), p: d.p2.p }; d.p1 = { t: d.p1.t, p: m.p }; }
    scheduleDraw();
  }
  function onUp(e) {
    if (S.draft) { // протяжка «нажал - тянул - отпустил» завершает рисунок; одиночный клик - ждём второй
      if (S.draft.moved) { const m = mouse(e); if (m.t != null && m.p != null) finishDraft(m); }
      return;
    }
    if (S.drag) { S.drag = null; persist(); }
  }
  function onKey(e) {
    if (e.key === 'Escape') { S.draft = null; setTool('cursor'); select(null); }
    else if ((e.key === 'Delete' || e.key === 'Backspace') && S.selected) deleteSelected();
  }

  /* ------------------------------------------------------------------ панель */
  function buildPanel() {
    const root = $('panel'); root.textContent = '';
    const el = (tag, cls, text, parent) => { const e = document.createElement(tag); if (cls) e.className = cls; if (text != null) e.textContent = text; (parent || root).appendChild(e); return e; };
    const row = (parent, k, v, cls) => { const r = el('div', 'row', null, parent); el('span', null, k, r); el('span', cls || null, v, r); };
    const d = spec.panel;
    if (CFG.bot) {
      const c = el('div', 'card bot' + (CFG.bot.verified ? '' : ' bad')); el('h3', null, 'Что увидел бот', c);
      CFG.bot.notes.forEach((n) => el('div', 'step', n, c));
    }
    const res = d.result, c = el('div', 'card ' + (res.tone === 'neutral' ? '' : res.tone)); el('h3', null, 'Результат', c);
    el('div', 'big t-' + res.tone, res.r + (res.pnl ? '   ' + res.pnl : ''), c);
    row(c, 'Статус', res.status); if (res.exit_reason) row(c, 'Причина выхода', res.exit_reason);
    res.closes.forEach((x) => row(c, x.n + '. ' + x.label + '  ' + x.time + '  @ ' + x.price, x.r, 't-' + x.tone));
    if (res.warning) el('div', 'warn', res.warning, c);
    if (d.review) { const v = el('div', 'card'); el('h3', null, 'Разбор сделки', v); el('div', 'score', d.review.headline, v); d.review.points.forEach((p) => el('div', 'pt ' + p.tone, p.text, v)); }
    if (d.timeline && d.timeline.length) { const tl = el('div', 'card'); el('h3', null, 'Хронология', tl); d.timeline.forEach((e) => { const r = el('div', 'ev', null, tl); el('span', 'code t-' + e.tone, e.code, r); el('span', 'when', e.time, r); el('span', 'what', e.text, r); }); }
    const r = el('div', 'card'); el('h3', null, 'Причина входа', r); el('div', null, d.reason.title, r).style.fontWeight = '600';
    d.reason.steps.forEach((s) => el('div', 'step', s, r));
    if (d.reason.facts.length) { const f = el('div', 'card'); el('h3', null, 'Параметры сигнала', f); d.reason.facts.forEach((kv) => row(f, kv[0], kv[1])); }
    d.notes.forEach((n) => el('div', 'warn', n));
  }

  /* ------------------------------------------------------------------ избранное */
  function renderFav() { const b = $('b-fav'); b.classList.toggle('on', S.fav); b.textContent = S.fav ? '★ В избранном' : '☆ В избранное'; }
  async function toggleFav() {
    const b = $('b-fav'); b.disabled = true;
    try {
      const res = await call('toggle_favorite', !S.fav);
      if (res && res.ok) { S.fav = !!res.is_favorite; toast(S.fav ? 'Добавлено в избранное' : 'Убрано из избранного'); } else { toast('Избранное: ' + ((res && res.error) || 'ошибка'), 6000); }
    } catch (e) { toast('Избранное: ' + (e && e.message ? e.message : e), 6000); }
    b.disabled = false; renderFav();
  }

  /* ------------------------------------------------------------------ запуск */
  function init() {
    S.drawings = normDrawings(CFG.drawings);
    $('title').textContent = spec.title;
    const tfs = $('tfs');
    (CFG.timeframes || ['5m', '15m', '1h', '4h']).forEach((tf) => {
      const b = document.createElement('button'); b.className = 'btn' + (tf === S.interval ? ' on' : ''); b.textContent = tf; b.dataset.tf = tf;
      b.onclick = () => switchTf(tf); tfs.appendChild(b); tfs.appendChild(document.createTextNode(' '));
    });
    chart = LightweightCharts.createChart($('chart'), {
      layout: { background: { color: '#0e1117' }, textColor: '#e6e9ef', fontSize: 13, fontFamily: 'Segoe UI, Arial, sans-serif' },
      grid: { vertLines: { color: 'rgba(255,255,255,0.035)' }, horzLines: { color: 'rgba(255,255,255,0.035)' } },
      rightPriceScale: { borderColor: '#2a313c', scaleMargins: { top: 0.08, bottom: 0.2 } },
      timeScale: {
        borderColor: '#2a313c', timeVisible: true, secondsVisible: false, rightOffset: 6, minBarSpacing: 1.5,
        tickMarkFormatter: (t, type) => {
          const d = new Date(t * 1000);
          if (type === 0) return String(d.getFullYear());
          if (type === 1) return pad(d.getMonth() + 1) + '.' + d.getFullYear();
          if (type === 2) return pad(d.getDate()) + '.' + pad(d.getMonth() + 1);
          return pad(d.getHours()) + ':' + pad(d.getMinutes());
        },
      },
      localization: { timeFormatter: (t) => fmtLocal(t), priceFormatter: (p) => p.toFixed(CFG.digits) },
      crosshair: { mode: 0 },
    });
    series = chart.addCandlestickSeries({ upColor: '#26a69a', downColor: '#ef5350', borderVisible: false, wickUpColor: '#26a69a', wickDownColor: '#ef5350',
      priceFormat: { type: 'price', precision: CFG.digits, minMove: Math.pow(10, -CFG.digits) }, lastValueVisible: true, priceLineVisible: true });
    vol = chart.addHistogramSeries({ priceFormat: { type: 'volume' }, priceScaleId: 'vol', lastValueVisible: false, priceLineVisible: false });
    chart.priceScale('vol').applyOptions({ scaleMargins: { top: 0.84, bottom: 0 } });
    applyPriceLines();
    setInterval(statusTick, 1000);
    chart.timeScale().subscribeVisibleLogicalRangeChange((r) => { scheduleDraw(); if (r && r.from < 25 && !S.quiet && !S.view) loadOlder(); });
    chart.subscribeCrosshairMove(() => scheduleDraw());
    new ResizeObserver(() => { chart.resize($('chartbox').clientWidth, $('chartbox').clientHeight); applyView(); scheduleDraw(); }).observe($('chartbox'));

    // инструменты
    document.querySelectorAll('#tools [data-tool]').forEach((b) => { b.onclick = () => setTool(b.dataset.tool); });
    $('b-del').onclick = deleteSelected; $('b-clear').onclick = clearAll;
    SWATCHES.forEach((c) => { const s = document.createElement('span'); s.className = 'sw'; s.style.background = c; s.dataset.c = c; s.onclick = () => applyStyle({ color: c }); $('swatches').appendChild(s); });
    WIDTHS.forEach((w) => { const b = document.createElement('button'); b.className = 'btn wbtn'; b.textContent = w; b.dataset.w = w; b.onclick = () => applyStyle({ width: w }); $('widths').appendChild(b); });
    $('color').oninput = (e) => applyStyle({ color: e.target.value });
    $('opacity').oninput = (e) => applyStyle({ opacity: +e.target.value / 100 });
    const box = $('chartbox');
    box.addEventListener('mousedown', onDown, true);
    box.addEventListener('pointerdown', (e) => { if (e.button === 0 && mouse(e).inPlot && (S.tool !== 'cursor' || pickAny(e))) e.stopPropagation(); }, true); // график не должен начинать протяжку под рисунком
    window.addEventListener('mousemove', onMove, true); window.addEventListener('mouseup', onUp, true); window.addEventListener('keydown', onKey);

    $('b-trade').onclick = jumpToTrade; $('b-live').onclick = toRealtime;
    $('b-panel').onclick = () => { const p = $('panel'); p.classList.toggle('hidden'); $('b-panel').classList.toggle('on', !p.classList.contains('hidden')); setTimeout(() => chart.resize($('chartbox').clientWidth, $('chartbox').clientHeight), 30); };
    if (CFG.bot) { $('b-bot').style.display = ''; $('b-bot').onclick = () => { S.showBot = !S.showBot; $('b-bot').classList.toggle('on', S.showBot); scheduleDraw(); }; }
    if (CFG.favorite) { $('b-fav').style.display = ''; $('b-fav').onclick = toggleFav; renderFav(); }
    buildPanel(); syncStyleUi();
    S.quiet = true; setBars(snapshotBars()); S.view = { keep: null }; applyView();
    window.__dbg = { chart, S, series, vol };
    window.__selftest = () => chart.takeScreenshot().toDataURL('image/png');
    loadInitial(null).then(() => { window.__ready = true; });
  }
  function pickAny(e) { const m = mouse(e); const d = byId(S.selected); return !!(pick(m.x, m.y) || (d && pickHandle(d, m.x, m.y))); }
  function switchTf(tf) {
    if (tf === S.interval) return;
    const range = currentRangeTimes();
    S.interval = tf;
    document.querySelectorAll('#tfs .btn').forEach((b) => b.classList.toggle('on', b.dataset.tf === tf));
    loadInitial(range);
  }
  init();
})();
