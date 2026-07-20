/* ==========================================================================
   XAU/USDT Perpetual Dashboard
   Chart  : TradingView widget            (symbol: BINANCE:PAXGUSDT.P)
   Data   : Binance Futures WebSocket     (stream: paxgusdt)

   NOTE ON THE SYMBOL
   Binance does not list a native "XAUUSDT" perpetual. PAXG (PAX Gold,
   1 token ≈ 1 troy oz of physical gold) is used as the closest liquid
   USDT-margined perpetual proxy for spot gold. Swap SYMBOL below if you
   move to an exchange/pair that lists XAUUSDT directly (e.g. MEXC).
   ========================================================================== */

const SYMBOL = "paxgusdt";                 // Binance Futures WS symbol (lowercase)
const TV_SYMBOL = "BINANCE:PAXGUSDT.P";    // TradingView chart symbol

// Binance migrated WS streams onto routed endpoints (/public, /market, /private).
// @depth belongs to /public. @markPrice, @aggTrade and @ticker belong to /market.
// Unrouted connections now only receive /public data, so we need two sockets.
const WS_PUBLIC_URL = `wss://fstream.binance.com/public/stream?streams=${SYMBOL}@depth20@100ms`;
const WS_MARKET_URL = `wss://fstream.binance.com/market/stream?streams=${SYMBOL}@markPrice@1s/${SYMBOL}@aggTrade/${SYMBOL}@ticker/${SYMBOL}@kline_15m/${SYMBOL}@kline_1h/${SYMBOL}@kline_4h`;

const DEPTH_ROWS = 20;      // rows shown per side in the order book list (max available from depth20 stream)
const TAPE_MAX_ROWS = 40;   // trades kept in the tape
const RECONNECT_DELAY = 2500;

// --- auto wall detection ---
const WALL_MULTIPLIER = 3;   // a row is a "wall" if its size >= 3x the average visible size
const WALL_MIN_ABS = 1.5;    // ...and at least this many PAXG, to ignore noise when the book is thin

// --- footprint (buy vs sell volume per price bucket) ---
const FOOTPRINT_BUCKET = 0.5;     // price bucket width (USDT)
const FOOTPRINT_ROWS_EACH_SIDE = 8; // buckets shown above & below current price
const FOOTPRINT_TRADE_HISTORY = 600; // how many recent trades feed the footprint

// --- trend detection (from tick-by-tick price, since we have no OHLC feed client-side) ---
const TREND_FAST_WINDOW = 40;    // ticks — short-term average (widened so tiny wobbles don't flip it)
const TREND_SLOW_WINDOW = 200;   // ticks — longer-term average
const TREND_HYSTERESIS = 0.00006; // must clear the flip point by this much to actually change direction
const PRICE_TICK_HISTORY = 400;

const TP_RATIO = 2; // take-profit distance = this many times the stop-loss distance (1:2 risk:reward)

let trendState = null; // "up" | "down" — sticky once set, no more "flat" wobble
let lastBuyPct = 50;
let lastSellPct = 50;
let lastDemandSize = 0;
let lastSupplySize = 0;

// --- 7 Candle / 7 Naga entry technique, across 3 timeframes ---
const TIMEFRAMES = [
  { key: "m15", label: "M15", interval: "15m" },
  { key: "h1",  label: "H1",  interval: "1h"  },
  { key: "h4",  label: "H4",  interval: "4h"  }
];
let tfData = {
  m15: { history: [], current: null },
  h1:  { history: [], current: null },
  h4:  { history: [], current: null }
};

let wsPublic = null;
let wsMarket = null;
let reconnectTimerPublic = null;
let reconnectTimerMarket = null;
let connState = { public: "connecting", market: "connecting" };
let book = { bids: [], asks: [] };
let lastPrice = null;
let prevPrice = null;
let tradeHistory = []; // { price, qty, isSell }
let priceTicks = [];   // { p: price } — used for the trend indicator

/* ---------------------------------------------------------------------- */
/* TradingView chart                                                      */
/* ---------------------------------------------------------------------- */
function initChart(){
  if (typeof TradingView === "undefined"){
    setTimeout(initChart, 300);
    return;
  }
  new TradingView.widget({
    autosize: true,
    symbol: TV_SYMBOL,
    interval: "5",
    timezone: "Etc/UTC",
    theme: "dark",
    style: "1",
    locale: "ms_MY",
    toolbar_bg: "#10131a",
    enable_publishing: false,
    hide_top_toolbar: false,
    hide_legend: false,
    save_image: false,
    backgroundColor: "#10131a",
    gridColor: "rgba(255,255,255,0.04)",
    container_id: "tv_chart_container",
    studies: ["Volume@tv-basicstudies"]
  });
}

/* ---------------------------------------------------------------------- */
/* WebSocket lifecycle — two routed connections                          */
/* ---------------------------------------------------------------------- */
function connectPublic(){
  connState.public = "connecting";
  updateConnLabel();
  wsPublic = new WebSocket(WS_PUBLIC_URL);

  wsPublic.onopen = () => { connState.public = "live"; updateConnLabel(); };

  wsPublic.onmessage = (evt) => {
    let msg;
    try { msg = JSON.parse(evt.data); } catch { return; }
    const { stream, data } = msg;
    if (!stream || !data) return;
    if (stream.endsWith("@depth20@100ms")) handleDepth(data);
  };

  wsPublic.onerror = () => { connState.public = "error"; updateConnLabel(); };

  wsPublic.onclose = () => {
    connState.public = "error";
    updateConnLabel();
    clearTimeout(reconnectTimerPublic);
    reconnectTimerPublic = setTimeout(connectPublic, RECONNECT_DELAY);
  };
}

function connectMarket(){
  connState.market = "connecting";
  updateConnLabel();
  wsMarket = new WebSocket(WS_MARKET_URL);

  wsMarket.onopen = () => { connState.market = "live"; updateConnLabel(); };

  wsMarket.onmessage = (evt) => {
    let msg;
    try { msg = JSON.parse(evt.data); } catch { return; }
    const { stream, data } = msg;
    if (!stream || !data) return;
    if (stream.endsWith("@markPrice@1s")) handleMarkPrice(data);
    else if (stream.endsWith("@aggTrade")) handleTrade(data);
    else if (stream.endsWith("@ticker")) handleTicker(data);
    else if (stream.endsWith("@kline_15m")) handleKline("m15", data);
    else if (stream.endsWith("@kline_1h")) handleKline("h1", data);
    else if (stream.endsWith("@kline_4h")) handleKline("h4", data);
  };

  wsMarket.onerror = () => { connState.market = "error"; updateConnLabel(); };

  wsMarket.onclose = () => {
    connState.market = "error";
    updateConnLabel();
    clearTimeout(reconnectTimerMarket);
    reconnectTimerMarket = setTimeout(connectMarket, RECONNECT_DELAY);
  };
}

function updateConnLabel(){
  const dot = document.getElementById("connDot");
  const label = document.getElementById("connLabel");
  dot.classList.remove("live", "error");

  const { public: pub, market: mkt } = connState;
  if (pub === "live" && mkt === "live"){
    dot.classList.add("live");
    label.textContent = "Langsung";
  } else if (pub === "error" && mkt === "error"){
    dot.classList.add("error");
    label.textContent = "Terputus — cuba semula…";
  } else if (pub === "live" || mkt === "live"){
    dot.classList.add("live");
    label.textContent = "Sebahagian langsung…";
  } else {
    label.textContent = "Menyambung…";
  }
}

/* ---------------------------------------------------------------------- */
/* Order book (heatmap list) + depth map                                  */
/* ---------------------------------------------------------------------- */
function handleDepth(d){
  const bids = d.b.map(([p, q]) => [parseFloat(p), parseFloat(q)]).filter(([, q]) => q > 0);
  const asks = d.a.map(([p, q]) => [parseFloat(p), parseFloat(q)]).filter(([, q]) => q > 0);
  bids.sort((a, b) => b[0] - a[0]);
  asks.sort((a, b) => a[0] - b[0]);
  book = { bids, asks };
  renderBook();
  renderDepth();
}

function fmt(n, dp = 2){
  return n.toLocaleString("en-US", { minimumFractionDigits: dp, maximumFractionDigits: dp });
}

function renderBook(){
  const bidsEl = document.getElementById("bookBids");
  const asksEl = document.getElementById("bookAsks");
  const bids = book.bids.slice(0, DEPTH_ROWS);
  const asks = book.asks.slice(0, DEPTH_ROWS);
  const maxSize = Math.max(
    ...bids.map(b => b[1]), ...asks.map(a => a[1]), 0.0001
  );

  // average size across all visible rows — a row well above this is flagged as a "wall"
  const allSizes = [...bids.map(b => b[1]), ...asks.map(a => a[1])];
  const avgSize = allSizes.reduce((a, b) => a + b, 0) / Math.max(allSizes.length, 1);
  const wallThreshold = Math.max(avgSize * WALL_MULTIPLIER, WALL_MIN_ABS);
  const isWall = (size) => size >= wallThreshold;

  let bidTotal = 0, askTotal = 0;
  const bidsHtml = bids.map(([price, size]) => {
    bidTotal += size;
    const pct = Math.min(100, (size / maxSize) * 100);
    const wallClass = isWall(size) ? " book__row--wall" : "";
    return `<div class="book__row book__row--bid${wallClass}">
      <span class="r-bar" style="width:${pct}%"></span>
      <span class="r-price">${fmt(price)}</span>
      <span class="r-size">${fmt(size, 3)}</span>
      <span class="r-total">${fmt(bidTotal, 3)}</span>
    </div>`;
  }).join("");

  const asksHtml = asks.map(([price, size]) => {
    askTotal += size;
    const pct = Math.min(100, (size / maxSize) * 100);
    const wallClass = isWall(size) ? " book__row--wall" : "";
    return `<div class="book__row book__row--ask${wallClass}">
      <span class="r-bar" style="width:${pct}%"></span>
      <span class="r-price">${fmt(price)}</span>
      <span class="r-size">${fmt(size, 3)}</span>
      <span class="r-total">${fmt(askTotal, 3)}</span>
    </div>`;
  }).join("");

  bidsEl.innerHTML = bidsHtml;
  asksEl.innerHTML = asksHtml;

  if (bids.length && asks.length){
    const bestBid = bids[0][0], bestAsk = asks[0][0];
    const mid = (bestBid + bestAsk) / 2;
    const spread = bestAsk - bestBid;
    document.getElementById("bookMidPrice").textContent = fmt(mid);
    document.getElementById("bookSpread").textContent =
      `spread ${fmt(spread, 2)} (${((spread / mid) * 100).toFixed(3)}%)`;
  }

  renderZones(bids, asks);
}

/* ---------------------------------------------------------------------- */
/* Demand / Supply zones — the single biggest resting order on each side */
/* of the book right now. This is the order-book equivalent of "demand"  */
/* (bids = buyers waiting) and "supply" (asks = sellers waiting).        */
/* ---------------------------------------------------------------------- */
function renderZones(bids, asks){
  const demandEl = document.getElementById("demandZonePrice");
  const demandSizeEl = document.getElementById("demandZoneSize");
  const supplyEl = document.getElementById("supplyZonePrice");
  const supplySizeEl = document.getElementById("supplyZoneSize");
  if (!demandEl) return;

  if (bids.length){
    const biggestBid = bids.reduce((best, row) => row[1] > best[1] ? row : best, bids[0]);
    demandEl.textContent = fmt(biggestBid[0]);
    demandSizeEl.textContent = `${fmt(biggestBid[1], 3)} PAXG`;
    lastDemandSize = biggestBid[1];
  }
  if (asks.length){
    const biggestAsk = asks.reduce((best, row) => row[1] > best[1] ? row : best, asks[0]);
    supplyEl.textContent = fmt(biggestAsk[0]);
    supplySizeEl.textContent = `${fmt(biggestAsk[1], 3)} PAXG`;
    lastSupplySize = biggestAsk[1];
  }
}

/* ---- canvas depth map (cumulative liquidity either side of mid) ---- */
const canvas = document.getElementById("depthCanvas");
const ctx = canvas.getContext("2d");

function resizeCanvas(){
  const dpr = window.devicePixelRatio || 1;
  const rect = canvas.getBoundingClientRect();
  canvas.width = rect.width * dpr;
  canvas.height = rect.height * dpr;
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
}
window.addEventListener("resize", () => { resizeCanvas(); renderDepth(); });

function renderDepth(){
  const rect = canvas.getBoundingClientRect();
  const w = rect.width, h = rect.height;
  if (w === 0 || h === 0) return;
  ctx.clearRect(0, 0, w, h);
  if (!book.bids.length || !book.asks.length) return;

  const bids = book.bids;
  const asks = book.asks;

  // cumulative volume arrays
  let cum = 0;
  const bidCum = bids.map(([p, q]) => { cum += q; return [p, cum]; });
  cum = 0;
  const askCum = asks.map(([p, q]) => { cum += q; return [p, cum]; });

  const maxCumBid = bidCum.at(-1)[1];
  const maxCumAsk = askCum.at(-1)[1];
  const minPrice = bidCum.at(-1)[0];
  const maxPrice = askCum.at(-1)[0];
  const priceRange = Math.max(maxPrice - minPrice, 0.0001);

  const xFor = (price) => ((price - minPrice) / priceRange) * w;
  const yForSide = (vol, sideMax) => h - (vol / sideMax) * (h - 18) - 4;

  ctx.strokeStyle = "rgba(255,255,255,0.05)";
  ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.moveTo(0, h - 4);
  ctx.lineTo(w, h - 4);
  ctx.stroke();

  drawArea(bidCum, xFor, (v) => yForSide(v, maxCumBid), h, "rgba(34,168,120,0.55)", "rgba(34,168,120,0.02)", "#3fd39c");
  drawArea(askCum, xFor, (v) => yForSide(v, maxCumAsk), h, "rgba(214,75,79,0.55)", "rgba(214,75,79,0.02)", "#f26b6f");

  if (lastPrice){
    const mx = xFor(lastPrice);
    ctx.strokeStyle = "#cf9f3f";
    ctx.setLineDash([3, 3]);
    ctx.beginPath();
    ctx.moveTo(mx, 0);
    ctx.lineTo(mx, h - 4);
    ctx.stroke();
    ctx.setLineDash([]);
  }

  document.getElementById("depthAxis").innerHTML =
    `<span>${fmt(minPrice)}</span><span>tengah ${lastPrice ? fmt(lastPrice) : "—"}</span><span>${fmt(maxPrice)}</span>`;
}

function drawArea(points, xFor, yFor, h, fillTop, fillBottom, stroke){
  if (points.length < 2) return;
  const grad = ctx.createLinearGradient(0, 0, 0, h);
  grad.addColorStop(0, fillTop);
  grad.addColorStop(1, fillBottom);

  ctx.beginPath();
  ctx.moveTo(xFor(points[0][0]), h - 4);
  points.forEach(([p, v]) => ctx.lineTo(xFor(p), yFor(v)));
  ctx.lineTo(xFor(points.at(-1)[0]), h - 4);
  ctx.closePath();
  ctx.fillStyle = grad;
  ctx.fill();

  ctx.beginPath();
  points.forEach(([p, v], i) => {
    const x = xFor(p), y = yFor(v);
    i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
  });
  ctx.strokeStyle = stroke;
  ctx.lineWidth = 1.5;
  ctx.stroke();
}

/* ---------------------------------------------------------------------- */
/* Header stats: mark price / funding                                    */
/* ---------------------------------------------------------------------- */
function handleMarkPrice(d){
  const mark = parseFloat(d.p);
  const funding = parseFloat(d.r) * 100;
  document.getElementById("statMark").textContent = fmt(mark);
  const fundEl = document.getElementById("statFunding");
  fundEl.textContent = `${funding >= 0 ? "+" : ""}${funding.toFixed(4)}%`;
  fundEl.classList.toggle("up", funding >= 0);
  fundEl.classList.toggle("down", funding < 0);
}

function handleTicker(d){
  const last = parseFloat(d.c);
  const pct = parseFloat(d.P);
  document.getElementById("statChangePct").textContent = `${pct >= 0 ? "+" : ""}${pct.toFixed(2)}%`;
  const pctEl = document.getElementById("statChangePct");
  pctEl.classList.toggle("up", pct >= 0);
  pctEl.classList.toggle("down", pct < 0);
  document.getElementById("statHigh").textContent = fmt(parseFloat(d.h));
  document.getElementById("statLow").textContent = fmt(parseFloat(d.l));
  document.getElementById("statVolume").textContent = fmt(parseFloat(d.v), 1);
  setLastPrice(last);
}

/* ---------------------------------------------------------------------- */
/* Trade tape                                                             */
/* ---------------------------------------------------------------------- */
function handleTrade(d){
  const price = parseFloat(d.p);
  const qty = parseFloat(d.q);
  const isSell = d.m === true; // buyer is maker -> aggressor sold
  setLastPrice(price);

  tradeHistory.push({ price, qty, isSell });
  if (tradeHistory.length > FOOTPRINT_TRADE_HISTORY) tradeHistory.shift();
  renderFootprint();
  renderPressure();

  const row = document.createElement("div");
  row.className = "tape__row";
  const time = new Date(d.T).toLocaleTimeString("en-GB", { hour12: false });
  row.innerHTML = `
    <span class="t-time">${time}</span>
    <span class="t-price ${isSell ? "sell" : "buy"}">${fmt(price)}</span>
    <span class="t-qty">${fmt(qty, 3)}</span>
    <span class="t-side ${isSell ? "sell" : "buy"}">${isSell ? "Jual" : "Beli"}</span>
  `;
  const tape = document.getElementById("tapeBody");
  tape.prepend(row);
  while (tape.children.length > TAPE_MAX_ROWS) tape.lastChild.remove();
}

/* ---------------------------------------------------------------------- */
/* Footprint — aggregates recent trades into buy/sell volume per price   */
/* bucket, centred on the current price. Classic footprint-chart reading:*/
/* green (buy) bar right, red (sell) bar left, delta = buy minus sell.   */
/* ---------------------------------------------------------------------- */
function renderFootprint(){
  const body = document.getElementById("footprintBody");
  if (!lastPrice || tradeHistory.length === 0){
    body.innerHTML = `<div class="footprint__empty">Menunggu dagangan…</div>`;
    return;
  }

  const bucketOf = (price) => Math.round(price / FOOTPRINT_BUCKET) * FOOTPRINT_BUCKET;
  const buckets = new Map(); // bucketPrice -> { buy, sell }

  for (const t of tradeHistory){
    const b = bucketOf(t.price);
    if (!buckets.has(b)) buckets.set(b, { buy: 0, sell: 0 });
    const entry = buckets.get(b);
    if (t.isSell) entry.sell += t.qty; else entry.buy += t.qty;
  }

  const currentBucket = bucketOf(lastPrice);
  const rows = [];
  for (let i = FOOTPRINT_ROWS_EACH_SIDE; i >= -FOOTPRINT_ROWS_EACH_SIDE; i--){
    const p = +(currentBucket + i * FOOTPRINT_BUCKET).toFixed(2);
    const entry = buckets.get(p) || { buy: 0, sell: 0 };
    rows.push({ price: p, ...entry });
  }

  const maxVol = Math.max(...rows.map(r => Math.max(r.buy, r.sell)), 0.001);

  body.innerHTML = rows.map(r => {
    const buyPct = Math.min(100, (r.buy / maxVol) * 100);
    const sellPct = Math.min(100, (r.sell / maxVol) * 100);
    const delta = r.buy - r.sell;
    const deltaClass = delta > 0 ? "pos" : delta < 0 ? "neg" : "";
    const isCurrent = r.price === currentBucket;
    return `<div class="footprint__row">
      <div class="fp-sell-bar-wrap">
        ${r.sell > 0 ? `<span class="fp-vol fp-vol--sell">${fmt(r.sell, 2)}</span><span class="fp-bar fp-bar--sell" style="width:${sellPct}%"></span>` : ""}
      </div>
      <div class="fp-price${isCurrent ? " current" : ""}">${fmt(r.price)}</div>
      <div class="fp-buy-bar-wrap">
        ${r.buy > 0 ? `<span class="fp-bar fp-bar--buy" style="width:${buyPct}%"></span><span class="fp-vol fp-vol--buy">${fmt(r.buy, 2)}</span>` : ""}
      </div>
      <div class="fp-delta ${deltaClass}">${delta >= 0 ? "+" : ""}${fmt(delta, 2)}</div>
    </div>`;
  }).join("");
}

function setLastPrice(price){
  prevPrice = lastPrice;
  lastPrice = price;
  const el = document.getElementById("statLast");
  el.textContent = fmt(price);
  if (prevPrice != null){
    el.classList.toggle("up", price >= prevPrice);
    el.classList.toggle("down", price < prevPrice);
  }

  priceTicks.push({ p: price });
  if (priceTicks.length > PRICE_TICK_HISTORY) priceTicks.shift();
  renderTrend();
  renderRoundNumbers();
}

/* ---------------------------------------------------------------------- */
/* Big Round Number — nearest psychologically "round" price levels just  */
/* below and above the current price. Multiples of 100 are the strongest */
/* (heaviest psychological pull), then 50, then plain multiples of 10.   */
/* ---------------------------------------------------------------------- */
function roundNumberTier(n){
  if (Math.round(n) % 100 === 0) return "Kuat";
  if (Math.round(n) % 50 === 0) return "Sederhana";
  return "Lemah";
}

function renderRoundNumbers(){
  const belowPriceEl = document.getElementById("roundBelowPrice");
  if (!belowPriceEl || lastPrice == null) return;

  const below = Math.floor(lastPrice / 10) * 10;
  const above = below + 10;

  belowPriceEl.textContent = fmt(below);
  document.getElementById("roundBelowHint").textContent =
    `${roundNumberTier(below)} · jarak ${fmt(lastPrice - below)}`;

  document.getElementById("roundAbovePrice").textContent = fmt(above);
  document.getElementById("roundAboveHint").textContent =
    `${roundNumberTier(above)} · jarak ${fmt(above - lastPrice)}`;
}

/* ---------------------------------------------------------------------- */
/* Trend indicator — fast vs slow average of recent tick prices          */
/* ---------------------------------------------------------------------- */
function renderTrend(){
  const badge = document.getElementById("trendBadge");
  const icon = document.getElementById("trendIcon");
  const label = document.getElementById("trendLabel");
  const sub = document.getElementById("trendSub");

  if (priceTicks.length < TREND_FAST_WINDOW + 5){
    badge.className = "trend-badge";
    icon.textContent = "…";
    label.textContent = "Mengumpul data…";
    sub.textContent = `${priceTicks.length}/${TREND_FAST_WINDOW + 5} tick`;
    return;
  }

  const avg = (arr) => arr.reduce((a, b) => a + b.p, 0) / arr.length;
  const fastAvg = avg(priceTicks.slice(-TREND_FAST_WINDOW));
  const slowAvg = avg(priceTicks.slice(-Math.min(TREND_SLOW_WINDOW, priceTicks.length)));
  const diffPct = (fastAvg - slowAvg) / slowAvg;

  if (trendState === null){
    trendState = diffPct >= 0 ? "up" : "down";
  } else if (trendState === "up" && diffPct < -TREND_HYSTERESIS){
    trendState = "down";
  } else if (trendState === "down" && diffPct > TREND_HYSTERESIS){
    trendState = "up";
  }

  badge.className = "trend-badge";
  if (trendState === "up"){
    badge.classList.add("up");
    icon.textContent = "▲";
    label.textContent = "TREN NAIK";
  } else {
    badge.classList.add("down");
    icon.textContent = "▼";
    label.textContent = "TREN TURUN";
  }
  sub.textContent = `EMA${TREND_FAST_WINDOW} vs EMA${TREND_SLOW_WINDOW} · ${diffPct >= 0 ? "+" : ""}${(diffPct * 100).toFixed(3)}%`;
}

/* ---------------------------------------------------------------------- */
/* Buyer vs seller pressure — share of buy vs sell volume in recent      */
/* trade history (same window that feeds the footprint).                 */
/* ---------------------------------------------------------------------- */
function renderPressure(){
  if (tradeHistory.length === 0) return;
  let buyVol = 0, sellVol = 0;
  for (const t of tradeHistory){
    if (t.isSell) sellVol += t.qty; else buyVol += t.qty;
  }
  const total = buyVol + sellVol || 1;
  const buyPct = (buyVol / total) * 100;
  const sellPct = 100 - buyPct;
  lastBuyPct = buyPct;
  lastSellPct = sellPct;

  document.getElementById("buyerFill").style.width = `${buyPct}%`;
  document.getElementById("sellerFill").style.width = `${sellPct}%`;
  document.getElementById("buyerPct").textContent = `${buyPct.toFixed(0)}%`;
  document.getElementById("sellerPct").textContent = `${sellPct.toFixed(0)}%`;
}

/* ---------------------------------------------------------------------- */
/* 7 Candle / 7 Naga entry technique — across M15, H1 and H4              */
/* For each timeframe we look at the last 7 CLOSED candles:               */
/*   7/7 same colour  -> "7 NAGA" (strongest signal on that timeframe)    */
/*   5-6/7 same colour -> "Kukuh" (moderate signal)                       */
/*   otherwise         -> "Campuran" (no lean on that timeframe)          */
/* The overall entry badge fires when at least 2 of the 3 timeframes      */
/* lean the same direction — cross-timeframe confluence, not just one     */
/* chart in isolation.                                                    */
/* ---------------------------------------------------------------------- */
async function fetchInitialCandles(){
  // Backfills recent closed candles via REST on page load, for every
  // timeframe. Mobile tabs often get discarded in the background (screen
  // lock, app switch), wiping all in-memory JS state — without this the
  // signal would have to rebuild from zero every single time that happens.
  for (const tf of TIMEFRAMES){
    try {
      const url = `https://fapi.binance.com/fapi/v1/klines?symbol=${SYMBOL.toUpperCase()}&interval=${tf.interval}&limit=9`;
      const res = await fetch(url);
      const rows = await res.json();
      const now = Date.now();
      tfData[tf.key].history = rows
        .filter(k => k[6] < now) // closeTime in the past = candle is actually closed
        .map(k => ({
          open: parseFloat(k[1]),
          high: parseFloat(k[2]),
          low: parseFloat(k[3]),
          close: parseFloat(k[4])
        }));
    } catch (e){
      console.error(`Gagal tarik sejarah candle ${tf.label}:`, e);
      // not fatal — the live kline stream will still fill it in over time
    }
  }
  renderSignal();
}

function handleKline(tfKey, d){
  const k = d.k;
  const candle = {
    open: parseFloat(k.o),
    close: parseFloat(k.c),
    high: parseFloat(k.h),
    low: parseFloat(k.l),
    closeTime: k.T
  };
  const slot = tfData[tfKey];
  if (k.x){
    slot.history.push(candle);
    if (slot.history.length > 12) slot.history.shift();
    slot.current = null;
  } else {
    slot.current = candle;
  }
  renderSignal();
}

// Classifies one timeframe's last 7 closed candles.
function evalTimeframe(history){
  if (history.length < 7) return { state: "collecting", count: history.length };
  const last7 = history.slice(-7);
  const greenCount = last7.filter(c => c.close >= c.open).length;
  const redCount = 7 - greenCount;
  if (greenCount === 7) return { state: "naga", dir: "up", last7 };
  if (redCount === 7) return { state: "naga", dir: "down", last7 };
  if (greenCount >= 5) return { state: "kukuh", dir: "up", last7 };
  if (redCount >= 5) return { state: "kukuh", dir: "down", last7 };
  return { state: "campuran", last7 };
}

// Preview version — swaps in the still-forming candle as the 7th one, so
// we can show an early, UNCONFIRMED read of where this timeframe is
// heading before its candle actually closes. Never used for the official
// entry badge — preview only.
function evalTimeframePreview(history, current){
  if (!current || history.length < 6) return null;
  const combined = [...history.slice(-6), current];
  return evalTimeframe(combined);
}

function renderSignal(){
  const badge = document.getElementById("signalBadge");
  const icon = document.getElementById("signalIcon");
  const label = document.getElementById("signalLabel");
  const sub = document.getElementById("signalSub");
  const tfGridEl = document.getElementById("tfGrid");
  const checklistEl = document.getElementById("signalChecklist");
  const liveEl = document.getElementById("signalLive");
  if (!badge) return;

  const evals = {};
  for (const tf of TIMEFRAMES) evals[tf.key] = evalTimeframe(tfData[tf.key].history);

  // render the 3 timeframe cards
  tfGridEl.innerHTML = TIMEFRAMES.map(tf => {
    const ev = evals[tf.key];
    if (ev.state === "collecting"){
      return `<div class="tf-card collecting">
        <div class="tf-card__label">${tf.label}</div>
        <div class="tf-card__status">Mengumpul ${ev.count}/7</div>
      </div>`;
    }
    const dots = ev.last7.map(c => {
      const isGreen = c.close >= c.open;
      return `<span class="tf-dot ${isGreen ? "up" : "down"}"></span>`;
    }).join("");
    const stateLabel = ev.state === "naga"
      ? `7 NAGA ${ev.dir === "up" ? "▲" : "▼"}`
      : ev.state === "kukuh"
      ? `Kukuh ${ev.dir === "up" ? "▲" : "▼"}`
      : "Campuran";
    const cardClass = ev.state === "campuran" ? "neutral" : ev.dir;
    return `<div class="tf-card ${cardClass}">
      <div class="tf-card__label">${tf.label}</div>
      <div class="tf-card__dots">${dots}</div>
      <div class="tf-card__status">${stateLabel}</div>
    </div>`;
  }).join("");

  // overall vote across the 3 timeframes
  const dirs = TIMEFRAMES.map(tf => evals[tf.key].dir).filter(Boolean);
  const upVotes = dirs.filter(d => d === "up").length;
  const downVotes = dirs.filter(d => d === "down").length;
  const nagaWinDir = TIMEFRAMES.some(tf => evals[tf.key].state === "naga" && evals[tf.key].dir === "up")
    ? "up"
    : TIMEFRAMES.some(tf => evals[tf.key].state === "naga" && evals[tf.key].dir === "down")
    ? "down"
    : null;

  let wantUp = null;
  if (upVotes >= 2) wantUp = true;
  else if (downVotes >= 2) wantUp = false;

  const entryPriceEl = document.getElementById("entryPriceValue");
  const entryHintEl = document.getElementById("entryPriceHint");
  const stopPriceEl = document.getElementById("stopLossValue");
  const stopHintEl = document.getElementById("stopLossHint");
  const tpPriceEl = document.getElementById("takeProfitValue");
  const tpHintEl = document.getElementById("takeProfitHint");

  badge.className = "signal-badge";
  if (wantUp === true){
    badge.classList.add("buy");
    icon.textContent = "▲";
    label.textContent = "ENTRY BELI";
  } else if (wantUp === false){
    badge.classList.add("sell");
    icon.textContent = "▼";
    label.textContent = "ENTRY JUAL";
  } else {
    badge.classList.add("none");
    icon.textContent = "○";
    label.textContent = "TIADA ISYARAT";
    sub.textContent = "Tempoh masa tak sejajar — perlu 2 dari 3 (M15/H1/H4) sama arah";
    entryPriceEl.textContent = "—";
    entryHintEl.textContent = "Menunggu isyarat";
    stopPriceEl.textContent = "—";
    stopHintEl.textContent = "Menunggu isyarat";
    tpPriceEl.textContent = "—";
    tpHintEl.textContent = "Menunggu isyarat";
  }

  if (wantUp !== null){
    const votes = wantUp ? upVotes : downVotes;
    const trendOk = wantUp ? trendState === "up" : trendState === "down";
    const pressureVal = wantUp ? lastBuyPct : lastSellPct;
    const pressureOk = pressureVal > 55;

    // weighted confidence: timeframe alignment carries the most weight,
    // since this technique is fundamentally about multi-timeframe agreement
    const tfScore = (votes / 3) * 100;
    const nagaBonus = (nagaWinDir === (wantUp ? "up" : "down")) ? 10 : 0;
    const trendScore = trendOk ? 100 : 0;
    const pressureScore = Math.max(0, Math.min(100, (pressureVal - 50) * 2));
    const zoneFavor = wantUp ? lastDemandSize : lastSupplySize;
    const zoneAgainst = wantUp ? lastSupplySize : lastDemandSize;
    const zoneTotal = zoneFavor + zoneAgainst || 1;
    const zoneScore = (zoneFavor / zoneTotal) * 100;
    const confidence = Math.min(100, Math.round(
      tfScore * 0.35 + trendScore * 0.25 + pressureScore * 0.25 + zoneScore * 0.15 + nagaBonus
    ));

    const confLabel = confidence >= 70 ? "Keyakinan Tinggi"
      : confidence >= 40 ? "Keyakinan Sederhana"
      : "Keyakinan Rendah";
    const confClass = confidence >= 70 ? "yes" : confidence >= 40 ? "" : "no";

    sub.innerHTML = `${votes}/3 tempoh masa sejajar ${wantUp ? "NAIK" : "TURUN"} · <b class="confidence-inline ${confClass}">${confidence}% ${confLabel}</b>`;

    // stop-loss reference from whichever timeframe actually leans our way,
    // preferring H4 (widest) if it agrees, else H1, else M15
    const preferOrder = ["h4", "h1", "m15"];
    let refTf = null;
    for (const key of preferOrder){
      if (evals[key].dir === (wantUp ? "up" : "down")){ refTf = key; break; }
    }
    const refHistory = refTf ? evals[refTf].last7 : evals.h1.last7 || evals.m15.last7;
    const refPrice = wantUp
      ? Math.min(...refHistory.map(c => c.low))
      : Math.max(...refHistory.map(c => c.high));
    const refLabel = TIMEFRAMES.find(t => t.key === refTf)?.label || "H1";

    // fill the persistent entry / stop-loss / take-profit badges
    entryPriceEl.textContent = fmt(lastPrice);
    entryHintEl.textContent = "Harga semasa isyarat";
    stopPriceEl.textContent = fmt(refPrice);
    const stopDistance = Math.abs(lastPrice - refPrice);
    stopHintEl.textContent = `${refLabel} · jarak ${fmt(stopDistance)}`;

    const tpPrice = wantUp
      ? lastPrice + stopDistance * TP_RATIO
      : lastPrice - stopDistance * TP_RATIO;
    tpPriceEl.textContent = fmt(tpPrice);
    tpHintEl.textContent = `Nisbah 1:${TP_RATIO} · jarak ${fmt(stopDistance * TP_RATIO)}`;

    checklistEl.innerHTML = `
      <div class="signal-check yes">
        <span>✓</span> Tempoh masa sejajar (${votes}/3) — ${tfScore.toFixed(0)}%
      </div>
      <div class="signal-check ${trendOk ? "yes" : "no"}">
        <span>${trendOk ? "✓" : "✗"}</span> Badge Tren sejajar (${trendState === "up" ? "NAIK" : "TURUN"}) — ${trendScore}%
      </div>
      <div class="signal-check ${pressureOk ? "yes" : "no"}">
        <span>${pressureOk ? "✓" : "✗"}</span> ${wantUp ? "Pembeli" : "Penjual"} ${pressureVal.toFixed(0)}% — ${pressureScore.toFixed(0)}%
      </div>
      <div class="signal-check ${zoneScore > 50 ? "yes" : "no"}">
        <span>${zoneScore > 50 ? "✓" : "✗"}</span> Zon ${wantUp ? "Demand" : "Supply"} lebih besar — ${zoneScore.toFixed(0)}%
      </div>
      <div class="signal-check hint">
        <span>ℹ</span> Rujukan stop-loss (${refLabel}): ${wantUp ? "bawah" : "atas"} ${fmt(refPrice)}
      </div>
      <div class="signal-check hint">
        <span>ℹ</span> Skor confluence — bukan arahan saiz kemasukan
      </div>
    `;
  } else {
    checklistEl.innerHTML = "";
  }

  const liveH1 = tfData.h1.current;
  if (liveH1){
    const isGreen = liveH1.close >= liveH1.open;
    liveEl.innerHTML = `Candle H1 semasa (belum tutup): <span class="${isGreen ? "up" : "down"}">${isGreen ? "▲ hijau" : "▼ merah"} — belum sah</span>`;
  } else {
    liveEl.textContent = "";
  }

  // ---- early preview: peek at still-forming candles, unconfirmed ----
  const previewEl = document.getElementById("signalPreview");
  const previewEvals = {};
  for (const tf of TIMEFRAMES){
    previewEvals[tf.key] = evalTimeframePreview(tfData[tf.key].history, tfData[tf.key].current);
  }
  const previewDirs = TIMEFRAMES.map(tf => previewEvals[tf.key]?.dir).filter(Boolean);
  const previewUpVotes = previewDirs.filter(d => d === "up").length;
  const previewDownVotes = previewDirs.filter(d => d === "down").length;
  let previewWantUp = null;
  if (previewUpVotes >= 2) previewWantUp = true;
  else if (previewDownVotes >= 2) previewWantUp = false;

  // only show the preview if it points somewhere the CONFIRMED badge
  // doesn't already show — no point previewing what's already official
  if (previewWantUp !== null && previewWantUp !== wantUp){
    const contributingTfs = TIMEFRAMES.filter(tf => previewEvals[tf.key]?.dir === (previewWantUp ? "up" : "down"));
    const soonestCloseTime = Math.min(...contributingTfs.map(tf => tfData[tf.key].current?.closeTime || Infinity));
    const msLeft = soonestCloseTime - Date.now();
    const minsLeft = Math.max(0, Math.floor(msLeft / 60000));
    const secsLeft = Math.max(0, Math.floor((msLeft % 60000) / 1000));
    const tfNames = contributingTfs.map(tf => tf.label).join(" + ");
    previewEl.className = `signal-preview ${previewWantUp ? "up" : "down"}`;
    previewEl.innerHTML = `⚠ PRATONTON (belum sah): jika ${tfNames} tutup macam sekarang, isyarat ${previewWantUp ? "BELI" : "JUAL"} berpotensi keluar dalam ~${minsLeft}m ${secsLeft}s. Candle boleh berubah sebelum tutup — jangan masuk berdasarkan ini sahaja.`;
  } else {
    previewEl.className = "signal-preview";
    previewEl.textContent = "";
  }
}

/* ---------------------------------------------------------------------- */
/* Clock + boot                                                           */
/* ---------------------------------------------------------------------- */
function tickClock(){
  document.getElementById("clock").textContent =
    new Date().toLocaleString("ms-MY", { hour12: false });
}

window.addEventListener("DOMContentLoaded", () => {
  initChart();
  resizeCanvas();
  connectPublic();
  connectMarket();
  fetchInitialCandles();
  tickClock();
  setInterval(tickClock, 1000);
  setInterval(renderSignal, 15000); // keeps the preview countdown fresh
});
