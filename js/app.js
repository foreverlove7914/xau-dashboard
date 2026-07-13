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
const WS_MARKET_URL = `wss://fstream.binance.com/market/stream?streams=${SYMBOL}@markPrice@1s/${SYMBOL}@aggTrade/${SYMBOL}@ticker`;

const DEPTH_ROWS = 20;      // rows shown per side in the order book list (max available from depth20 stream)      // rows shown per side in the order book list
const TAPE_MAX_ROWS = 40;   // trades kept in the tape
const RECONNECT_DELAY = 2500;

let wsPublic = null;
let wsMarket = null;
let reconnectTimerPublic = null;
let reconnectTimerMarket = null;
let connState = { public: "connecting", market: "connecting" };
let book = { bids: [], asks: [] };
let lastPrice = null;
let prevPrice = null;

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

  let bidTotal = 0, askTotal = 0;
  const bidsHtml = bids.map(([price, size]) => {
    bidTotal += size;
    const pct = Math.min(100, (size / maxSize) * 100);
    return `<div class="book__row book__row--bid">
      <span class="r-bar" style="width:${pct}%"></span>
      <span class="r-price">${fmt(price)}</span>
      <span class="r-size">${fmt(size, 3)}</span>
      <span class="r-total">${fmt(bidTotal, 3)}</span>
    </div>`;
  }).join("");

  const asksHtml = asks.map(([price, size]) => {
    askTotal += size;
    const pct = Math.min(100, (size / maxSize) * 100);
    return `<div class="book__row book__row--ask">
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

  const maxCum = Math.max(bidCum.at(-1)[1], askCum.at(-1)[1]);
  const minPrice = bidCum.at(-1)[0];
  const maxPrice = askCum.at(-1)[0];
  const priceRange = Math.max(maxPrice - minPrice, 0.0001);

  const xFor = (price) => ((price - minPrice) / priceRange) * w;
  const yFor = (vol) => h - (vol / maxCum) * (h - 18) - 4;

  // grid baseline
  ctx.strokeStyle = "rgba(255,255,255,0.05)";
  ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.moveTo(0, h - 4);
  ctx.lineTo(w, h - 4);
  ctx.stroke();

  // bid area (gold-green gradient, drawn left -> mid)
  drawArea(bidCum, xFor, yFor, h, "rgba(34,168,120,0.55)", "rgba(34,168,120,0.02)", "#3fd39c");
  // ask area (red gradient, drawn mid -> right)
  drawArea(askCum, xFor, yFor, h, "rgba(214,75,79,0.55)", "rgba(214,75,79,0.02)", "#f26b6f");

  // mid price marker
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

function setLastPrice(price){
  prevPrice = lastPrice;
  lastPrice = price;
  const el = document.getElementById("statLast");
  el.textContent = fmt(price);
  if (prevPrice != null){
    el.classList.toggle("up", price >= prevPrice);
    el.classList.toggle("down", price < prevPrice);
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
  tickClock();
  setInterval(tickClock, 1000);
});
