# XAU/USDT Perpetual — Dashboard Real-Time

Dashboard trading gaya terminal untuk kontrak perpetual **XAU/USDT**, dengan carta harga TradingView dan visual "peta buku pesanan" (order book heatmap + depth map) seperti Bookmap. 100% statik — tiada backend, tiada build step — sesuai dihoskan terus di **GitHub Pages**.

## Tentang simbol yang digunakan

Binance **tidak** menyenaraikan pasangan `XAUUSDT` secara native untuk kontrak perpetual. Dashboard ini guna **PAXG/USDT Perpetual** (`PAXGUSDT`) sebagai proksi — PAXG (PAX Gold) ialah token yang disokong 1:1 oleh emas fizikal, jadi pergerakan harganya mengikut harga emas spot dengan sangat rapat.

| Komponen | Sumber |
|---|---|
| Carta candlestick | Widget TradingView — simbol `BINANCE:PAXGUSDT.P` |
| Order book / heatmap / depth map | Binance Futures WebSocket — `paxgusdt@depth20@100ms` |
| Harga tanda & kadar dana | `paxgusdt@markPrice@1s` |
| Trade tape | `paxgusdt@aggTrade` |
| Statistik 24 jam | `paxgusdt@ticker` |

## Ciri-ciri

- Carta langsung TradingView (zoom, indikator, pelbagai tempoh masa)
- Buku pesanan heatmap — bar latar belakang mengikut saiz relatif
- Peta kecairan (depth map) gaya Bookmap dalam canvas
- Trade tape langsung
- Statistik header: harga, % 24j, tinggi/rendah, volum, kadar dana, harga tanda
- Auto-reconnect websocket

## Struktur fail
