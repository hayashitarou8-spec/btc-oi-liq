# Deploy (Render free / Singapore)

1. Create empty public GitHub repo `hayashitarou8-spec/btc-oi-liq`.
2. From this directory:
   ```
   git remote add origin https://github.com/hayashitarou8-spec/btc-oi-liq.git
   git push -u origin main
   ```
3. Create Render web service (Docker, free, singapore) from that repo.
4. Set env `ACCESS_TOKEN` to a >=16 char secret (see sibling `.access_token` on the build machine; do not commit it).
5. Health check: `/healthz`
6. Open `https://<service>.onrender.com/?t=<ACCESS_TOKEN>`

Public sources used (no API keys):
- Binance Futures REST: openInterest, openInterestHist, ticker, premiumIndex; WS `btcusdt@forceOrder`
- Bybit v5 REST: tickers, open-interest; WS `allLiquidation.BTCUSDT`
- OKX REST: open-interest, rubik OI history, liquidation-orders; instrument ctVal

Caveats:
- Liquidation 1h/24h totals are from the server's received buffer (OKX REST seeds recent ~1–2h; Binance/Bybit accumulate via WS). Incomplete after cold start / free-tier sleep.
- Free Render sleeps when idle; first hit can take ~30–60s.
- Coinglass free API requires a key (skipped). Binance REST forceOrders requires API key (skipped).
