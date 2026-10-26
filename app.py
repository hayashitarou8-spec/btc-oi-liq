"""BTC OI / Liquidation monitor — public CEX data only (Binance / Bybit / OKX)."""
from __future__ import annotations

import hmac
import json
import os
import threading
import time
import urllib.error
import urllib.request
from collections import deque
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse

try:
    import websocket  # websocket-client
except Exception:  # pragma: no cover
    websocket = None

STATIC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)

_tok = (os.environ.get("ACCESS_TOKEN") or "").strip()
_tok_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".access_token")
if not _tok and os.path.isfile(_tok_file):
    _tok = open(_tok_file).read().strip()
if len(_tok) < 16:
    raise RuntimeError("ACCESS_TOKEN env (or .access_token) required, >=16 chars")
TOKEN = _tok

# ---- auth gate ----
@app.get("/healthz")
def healthz():
    return {"ok": True}


@app.middleware("http")
async def gate(request: Request, call_next):
    if request.url.path in ("/healthz", "/icon.png", "/manifest.webmanifest"):
        return await call_next(request)
    q = request.query_params.get("t")
    c = request.cookies.get("btc_t")
    ok = (q and hmac.compare_digest(q, TOKEN)) or (c and hmac.compare_digest(c, TOKEN))
    if not ok:
        return HTMLResponse("<h1>401</h1><p>アクセストークンが必要です。</p>", status_code=401)
    if q:
        resp = (
            RedirectResponse(request.url.path or "/", status_code=303)
            if request.url.path == "/"
            else await call_next(request)
        )
        resp.set_cookie(
            "btc_t",
            TOKEN,
            httponly=True,
            secure=request.headers.get("x-forwarded-proto") == "https",
            samesite="lax",
            max_age=30 * 86400,
        )
        return resp
    return await call_next(request)


# ---- helpers ----
UA = {"User-Agent": "btc-oi-liq/1.0"}
_lock = threading.RLock()
_cache: dict[str, tuple[float, Any]] = {}
_liqs: deque[dict] = deque(maxlen=5000)
_liq_keys: set[tuple] = set()
_ws_status: dict[str, Any] = {
    "binance": {"ok": False, "last": None, "err": None},
    "bybit": {"ok": False, "last": None, "err": None},
}
_started = time.time()
_okx_ct_val = 0.01  # BTC per contract for BTC-USDT-SWAP


def http_json(url: str, timeout: float = 10) -> Any:
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.load(r)


def cached(key: str, ttl: float, fn):
    with _lock:
        hit = _cache.get(key)
        if hit and time.time() - hit[0] < ttl:
            return hit[1]
    try:
        val = fn()
    except Exception as e:
        if hit:
            return hit[1]
        return {"error": str(e)}
    with _lock:
        _cache[key] = (time.time(), val)
    return val


def _num(x, default=None):
    try:
        return float(x)
    except Exception:
        return default


def _push_liq(item: dict):
    """Dedup by (ex, ts, side, qty, price)."""
    key = (
        item.get("ex"),
        item.get("ts"),
        item.get("side"),
        round(float(item.get("qty") or 0), 6),
        round(float(item.get("price") or 0), 2),
    )
    with _lock:
        if key in _liq_keys:
            return
        _liq_keys.add(key)
        _liqs.appendleft(item)
        # keep key set bounded with deque
        while len(_liq_keys) > _liqs.maxlen:
            # drop arbitrary old keys by rebuilding from buffer
            _liq_keys.clear()
            for existing in _liqs:
                _liq_keys.add(
                    (
                        existing.get("ex"),
                        existing.get("ts"),
                        existing.get("side"),
                        round(float(existing.get("qty") or 0), 6),
                        round(float(existing.get("price") or 0), 2),
                    )
                )
            break


# ---- OI fetchers ----
def fetch_binance_oi() -> dict:
    cur = http_json("https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT")
    ticker = http_json("https://fapi.binance.com/fapi/v1/ticker/24hr?symbol=BTCUSDT")
    mark = http_json("https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BTCUSDT")
    h5 = http_json(
        "https://fapi.binance.com/futures/data/openInterestHist?symbol=BTCUSDT&period=5m&limit=13"
    )
    h1 = http_json(
        "https://fapi.binance.com/futures/data/openInterestHist?symbol=BTCUSDT&period=1h&limit=25"
    )
    oi = _num(cur.get("openInterest"))
    px = _num(mark.get("markPrice")) or _num(ticker.get("lastPrice"))
    oi_usd = oi * px if oi is not None and px else None

    def delta(hist, n_back):
        if not hist or len(hist) <= n_back:
            return None, None, None
        a = hist[-(n_back + 1)]
        b = hist[-1]
        oi_a = _num(a.get("sumOpenInterest"))
        oi_b = _num(b.get("sumOpenInterest"))
        usd_a = _num(a.get("sumOpenInterestValue"))
        usd_b = _num(b.get("sumOpenInterestValue"))
        if oi_a is None or oi_b is None:
            return None, None, None
        d = oi_b - oi_a
        pct = (d / oi_a * 100) if oi_a else None
        d_usd = (usd_b - usd_a) if (usd_a is not None and usd_b is not None) else None
        return d, pct, d_usd

    d1h, p1h, u1h = delta(h5, 12)  # 12 * 5m ≈ 1h
    d24, p24, u24 = delta(h1, 24)
    return {
        "ex": "binance",
        "symbol": "BTCUSDT",
        "oi_btc": oi,
        "oi_usd": oi_usd,
        "price": px,
        "chg24h_pct": _num(ticker.get("priceChangePercent")),
        "funding": _num(mark.get("lastFundingRate")),
        "d1h_btc": d1h,
        "d1h_pct": p1h,
        "d1h_usd": u1h,
        "d24h_btc": d24,
        "d24h_pct": p24,
        "d24h_usd": u24,
        "ts": int(cur.get("time") or time.time() * 1000),
    }


def fetch_bybit_oi() -> dict:
    tk = http_json("https://api.bybit.com/v5/market/tickers?category=linear&symbol=BTCUSDT")
    row = (tk.get("result") or {}).get("list") or []
    if not row:
        raise RuntimeError("bybit empty ticker")
    r = row[0]
    hist = http_json(
        "https://api.bybit.com/v5/market/open-interest?category=linear&symbol=BTCUSDT&intervalTime=5min&limit=13"
    )
    hlist = (hist.get("result") or {}).get("list") or []
    # Bybit returns newest first
    oi = _num(r.get("openInterest"))
    oi_usd = _num(r.get("openInterestValue"))
    px = _num(r.get("lastPrice"))

    def delta_from_hist(n_back):
        if len(hlist) <= n_back:
            return None, None
        now_oi = _num(hlist[0].get("openInterest"))
        then_oi = _num(hlist[n_back].get("openInterest"))
        if now_oi is None or then_oi is None or then_oi == 0:
            return None, None
        d = now_oi - then_oi
        return d, d / then_oi * 100

    d1h, p1h = delta_from_hist(12)
    # 24h: use 1h interval
    h1 = http_json(
        "https://api.bybit.com/v5/market/open-interest?category=linear&symbol=BTCUSDT&intervalTime=1h&limit=25"
    )
    h1list = (h1.get("result") or {}).get("list") or []
    d24 = p24 = None
    if len(h1list) > 24:
        now_oi = _num(h1list[0].get("openInterest"))
        then_oi = _num(h1list[24].get("openInterest"))
        if now_oi is not None and then_oi:
            d24 = now_oi - then_oi
            p24 = d24 / then_oi * 100
    return {
        "ex": "bybit",
        "symbol": "BTCUSDT",
        "oi_btc": oi,
        "oi_usd": oi_usd,
        "price": px,
        "chg24h_pct": _num(r.get("price24hPcnt"), 0) * 100 if r.get("price24hPcnt") is not None else None,
        "funding": _num(r.get("fundingRate")),
        "d1h_btc": d1h,
        "d1h_pct": p1h,
        "d1h_usd": (d1h * px) if (d1h is not None and px) else None,
        "d24h_btc": d24,
        "d24h_pct": p24,
        "d24h_usd": (d24 * px) if (d24 is not None and px) else None,
        "ts": int(time.time() * 1000),
    }


def fetch_okx_oi() -> dict:
    global _okx_ct_val
    oi = http_json("https://www.okx.com/api/v5/public/open-interest?instType=SWAP&instId=BTC-USDT-SWAP")
    row = (oi.get("data") or [None])[0]
    if not row:
        raise RuntimeError("okx empty oi")
    ticker = http_json("https://www.okx.com/api/v5/market/ticker?instId=BTC-USDT-SWAP")
    trow = (ticker.get("data") or [None])[0] or {}
    try:
        inst = http_json(
            "https://www.okx.com/api/v5/public/instruments?instType=SWAP&instId=BTC-USDT-SWAP"
        )
        _okx_ct_val = _num((inst.get("data") or [{}])[0].get("ctVal"), 0.01) or 0.01
    except Exception:
        pass
    # OKX oiCcy is in BTC; oiUsd provided
    oi_btc = _num(row.get("oiCcy"))
    oi_usd = _num(row.get("oiUsd"))
    px = _num(trow.get("last"))
    d1h = p1h = d24 = p24 = None
    def _oi_ccy(row):
        # [ts, oi, oiCcy, oiUsd]
        if not row:
            return None
        return _num(row[2] if len(row) > 2 else row[1])
    try:
        hist5 = http_json(
            "https://www.okx.com/api/v5/rubik/stat/contracts/open-interest-history"
            "?instId=BTC-USDT-SWAP&period=5m"
        )
        data = hist5.get("data") or []
        if len(data) > 12:
            now_b, then_b = _oi_ccy(data[0]), _oi_ccy(data[12])
            if now_b is not None and then_b:
                d1h = now_b - then_b
                p1h = d1h / then_b * 100
    except Exception:
        pass
    try:
        hist1 = http_json(
            "https://www.okx.com/api/v5/rubik/stat/contracts/open-interest-history"
            "?instId=BTC-USDT-SWAP&period=1H"
        )
        data = hist1.get("data") or []
        if len(data) > 24:
            now_b, then_b = _oi_ccy(data[0]), _oi_ccy(data[24])
            if now_b is not None and then_b:
                d24 = now_b - then_b
                p24 = d24 / then_b * 100
    except Exception:
        pass
    chg = None
    o = _num(trow.get("open24h"))
    if o and px:
        chg = (px / o - 1) * 100
    return {
        "ex": "okx",
        "symbol": "BTC-USDT-SWAP",
        "oi_btc": oi_btc,
        "oi_usd": oi_usd,
        "price": px,
        "chg24h_pct": chg,
        "funding": None,
        "d1h_btc": d1h,
        "d1h_pct": p1h,
        "d1h_usd": (d1h * px) if (d1h is not None and px) else None,
        "d24h_btc": d24,
        "d24h_pct": p24,
        "d24h_usd": (d24 * px) if (d24 is not None and px) else None,
        "ts": int(_num(row.get("ts"), time.time() * 1000)),
    }


def snapshot_oi() -> dict:
    out = {"ts": int(time.time() * 1000), "exchanges": {}}
    from concurrent.futures import ThreadPoolExecutor, as_completed
    jobs = (
        ("binance", fetch_binance_oi, 4),
        ("bybit", fetch_bybit_oi, 4),
        ("okx", fetch_okx_oi, 4),
    )
    def _one(name, fn, ttl):
        return name, cached(f"oi:{name}", ttl, fn)
    with ThreadPoolExecutor(max_workers=3) as pool:
        futs = [pool.submit(_one, n, f, t) for n, f, t in jobs]
        for fut in as_completed(futs):
            name, val = fut.result()
            out["exchanges"][name] = val
    # aggregate available
    total_btc = 0.0
    total_usd = 0.0
    n = 0
    for v in out["exchanges"].values():
        if isinstance(v, dict) and "oi_btc" in v and v.get("oi_btc") is not None:
            total_btc += v["oi_btc"]
            if v.get("oi_usd") is not None:
                total_usd += v["oi_usd"]
            n += 1
    out["agg"] = {
        "n": n,
        "oi_btc": total_btc if n else None,
        "oi_usd": total_usd if n else None,
        "note": "Binance+Bybit+OKX USDT永続の合算（重複なし・各所別建玉）",
    }
    return out


# ---- liquidations ----
def ingest_okx_rest():
    global _okx_ct_val
    data = http_json(
        "https://www.okx.com/api/v5/public/liquidation-orders"
        "?instType=SWAP&uly=BTC-USDT&state=filled&limit=100"
    )
    rows = data.get("data") or []
    if not rows:
        return 0
    details = rows[0].get("details") or []
    n = 0
    for d in details:
        side = d.get("posSide")  # long = long liquidated
        if side not in ("long", "short"):
            # fall back from trade side
            side = "long" if d.get("side") == "sell" else "short" if d.get("side") == "buy" else None
        qty_contracts = _num(d.get("sz"), 0) or 0
        qty_btc = qty_contracts * _okx_ct_val
        px = _num(d.get("bkPx"))
        ts = int(_num(d.get("ts") or d.get("time"), 0) or 0)
        if not ts or not side:
            continue
        _push_liq(
            {
                "ex": "okx",
                "side": side,
                "qty": qty_btc,
                "notional": (qty_btc * px) if px else None,
                "price": px,
                "ts": ts,
                "src": "rest",
            }
        )
        n += 1
    return n


def _agg_liqs(window_ms: int | None = None) -> dict:
    now = int(time.time() * 1000)
    long_btc = short_btc = 0.0
    long_usd = short_usd = 0.0
    count = 0
    oldest = None
    newest = None
    by_ex: dict[str, dict] = {}
    with _lock:
        items = list(_liqs)
    for it in items:
        ts = int(it.get("ts") or 0)
        if window_ms is not None and ts and now - ts > window_ms:
            continue
        qty = _num(it.get("qty"), 0) or 0
        notion = _num(it.get("notional"))
        if notion is None and it.get("price") is not None:
            notion = qty * float(it["price"])
        side = it.get("side")
        ex = it.get("ex") or "?"
        slot = by_ex.setdefault(ex, {"long_btc": 0.0, "short_btc": 0.0, "long_usd": 0.0, "short_usd": 0.0, "n": 0})
        if side == "long":
            long_btc += qty
            long_usd += notion or 0
            slot["long_btc"] += qty
            slot["long_usd"] += notion or 0
        elif side == "short":
            short_btc += qty
            short_usd += notion or 0
            slot["short_btc"] += qty
            slot["short_usd"] += notion or 0
        else:
            continue
        slot["n"] += 1
        count += 1
        if oldest is None or ts < oldest:
            oldest = ts
        if newest is None or ts > newest:
            newest = ts
    return {
        "long_btc": long_btc,
        "short_btc": short_btc,
        "long_usd": long_usd,
        "short_usd": short_usd,
        "total_btc": long_btc + short_btc,
        "total_usd": long_usd + short_usd,
        "n": count,
        "oldest_ts": oldest,
        "newest_ts": newest,
        "by_ex": by_ex,
    }


def snapshot_liq() -> dict:
    # refresh OKX REST into buffer (cached lightly)
    try:
        cached("okx_liq_ingest", 8, ingest_okx_rest)
    except Exception as e:
        okx_err = str(e)
    else:
        okx_err = None
    with _lock:
        feed = list(_liqs)[:80]
        ws = dict(_ws_status)
    return {
        "ts": int(time.time() * 1000),
        "uptime_s": int(time.time() - _started),
        "ws": ws,
        "okx_rest_error": okx_err,
        "h1": _agg_liqs(3600_000),
        "h24": _agg_liqs(86400_000),
        "all_buf": _agg_liqs(None),
        "feed": feed,
        "caveat": (
            "清算の1h/24hはサーバーが受け取った分の合算。"
            "OKXはRESTで直近を補完。Binance/Bybitは公開WSのライブ蓄積。"
            "コールドスタート直後は24hが不完全になりやすい。"
        ),
    }


# ---- websockets ----
def _ws_binance_loop():
    if websocket is None:
        _ws_status["binance"]["err"] = "websocket-client not installed"
        return
    url = "wss://fstream.binance.com/ws/btcusdt@forceOrder"

    def on_message(_ws, message):
        try:
            d = json.loads(message)
            o = d.get("o") or {}
            if (o.get("s") or "").upper() != "BTCUSDT":
                return
            # S = order side: SELL means long liquidated
            side = "long" if o.get("S") == "SELL" else "short" if o.get("S") == "BUY" else None
            qty = _num(o.get("q") or o.get("z"), 0) or 0
            px = _num(o.get("ap") or o.get("p"))
            ts = int(_num(o.get("T") or d.get("E"), time.time() * 1000))
            if not side:
                return
            _push_liq(
                {
                    "ex": "binance",
                    "side": side,
                    "qty": qty,
                    "notional": (qty * px) if px else None,
                    "price": px,
                    "ts": ts,
                    "src": "ws",
                }
            )
            with _lock:
                _ws_status["binance"] = {"ok": True, "last": ts, "err": None}
        except Exception as e:
            with _lock:
                _ws_status["binance"]["err"] = str(e)

    def on_error(_ws, err):
        with _lock:
            _ws_status["binance"]["ok"] = False
            _ws_status["binance"]["err"] = str(err)

    def on_open(_ws):
        with _lock:
            _ws_status["binance"]["ok"] = True
            _ws_status["binance"]["err"] = None

    while True:
        try:
            ws = websocket.WebSocketApp(
                url, on_message=on_message, on_error=on_error, on_open=on_open
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            with _lock:
                _ws_status["binance"]["ok"] = False
                _ws_status["binance"]["err"] = str(e)
        time.sleep(3)


def _ws_bybit_loop():
    if websocket is None:
        _ws_status["bybit"]["err"] = "websocket-client not installed"
        return
    url = "wss://stream.bybit.com/v5/public/linear"

    def on_open(ws):
        ws.send(json.dumps({"op": "subscribe", "args": ["allLiquidation.BTCUSDT"]}))
        with _lock:
            _ws_status["bybit"]["ok"] = True
            _ws_status["bybit"]["err"] = None

    def on_message(_ws, message):
        try:
            d = json.loads(message)
            if d.get("topic") != "allLiquidation.BTCUSDT":
                return
            rows = d.get("data") or []
            if isinstance(rows, dict):
                rows = [rows]
            for o in rows:
                # Bybit: side Buy = buy to close short => short liquidated
                side = "short" if o.get("side") == "Buy" else "long" if o.get("side") == "Sell" else None
                qty = _num(o.get("size") or o.get("qty"), 0) or 0
                px = _num(o.get("price"))
                ts = int(_num(o.get("updatedTime") or o.get("T"), time.time() * 1000))
                if not side:
                    continue
                _push_liq(
                    {
                        "ex": "bybit",
                        "side": side,
                        "qty": qty,
                        "notional": (qty * px) if px else None,
                        "price": px,
                        "ts": ts,
                        "src": "ws",
                    }
                )
                with _lock:
                    _ws_status["bybit"] = {"ok": True, "last": ts, "err": None}
        except Exception as e:
            with _lock:
                _ws_status["bybit"]["err"] = str(e)

    def on_error(_ws, err):
        with _lock:
            _ws_status["bybit"]["ok"] = False
            _ws_status["bybit"]["err"] = str(err)

    while True:
        try:
            ws = websocket.WebSocketApp(
                url, on_message=on_message, on_error=on_error, on_open=on_open
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            with _lock:
                _ws_status["bybit"]["ok"] = False
                _ws_status["bybit"]["err"] = str(e)
        time.sleep(3)


def _okx_poll_loop():
    while True:
        try:
            ingest_okx_rest()
        except Exception:
            pass
        time.sleep(10)


@app.on_event("startup")
def _startup():
    try:
        ingest_okx_rest()
    except Exception:
        pass
    threading.Thread(target=_ws_binance_loop, name="ws-bn", daemon=True).start()
    threading.Thread(target=_ws_bybit_loop, name="ws-bb", daemon=True).start()
    threading.Thread(target=_okx_poll_loop, name="okx-rest", daemon=True).start()


# ---- routes ----
@app.get("/")
def index():
    return FileResponse(os.path.join(STATIC, "index.html"))


@app.get("/api/snapshot")
def api_snapshot():
    oi = snapshot_oi()
    liq = snapshot_liq()
    # mark unavailable exchanges
    for name, v in list(oi["exchanges"].items()):
        if isinstance(v, dict) and v.get("error"):
            oi["exchanges"][name] = {"ex": name, "error": v["error"], "unavailable": True}
    return JSONResponse(
        {
            "ts": int(time.time() * 1000),
            "oi": oi,
            "liq": liq,
        }
    )


@app.get("/manifest.webmanifest")
def manifest():
    return JSONResponse(
        {
            "name": "BTC OI / 清算",
            "short_name": "BTC OI",
            "start_url": "/",
            "display": "standalone",
            "background_color": "#020409",
            "theme_color": "#020409",
            "icons": [{"src": "/icon.png", "sizes": "192x192", "type": "image/png"}],
        },
        media_type="application/manifest+json",
    )


@app.get("/icon.png")
def icon():
    path = os.path.join(STATIC, "icon.png")
    if os.path.isfile(path):
        return FileResponse(path)
    return HTMLResponse("", status_code=404)
