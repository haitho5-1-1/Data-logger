#!/usr/bin/env python3
"""
fetch_microstructure.py

Fetch per‐minute microstructure data for 10 coins:
  - Net taker flow (signed buy/sell volume)
  - Order‐book imbalance (top N levels)

Saves each symbol’s data to CSV:
  live_{symbol}_micro.csv
"""

import os, csv, time, requests
from datetime import datetime

# ─── CONFIG ────────────────────────────────────────────────────────────────
SYMBOLS = [
    "BTCUSDT","ETHUSDT","ADAUSDT","SOLUSDT",
    "PEPEUSDT","DOGEUSDT","VISTAUSDT","OMUSDT",
    "MAGICUSDT","BANANAS31USDT"
]
INTERVAL_MS = 60_000        # 1 minute
LIMIT       = 1000          # aggTrades page size
DEPTH_LVL   = 5             # top N levels for imbalance

API_AGG   = "https://api.binance.com/api/v3/aggTrades"
API_DEPTH = "https://api.binance.com/api/v3/depth"
# ────────────────────────────────────────────────────────────────────────────

def get_csv_path(symbol):
    return f"live_{symbol}_micro.csv"

def append_row(symbol, row):
    fn  = get_csv_path(symbol)
    new = not os.path.exists(fn)
    with open(fn, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=row.keys())
        if new:
            w.writeheader()
        w.writerow(row)

def fetch_net_flow(symbol, start_ts, end_ts):
    all_trades = []
    cursor     = start_ts
    while cursor < end_ts:
        params = {"symbol": symbol, "startTime": cursor, "endTime": end_ts, "limit": LIMIT}
        r = requests.get(API_AGG, params=params, timeout=5)
        r.raise_for_status()
        data = r.json()
        if not data:
            break
        all_trades.extend(data)
        cursor = data[-1]["T"] + 1
        time.sleep(0.1)
    net = 0.0
    for t in all_trades:
        qty = float(t["q"])
        # m=True → trade was maker side, so taker sold → -qty
        net += -qty if t["m"] else qty
    return net

def fetch_imbalance(symbol):
    params = {"symbol": symbol, "limit": DEPTH_LVL}
    r = requests.get(API_DEPTH, params=params, timeout=5)
    r.raise_for_status()
    data = r.json()
    bids = sum(float(q) for _, q in data["bids"])
    asks = sum(float(q) for _, q in data["asks"])
    return (bids - asks) / (bids + asks) if (bids + asks) else 0.0

def main():
    now_ms = int((time.time() // 60) * 60 * 1000)
    for sym in SYMBOLS:
        try:
            start_ts = now_ms - INTERVAL_MS
            end_ts   = now_ms
            net_flow = fetch_net_flow(sym, start_ts, end_ts)
            imb      = fetch_imbalance(sym)
            row = {
                "ts": now_ms,
                "datetime": datetime.utcfromtimestamp(now_ms/1000).isoformat(),
                "net_taker_flow": net_flow,
                "order_book_imbalance": imb
            }
            append_row(sym, row)
            print(f"[{sym}] {row['datetime']} flow={net_flow:.4f} imb={imb:.4f}")
        except Exception as e:
            print(f"[{sym}] ERROR: {e}")

if __name__ == "__main__":
    main()
