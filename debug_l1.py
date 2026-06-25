import ccxt
import pandas as pd
from datetime import datetime, timezone

def compose_18d_bars(ohlcv_1d):
    if not ohlcv_1d or len(ohlcv_1d) < 3:
        return []
    PERIOD_MS = 18 * 24 * 3600 * 1000
    groups = {}
    for bar in ohlcv_1d:
        ts = bar[0]
        dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
        year_start_dt = datetime(dt.year, 1, 1, tzinfo=timezone.utc)
        year_epoch_ms = int(year_start_dt.timestamp() * 1000)
        group_idx = (ts - year_epoch_ms) // PERIOD_MS
        group_key = year_epoch_ms + group_idx * PERIOD_MS
        if group_key not in groups:
            groups[group_key] = []
        groups[group_key].append(bar)
    result = []
    for gts in sorted(groups.keys()):
        bars = sorted(groups[gts], key=lambda x: x[0])
        result.append([gts, bars[0][1], max(b[2] for b in bars),
                       min(b[3] for b in bars), bars[-1][4],
                       sum(b[5] for b in bars), gts + PERIOD_MS])
    return result

def get_swallow(c_close, p_open, p_close):
    p_body_high = max(p_open, p_close)
    p_body_low  = min(p_open, p_close)
    if c_close > p_body_high:
        return 'RED'
    elif c_close < p_body_low:
        return 'BLACK'
    return 'NONE'

exchange = ccxt.bitget({'options': {'defaultType': 'swap'}})

def fetch_365d(symbol):
    import time
    ohlcv = []
    now_ts = int(time.time() * 1000)
    since  = now_ts - 365 * 24 * 3600 * 1000
    while since < now_ts:
        batch = exchange.fetch_ohlcv(symbol, '1d', since=since, limit=100)
        if not batch:
            break
        ohlcv.extend(batch)
        since = batch[-1][0] + 1
        if len(batch) < 90:
            break
    return sorted({b[0]: b for b in ohlcv}.values(), key=lambda x: x[0])

ohlcv_1d = fetch_365d('AIN/USDT:USDT')
print(f"1D 資料筆數: {len(ohlcv_1d)}")
print(f"最早: {datetime.fromtimestamp(ohlcv_1d[0][0]/1000)}")
print(f"最新: {datetime.fromtimestamp(ohlcv_1d[-1][0]/1000)}")

ohlcv_18d = compose_18d_bars(ohlcv_1d)
df = pd.DataFrame(ohlcv_18d, columns=['ts','open','high','low','close','vol','close_ts'])
df['date'] = pd.to_datetime(df['ts'], unit='ms', utc=True).dt.tz_convert('Asia/Taipei').dt.strftime('%Y-%m-%d')

print("\n--- 18D 所有 K 棒 ---")
print(df[['date','open','high','low','close']].to_string())

print("\n--- L1 狀態機完整追蹤 ---")
l1_valid = False
l1_date  = "未知"
for i in range(1, len(df)):
    prev = df.iloc[i-1]
    curr = df.iloc[i]
    sw = get_swallow(curr['close'], prev['open'], prev['close'])
    if sw == 'RED':
        if not l1_valid:
            l1_valid = True
            l1_date  = curr['date']
            print(f"  [{curr['date']}] 黑→紅 → L1 成立")
        else:
            print(f"  [{curr['date']}] 紅繼續 (L1 已成立, 不更新日期)")
    elif sw == 'BLACK':
        if l1_valid:
            print(f"  [{curr['date']}] 紅→黑 → L1 失效")
        l1_valid = False
        l1_date  = "未知"
    else:
        print(f"  [{curr['date']}] NONE")

print(f"\n最終狀態: l1_valid={l1_valid}, l1_date={l1_date}")
