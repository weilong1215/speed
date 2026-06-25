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
        result.append([
            gts,
            bars[0][1],
            max(b[2] for b in bars),
            min(b[3] for b in bars),
            bars[-1][4],
            sum(b[5] for b in bars),
            gts + PERIOD_MS
        ])
    return result

def get_swallow(c_close, p_open, p_close):
    p_body_high = max(p_open, p_close)
    p_body_low = min(p_open, p_close)
    if c_close > p_body_high:
        return 'RED'
    elif c_close < p_body_low:
        return 'BLACK'
    return 'NONE'

exchange = ccxt.bitget({
    'enableRateLimit': True,
    'options': {'defaultType': 'swap'}
})

def run():
    symbol = 'AIN/USDT:USDT'
    klines = exchange.fetch_ohlcv(symbol, timeframe='1d', limit=1000)
    ohlcv_18d = compose_18d_bars(klines)
    df_18d = pd.DataFrame(ohlcv_18d, columns=['ts', 'open', 'high', 'low', 'close', 'vol', 'close_ts'])
    df_18d['date'] = pd.to_datetime(df_18d['ts'], unit='ms', utc=True).dt.tz_convert('Asia/Taipei').dt.strftime('%Y-%m-%d')
    print("--- 18D K 棒 (Bitget) ---")
    print(df_18d[['date', 'open', 'high', 'low', 'close']].tail(10).to_string())

    print("--- L1 吞噬檢查 ---")
    l1_valid = False
    l1_date_str = "未知"
    for i in range(1, len(df_18d)):
        _prev = df_18d.iloc[i-1]
        _curr = df_18d.iloc[i]
        c_close = float(_curr['close'])
        sw = get_swallow(c_close, _prev['open'], _prev['close'])
        
        if sw == 'RED':
            if not l1_valid:
                l1_valid = True
                l1_date_str = _curr['date']
                print(f"[{_curr['date']}] L1 成立 (sw={sw})")
        elif sw == 'BLACK':
            if l1_valid:
                l1_valid = False
                print(f"[{_curr['date']}] L1 失效 (sw={sw})")

if __name__ == '__main__':
    run()
