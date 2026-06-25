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
    if c_close > p_open and c_close > p_close:
        return 'RED'
    elif c_close < p_open and c_close < p_close:
        return 'BLACK'
    return 'NONE'

exchange = ccxt.binance({
    'enableRateLimit': True,
    'options': {'defaultType': 'future'}
})

def run():
    klines = exchange.fetch_ohlcv('AIN/USDT', timeframe='1d', limit=365)
    ohlcv_18d = compose_18d_bars(klines)
    df_18d = pd.DataFrame(ohlcv_18d, columns=['ts', 'open', 'high', 'low', 'close', 'vol', 'close_ts'])
    df_18d['date'] = pd.to_datetime(df_18d['ts'], unit='ms', utc=True).dt.tz_convert('Asia/Taipei').dt.strftime('%Y-%m-%d')
    print("--- 18D K 棒 ---")
    print(df_18d[['date', 'open', 'high', 'low', 'close']].tail(10).to_string())

    print("--- L1 吞噬檢查 ---")
    l1_state = 'NONE'
    for i in range(1, len(df_18d)):
        _prev = df_18d.iloc[i-1]
        _curr = df_18d.iloc[i]
        c_close = float(_curr['close'])
        sw = get_swallow(c_close, _prev['open'], _prev['close'])
        
        prev_state = l1_state
        if sw == 'RED':
            l1_state = 'RED'
        elif sw == 'BLACK':
            l1_state = 'BLACK'
            
        if prev_state != l1_state:
            print(f"[{_curr['date']}] 狀態改變為 {l1_state} (sw={sw})")

if __name__ == '__main__':
    run()
