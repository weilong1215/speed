import ccxt
import pandas as pd
from datetime import datetime, timezone

exchange = ccxt.bitget({'options': {'defaultType': 'swap'}})

def run():
    symbol = 'BEAT/USDT:USDT'
    import time
    now_ts = int(time.time() * 1000)
    since = now_ts - 365 * 24 * 3600 * 1000
    ohlcv = []
    
    for _ in range(10):
        if since >= now_ts: break
        try:
            batch = exchange.fetch_ohlcv(symbol, '1d', since=since, limit=100)
            if not batch: break
            ohlcv.extend(batch)
            since = batch[-1][0] + 1
        except Exception as e:
            print(f"Error fetching {symbol}: {e}")
            break
            
    ohlcv = sorted({b[0]: b for b in ohlcv}.values(), key=lambda x: x[0])
    
    print(f"BEAT 1D K棒總數: {len(ohlcv)}")
    if ohlcv:
        print(f"最早資料時間: {datetime.fromtimestamp(ohlcv[0][0]/1000)}")
    
if __name__ == '__main__':
    run()
