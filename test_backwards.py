import ccxt
from datetime import datetime

exchange = ccxt.bitget({'options': {'defaultType': 'swap'}})

def run():
    symbol = 'BEAT/USDT:USDT'
    import time
    now_ts = int(time.time() * 1000)
    
    # 測試從現在往回抓
    ohlcv = []
    end_time = now_ts
    
    for _ in range(10):
        try:
            batch = exchange.fetch_ohlcv(symbol, '1d', limit=100, params={'endTime': end_time})
            if not batch:
                break
            # 確保不會抓到重複的
            # Bitget 的 endTime 可能包含當根
            ohlcv.extend(batch)
            end_time = batch[0][0] - 1  # 下一頁的最晚時間，設為這批最早的時間往前 1 毫秒
        except Exception as e:
            print(f"Error fetching {symbol}: {e}")
            break
            
    ohlcv = sorted({b[0]: b for b in ohlcv}.values(), key=lambda x: x[0])
    
    print(f"BEAT 1D K棒總數: {len(ohlcv)}")
    if ohlcv:
        print(f"最早資料時間: {datetime.fromtimestamp(ohlcv[0][0]/1000)}")
        print(f"最晚資料時間: {datetime.fromtimestamp(ohlcv[-1][0]/1000)}")

if __name__ == '__main__':
    run()
