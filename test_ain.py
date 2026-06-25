import ccxt
import pandas as pd

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
    klines = exchange.fetch_ohlcv('AIN/USDT', timeframe='1d', limit=100)
    df = pd.DataFrame(klines, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
    df['date'] = pd.to_datetime(df['ts'], unit='ms', utc=True).dt.tz_convert('Asia/Taipei').dt.strftime('%Y-%m-%d')
    
    print(df[['date', 'open', 'high', 'low', 'close']].tail(60).to_string())

    l3_state = 'NONE'
    temp_top = -1.0
    temp_bottom = float('inf')
    confirmed_top = -1.0
    confirmed_bottom = float('inf')
    pending_top = -1.0
    pending_bottom = float('inf')

    print("--- 模擬 L3 轉折 ---")
    for i in range(1, len(df)):
        _prev = df.iloc[i-1]
        _curr = df.iloc[i]
        c_high = float(_curr['high'])
        c_low = float(_curr['low'])
        c_close = float(_curr['close'])
        c_date = _curr['date']
        
        sw = get_swallow(c_close, _prev['open'], _prev['close'])
        prev_state = l3_state
        if sw == 'RED':
            l3_state = 'RED'
        elif sw == 'BLACK':
            l3_state = 'BLACK'
            
        if prev_state != l3_state:
            if l3_state == 'RED':
                if confirmed_bottom == float('inf'):
                    confirmed_bottom = temp_bottom
                else:
                    if temp_bottom < confirmed_bottom:
                        confirmed_bottom = temp_bottom
                        if pending_top != -1.0:
                            confirmed_top = pending_top
                        pending_top = -1.0
                        pending_bottom = float('inf')
                    else:
                        if temp_bottom < pending_bottom:
                            pending_bottom = temp_bottom
                temp_top = c_high
                print(f"[{c_date}] 黑轉紅 (sw={sw}) -> temp_bottom={temp_bottom:.5f}, confirmed_bottom={confirmed_bottom:.5f}")
                
            elif l3_state == 'BLACK':
                if confirmed_top == -1.0:
                    confirmed_top = temp_top
                else:
                    if temp_top > confirmed_top:
                        confirmed_top = temp_top
                        if pending_bottom != float('inf'):
                            confirmed_bottom = pending_bottom
                        pending_bottom = float('inf')
                        pending_top = -1.0
                    else:
                        if temp_top > pending_top:
                            pending_top = temp_top
                temp_bottom = c_low
                print(f"[{c_date}] 紅轉黑 (sw={sw}) -> temp_top={temp_top:.5f}, confirmed_top={confirmed_top:.5f}")
        else:
            if l3_state == 'RED':
                if c_high > temp_top:
                    temp_top = c_high
            elif l3_state == 'BLACK':
                if c_low < temp_bottom:
                    temp_bottom = c_low

if __name__ == '__main__':
    run()
