import ccxt
import pandas as pd
from datetime import datetime

exchange = ccxt.bitget({'options': {'defaultType': 'swap'}})

def get_swallow(c_close, p_open, p_close):
    p_body_high = max(p_open, p_close)
    p_body_low = min(p_open, p_close)
    if c_close > p_body_high: return 'RED'
    elif c_close < p_body_low: return 'BLACK'
    return 'NONE'

def run():
    symbol = 'BEAT/USDT:USDT'
    import time
    now_utc = int(time.time() * 1000)
    
    ohlcv_1d = []
    _end_time = now_utc
    for _pg in range(10):
        try:
            batch = exchange.fetch_ohlcv(symbol, '1d', limit=100, params={'endTime': _end_time})
            if not batch: break
            ohlcv_1d.extend(batch)
            _end_time = batch[0][0] - 1
        except Exception as e:
            print(f"Error fetching: {e}")
            break
            
    ohlcv_1d = sorted({b[0]: b for b in ohlcv_1d}.values(), key=lambda x: x[0])
    df_1d = pd.DataFrame(ohlcv_1d, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
    
    # Simulate L3 logic
    l3_state = 'NONE'
    temp_top = -1.0
    temp_bottom = float('inf')
    confirmed_top = -1.0
    confirmed_bottom = float('inf')
    
    print("Date       | O       | H       | L       | C       | Sw    | L3_S  | cTop    | cBot    | tTop    | tBot")
    print("-" * 105)
    for i in range(1, len(df_1d)):
        _prev = df_1d.iloc[i-1]
        _curr = df_1d.iloc[i]
        c_open = float(_curr['open'])
        c_high = float(_curr['high'])
        c_low = float(_curr['low'])
        c_close = float(_curr['close'])
        c_date = pd.to_datetime(_curr['ts'], unit='ms', utc=True).tz_convert('Asia/Taipei').strftime('%Y-%m-%d')
        
        # 只看 2月到3月
        if "2026-01" < c_date < "2026-04":
            sw = get_swallow(c_close, _prev['open'], _prev['close'])
            prev_state = l3_state
            
            if sw == 'RED': l3_state = 'RED'
            elif sw == 'BLACK': l3_state = 'BLACK'
                
            if prev_state != l3_state:
                if l3_state == 'RED':
                    if c_low < temp_bottom: temp_bottom = c_low
                    if confirmed_bottom == float('inf'): confirmed_bottom = temp_bottom
                    else:
                        if temp_bottom < confirmed_bottom:
                            confirmed_bottom = temp_bottom
                    temp_top = c_high
                elif l3_state == 'BLACK':
                    if c_high > temp_top: temp_top = c_high
                    if confirmed_top == -1.0: confirmed_top = temp_top
                    else:
                        if temp_top > confirmed_top:
                            confirmed_top = temp_top
                    temp_bottom = c_low
            else:
                if l3_state == 'RED' and c_high > temp_top: temp_top = c_high
                elif l3_state == 'BLACK' and c_low < temp_bottom: temp_bottom = c_low
                    
            print(f"{c_date} | {c_open:<7.4f} | {c_high:<7.4f} | {c_low:<7.4f} | {c_close:<7.4f} | {sw:<5} | {l3_state:<5} | {confirmed_top:<7.4f} | {confirmed_bottom:<7.4f} | {temp_top:<7.4f} | {temp_bottom:<7.4f}")

if __name__ == '__main__':
    run()
