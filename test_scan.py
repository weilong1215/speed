import pandas as pd
import ccxt
import asyncio
import time

def compose_multi_bars(df_1d, period_days):
    if len(df_1d) < period_days: return []
    composed = []
    # 假設從最後一根往前切分，或者從頭往後切？
    # 他的 main.py 中的 compose_18d_bars 是由舊到新
    return composed

async def test():
    exchange = ccxt.binance()
    ohlcv = exchange.fetch_ohlcv('BTC/USDT', '1d', limit=1000)
    df = pd.DataFrame(ohlcv, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
    df['close_ts'] = df['ts'] + 24 * 3600 * 1000

    def get_swallow(c_close, p_open, p_close):
        p_body_high = max(p_open, p_close)
        p_body_low = min(p_open, p_close)
        if c_close > p_body_high: return 'RED'
        elif c_close < p_body_low: return 'BLACK'
        return 'NONE'

    l3_state = 'NONE'
    temp_top = -1.0
    temp_bottom = float('inf')
    confirmed_top = -1.0
    confirmed_bottom = float('inf')

    for i in range(1, len(df)):
        _prev = df.iloc[i-1]
        _curr = df.iloc[i]
        c_high = float(_curr['high'])
        c_low = float(_curr['low'])
        c_close = float(_curr['close'])
        c_ts = int(_curr['ts'])
        date_str = pd.to_datetime(c_ts, unit='ms').strftime('%Y-%m-%d')
        
        sw = get_swallow(c_close, _prev['open'], _prev['close'])

        prev_state = l3_state
        if sw == 'RED': l3_state = 'RED'
        elif sw == 'BLACK': l3_state = 'BLACK'

        if prev_state != l3_state:
            if l3_state == 'RED':
                # 黑轉紅：黑吞結束，確立底
                if prev_state == 'BLACK':
                    confirmed_bottom = temp_bottom
                # 開啟新紅吞
                temp_top = c_high
            elif l3_state == 'BLACK':
                # 紅轉黑：紅吞結束，確立頂
                if prev_state == 'RED':
                    confirmed_top = temp_top
                # 開啟新黑吞
                temp_bottom = c_low
        else:
            if l3_state == 'RED':
                if c_high > temp_top: temp_top = c_high
            elif l3_state == 'BLACK':
                if c_low < temp_bottom: temp_bottom = c_low

        if confirmed_bottom != float('inf') and c_close < confirmed_bottom:
            confirmed_top = -1.0

        if confirmed_top > 0 and c_close > confirmed_top:
            print(f"[{date_str}] SIGNAL! c_close={c_close} > confirmed_top={confirmed_top}")

if __name__ == "__main__":
    asyncio.run(test())
