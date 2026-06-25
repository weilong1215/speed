import ccxt

exchange = ccxt.bitget({'options': {'defaultType': 'swap'}})
klines = exchange.fetch_ohlcv('AIN/USDT:USDT', '1d', limit=1000)
print(f"AIN 1d klines count: {len(klines)}")
if klines:
    import datetime
    start = datetime.datetime.fromtimestamp(klines[0][0]/1000)
    print(f"Earliest data: {start}")
