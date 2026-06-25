import ccxt

exchange = ccxt.bitget({'options': {'defaultType': 'swap'}})
for tf in ['1h', '1d']:
    klines = exchange.fetch_ohlcv('AIN/USDT:USDT', tf, limit=1000)
    print(f"{tf} klines count: {len(klines)}")
