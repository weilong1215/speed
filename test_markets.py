import ccxt

exchange = ccxt.bitget({'options': {'defaultType': 'swap'}})

def run():
    try:
        markets = exchange.load_markets()
        print("Market loaded.")
        beat_markets = [k for k in markets.keys() if 'BEAT' in k.upper()]
        print("Markets containing 'BEAT':", beat_markets)
    except Exception as e:
        print(f"Error loading markets: {e}")

if __name__ == '__main__':
    run()
