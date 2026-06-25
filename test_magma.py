import asyncio
import ccxt.async_support as ccxt
from main import scan_for_symbol

async def main():
    exchange = ccxt.bitget()
    try:
        res = await scan_for_symbol(exchange, 'MAGMA/USDT:USDT', 'MAGMA', 4)
        if res and 'historical_c2s' in res:
            for sig in res['historical_c2s']:
                print(f"Entry: {sig['entry_price']}, SL: {sig['stop_loss']}, Trailing: {sig.get('trailing_sl')}, RR: {sig.get('real_rr')}")
        else:
            print("No signals returned.")
    finally:
        await exchange.close()

asyncio.run(main())
