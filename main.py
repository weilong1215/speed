#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# 檔名: main.py

import numpy as np
import pandas as pd
import ccxt.async_support as ccxt
import asyncio
import time
import requests
import logging
import os
import threading
from flask import Flask
from datetime import datetime
from dotenv import load_dotenv
import sys
import io

# ============================================================================
# 系統初始化 (日誌與環境變數)
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("3DScanner")

# 載入環境變數
load_dotenv()

# 固定 I/O 編碼
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# ============================================================================
# 配置區
# ============================================================================

TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "")
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "")

# 非加密貨幣黑名單前綴 (Bitget 上混雜的股票/貴金屬/外匯標的)
NON_CRYPTO_PREFIXES = frozenset([
    'XAU', 'XAG', 'WTI', 'BRENT',
    'SPX', 'NDX', 'DJI', 'VIX', 'DXY',
    'EUR', 'GBP', 'JPY', 'AUD', 'CAD', 'CHF',
    'AAPL', 'TSLA', 'AMZN', 'GOOG', 'MSFT', 'META', 'NVDA', 'MSTR',
])

def is_crypto_symbol(symbol: str) -> bool:
    base = symbol.split('/')[0]
    return not any(base == p or base.startswith(p) for p in NON_CRYPTO_PREFIXES)

logger.info(f"✅ 系統配置檢查: TG_TOKEN={'已設定' if TG_BOT_TOKEN else '未設定'}, TG_CHAT_ID={'已設定' if TG_CHAT_ID else '未設定'}")

# ============================================================================
# 通知與資源獲取
# ============================================================================

def get_base_coin(symbol: str) -> str:
    if not symbol: return ""
    return symbol.replace('/', ':').split(':')[0].upper()

def send_telegram_message(message: str) -> bool:
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        logger.warning("Telegram 配置缺失，跳過通知。")
        return False
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TG_CHAT_ID, "text": message, "parse_mode": "HTML"}
    try:
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code != 200:
            logger.error(f"Telegram 發送失敗 ({response.status_code}): {response.text}")
            return False
        return True
    except Exception as e:
        logger.error(f"Telegram 連線異常: {e}")
        return False

def send_signal_telegram(symbol: str, close_price: float, ma20: float, d1_date: str, precision: int, is_3h_met: bool = False, entry_price: float = 0.0, stop_loss: float = 0.0):
    display_symbol = get_base_coin(symbol)
    
    if is_3h_met:
        extra_msg = (
            f"🔄 <b>3H 條件:</b> ✅ 達成\n"
            f"📍 <b>進場價格:</b> <code>{entry_price:.{precision}f}</code>\n"
            f"🛡️ <b>止損價格:</b> <code>{stop_loss:.{precision}f}</code>"
        )
    else:
        extra_msg = f"🔄 <b>3H 條件:</b> ❌ 未達成"

    msg = (
        f"🟢 <b>[做多] 3D MA20 吞噬轉換</b>\n\n"
        f"💎 <b>交易對:</b> {display_symbol}\n"
        f"📅 <b>3D K棒起始日期:</b> {d1_date}\n\n"
        f"━━━━━ 狀態資訊 ━━━━━\n"
        f"📌 <b>收盤觸發價:</b> <code>{close_price:.{precision}f}</code>\n"
        f"📈 <b>3D MA20:</b> <code>{ma20:.{precision}f}</code>\n\n"
        f"━━━━━ 3H 確認 ━━━━━\n"
        f"{extra_msg}"
    )
    send_telegram_message(msg)

def get_exchange():
    exchange_config = {
        'timeout': 30000,
        'enableRateLimit': True,
        'options': {'defaultType': 'swap'}
    }
    return ccxt.bitget(exchange_config)

# ============================================================================
# 掃描模組
# ============================================================================

async def scan_for_symbol(exchange, symbol, name, precision, current_idx=0, total_coins=0):
    try:
        if current_idx > 0 and (current_idx % 50 == 0 or current_idx == total_coins):
            logger.info(f"📊 掃描進度: {current_idx}/{total_coins}...")
            
        # 抓取過去 200 天的 1D K線 (保證合成後涵蓋至少 60 根 3D 棒以避開均線暖機期)
        now_ms = int(time.time() * 1000)
        fetch_since_1d = now_ms - (200 * 24 * 3600 * 1000)
        
        ohlcv_1d = []
        curr_1d = fetch_since_1d
        for _ in range(3):
            batch = await exchange.fetch_ohlcv(symbol, '1d', since=curr_1d, limit=100)
            if not batch: break
            ohlcv_1d.extend(batch)
            curr_1d = batch[-1][0] + (24 * 3600 * 1000)
            if curr_1d >= now_ms: break

        if not ohlcv_1d: return 0
        df = pd.DataFrame(ohlcv_1d, columns=['ts', 'open', 'high', 'low', 'close', 'vol']).drop_duplicates(subset=['ts']).reset_index(drop=True)
        df['dt'] = pd.to_datetime(df['ts'], unit='ms', utc=True)
        
        # 建立 3D 分組依據: 每年 1/1 為原點固定聚合
        df['year'] = df['dt'].dt.year
        df['doy'] = df['dt'].dt.dayofyear
        df['group_id'] = (df['doy'] - 1) // 3
        df['g_key'] = df['year'].astype(str) + "_" + df['group_id'].astype(str).str.zfill(3)
        
        # 合成 3D K線
        df_3d = df.groupby('g_key').agg({
            'ts': 'first',
            'dt': 'first',
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'vol': 'sum'
        }).sort_values('ts').reset_index()
        
        # 排除尚未收盤的 3D K棒 (比對當日 UTC 歸屬之 Group Key)
        now_utc = pd.Timestamp.now(tz='UTC')
        current_g_key = f"{now_utc.year}_{((now_utc.dayofyear - 1) // 3):03d}"
        df_3d = df_3d[df_3d['g_key'] < current_g_key].reset_index(drop=True)
        
        if len(df_3d) < 23: return 0
        
        df_3d['ma20'] = df_3d['close'].rolling(window=20).mean()
        
        # 嚴格取最新三根完全閉合的 3D 棒作條件比對
        row_a = df_3d.iloc[-3]
        row_b = df_3d.iloc[-2]
        row_c = df_3d.iloc[-1]
        
        if pd.isna(row_c['ma20']):
            return 0
            
        # Bar A (第1根): 陰線 (收盤 < 開盤)
        a_is_bearish = row_a['close'] < row_a['open']
        
        # Bar B (第2根): 陰線 (收盤 < 開盤) 且 陰吞噬 Bar A (收盤 < Bar A 收盤)
        b_is_bearish = row_b['close'] < row_b['open']
        b_engulf_a = row_b['close'] < row_a['close']
        
        # Bar C (第3根): 陽線 (收盤 > 開盤) 且 陽吞噬 Bar B (收盤 > Bar B 開盤)
        c_is_bullish = row_c['close'] > row_c['open']
        c_engulf_b = row_c['close'] > row_b['open']
        
        # 指標條件: 最新一根 (第3根) 收盤大於 MA20
        c_above_ma20 = row_c['close'] > row_c['ma20']
        
        if a_is_bearish and b_is_bearish and b_engulf_a and c_is_bullish and c_engulf_b and c_above_ma20:
            
            # === 3H 條件進階判斷 ===
            fetch_since_1h = now_ms - (10 * 24 * 3600 * 1000) # 抓最近 10 天，確保有足夠 1H 組成數根 3H K棒
            batch_1h = await exchange.fetch_ohlcv(symbol, '1h', since=fetch_since_1h, limit=240)
            
            is_3h_met = False
            entry_price = 0.0
            stop_loss = 0.0
            
            if batch_1h:
                df_1h = pd.DataFrame(batch_1h, columns=['ts', 'open', 'high', 'low', 'close', 'vol']).drop_duplicates(subset=['ts']).reset_index(drop=True)
                df_1h['dt'] = pd.to_datetime(df_1h['ts'], unit='ms', utc=True)
                
                # 合成 3H K線 (以 UTC 00, 03, 06 為切割基準)
                df_1h['3h_period'] = df_1h['dt'].dt.floor('3h')
                df_3h = df_1h.groupby('3h_period').agg({
                    'ts': 'first',
                    'dt': 'first',
                    'open': 'first',
                    'high': 'max',
                    'low': 'min',
                    'close': 'last',
                    'vol': 'sum'
                }).sort_values('ts').reset_index()
                
                # 排除尚未收盤的 3H K棒
                local_now_utc = pd.Timestamp.now(tz='UTC')
                now_utc_3h_fl = local_now_utc.floor('3h')
                df_3h = df_3h[df_3h['3h_period'] < now_utc_3h_fl].reset_index(drop=True)
                
                if len(df_3h) > 0:
                    latest_3h = df_3h.iloc[-1]
                    latest_3h_close = latest_3h['close']
                    latest_3h_low = latest_3h['low']
                    
                    target_3d_high = row_c['high']
                    
                    # 判斷最新收盤的 3H 收盤價有沒有大於 3D K棒 (Bar C) 的最高點
                    if latest_3h_close > target_3d_high:
                        is_3h_met = True
                        entry_price = latest_3h_close
                        stop_loss = (latest_3h_close + latest_3h_low) / 2
            # ========================

            d1_date_str = row_c['dt'].strftime('%Y-%m-%d')
            send_signal_telegram(symbol, row_c['close'], row_c['ma20'], d1_date_str, precision, is_3h_met, entry_price, stop_loss)
            return 1
            
        return 0
    except Exception as e:
        logger.warning(f"掃描異常 ({symbol}): {type(e).__name__}: {e}")
        return 0

async def run_scan():
    logger.info("⏰ 開始執行 3D MA20 與 3H 條件掃描...")
    ex = get_exchange()
    try:
        try:
            markets = await ex.load_markets()
            coins = [s for s, m in markets.items() if m.get('linear') and m.get('quote') == 'USDT' and is_crypto_symbol(s)]
            precisions = {s: max(0, int(round(-np.log10(markets[s].get('precision', {}).get('price', 1e-8))))) for s in coins}
        except:
            coins = ["BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT", "XRP/USDT:USDT", "DOGE/USDT:USDT", "ADA/USDT:USDT"]
            precisions = {s: 4 for s in coins}; precisions.update({"BTC/USDT:USDT": 2, "ETH/USDT:USDT": 2})

        count = 0
        total_coins = len(coins)
        for i in range(0, total_coins, 20):
            batch = coins[i:i+20]
            tasks = [scan_for_symbol(ex, s, get_base_coin(s), precisions[s], i + idx + 1, total_coins) for idx, s in enumerate(batch)]
            results = await asyncio.gather(*tasks)
            count += sum(results)
            await asyncio.sleep(0.5)
            
        logger.info(f"✅ 掃描完成。本期發現符合 3D 條件訊號: {count} 個")
        if count == 0:
            send_telegram_message(f"✅ <b>條件掃描完成</b>\n本次共掃描 {total_coins} 個幣種，目前未發現符合 3D 條件之訊號。")
    finally: 
        await ex.close()

async def scheduler():
    last_exec_hour = -1
    last_day = -1
    try:
        await run_scan()
    except Exception as e:
        logger.error(f"初始掃描異常: {e}")
    
    while True:
        try:
            now = datetime.utcnow()
            # 每日 UTC 整點 (且須能被 3 整除的小時) 的第 1 分鐘觸發 (00:01, 03:01, 06:01...)
            if now.minute == 1 and now.hour % 3 == 0 and (now.day != last_day or now.hour != last_exec_hour):
                try:
                    await run_scan()
                except Exception as e:
                    logger.error(f"定時掃描異常: {e}")
                last_day = now.day
                last_exec_hour = now.hour
        except Exception as e:
            logger.critical(f"💥 Scheduler 頂層異常 (已攔截): {e}")
        await asyncio.sleep(60)

# ============================================================================
# Minimal Flask for Zeabur Health Check
# ============================================================================

app = Flask(__name__)
@app.route('/')
@app.route('/health')
def health(): return {"status": "ok", "service": "3D-Scanner-Only"}, 200

def run_flask():
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)

# = [啟動模組] ===============================================================

def run_background_system():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(scheduler())

logger.info("🚀 啟動 3D 結構專用掃描系統 (純推播模式)...")
bg_thread = threading.Thread(target=run_background_system, daemon=True)
bg_thread.start()

if __name__ == '__main__':
    run_flask()

