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
import json

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

def is_crypto_symbol(symbol: str, blacklist: list) -> bool:
    if not blacklist:
        return True
    base = symbol.split('/')[0]
    return not any(base == p or base.startswith(p) for p in blacklist)

logger.info(f"✅ 系統配置檢查: TG_TOKEN={'已設定' if TG_BOT_TOKEN else '未設定'}, TG_CHAT_ID={'已設定' if TG_CHAT_ID else '未設定'}")

# ============================================================================
# 狀態持久化 (Watchlist)
# ============================================================================

WATCHLIST_FILE = "/app/data/active_signals.json"

def load_watchlist():
    if os.path.exists(WATCHLIST_FILE):
        try:
            with open(WATCHLIST_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"讀取 watchlist 失敗: {e}")
    return {}

def save_watchlist(data):
    try:
        with open(WATCHLIST_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
    except Exception as e:
        logger.error(f"儲存 watchlist 失敗: {e}")

# ============================================================================
# 系統設定持久化
# ============================================================================

CONFIG_FILE = "/app/data/system_config.json"

def load_config():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                config = json.load(f)
            if "blacklist" not in config:
                config["blacklist"] = []
            return config
        except Exception as e:
            logger.error(f"讀取設定檔失敗: {e}")
    return {"default_loss_amount": 6, "blacklist": []}

def save_config(data):
    try:
        os.makedirs(os.path.dirname(CONFIG_FILE), exist_ok=True)
        with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
    except Exception as e:
        logger.error(f"儲存設定檔失敗: {e}")

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

async def scan_for_symbol(exchange, symbol, name, precision, current_idx=0, total_coins=0, cached_info=None):
    try:
        if current_idx > 0 and (current_idx % 50 == 0 or current_idx == total_coins):
            logger.info(f"📊 掃描進度: {current_idx}/{total_coins}...")
            
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

        if not ohlcv_1d: return None
        df = pd.DataFrame(ohlcv_1d, columns=['ts', 'open', 'high', 'low', 'close', 'vol']).drop_duplicates(subset=['ts']).reset_index(drop=True)
        df['dt'] = pd.to_datetime(df['ts'], unit='ms', utc=True)
        
        df['year'] = df['dt'].dt.year
        df['doy'] = df['dt'].dt.dayofyear
        df['group_id'] = (df['doy'] - 1) // 3
        df['g_key'] = df['year'].astype(str) + "_" + df['group_id'].astype(str).str.zfill(3)
        
        df_3d = df.groupby('g_key').agg({
            'ts': 'first',
            'dt': 'first',
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'vol': 'sum'
        }).sort_values('ts').reset_index()
        
        now_utc = pd.Timestamp.now(tz='UTC')
        current_g_key = f"{now_utc.year}_{((now_utc.dayofyear - 1) // 3):03d}"
        df_3d = df_3d[df_3d['g_key'] < current_g_key].reset_index(drop=True)
        
        if len(df_3d) < 3: return None
        
        df_3d['ma20'] = df_3d['close'].rolling(window=20).mean()
        row_c = df_3d.iloc[-1]
        
        target_info = None
        action = None
        
        # 1. 確認是否出現新的 3D 條件 (無條件覆蓋)
        if len(df_3d) >= 23 and not pd.isna(row_c['ma20']):
            row_a = df_3d.iloc[-3]
            row_b = df_3d.iloc[-2]
            
            a_is_bearish = row_a['close'] < row_a['open']
            b_is_bearish = row_b['close'] < row_b['open']
            b_engulf_a = row_b['close'] < row_a['close']
            c_is_bullish = row_c['close'] > row_c['open']
            c_engulf_b = row_c['close'] > row_b['open']
            c_above_ma20 = row_c['close'] > row_c['ma20']
            
            if a_is_bearish and b_is_bearish and b_engulf_a and c_is_bullish and c_engulf_b and c_above_ma20:
                target_info = {
                    'dt_str': row_c['dt'].isoformat(),
                    'close': float(row_c['close']),
                    'high': float(row_c['high']),
                    'low': float(row_c['low']),
                    'ma20': float(row_c['ma20'])
                }
                action = 'update'
                
        # 2. 如果沒有新訊號，但仍在追蹤名單中，執行剔除判斷
        if target_info is None and cached_info is not None:
            if row_c['close'] > cached_info['high'] or row_c['close'] < cached_info['low']:
                return {'symbol': symbol, 'action': 'remove'}
            else:
                target_info = cached_info
                action = 'keep'
                
        # 如果既沒新訊號且也不在追蹤名單中，直接結束
        if target_info is None:
            return None
            
        # === 3H 條件進階判斷 (針對 target_info 進行校驗) ===
        fetch_since_1h = now_ms - (30 * 24 * 3600 * 1000)
        batch_1h = await exchange.fetch_ohlcv(symbol, '1h', since=fetch_since_1h, limit=720)
        
        is_3h_met = False
        entry_price = 0.0
        stop_loss = 0.0
        
        if batch_1h:
            df_1h = pd.DataFrame(batch_1h, columns=['ts', 'open', 'high', 'low', 'close', 'vol']).drop_duplicates(subset=['ts']).reset_index(drop=True)
            df_1h['dt'] = pd.to_datetime(df_1h['ts'], unit='ms', utc=True)
            
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
            
            local_now_utc = pd.Timestamp.now(tz='UTC')
            now_utc_3h_fl = local_now_utc.floor('3h')
            df_3h = df_3h[df_3h['3h_period'] < now_utc_3h_fl].reset_index(drop=True)
            
            # 從當初成立的那根 3D 棒 (Bar C) 結束之後起算
            target_dt = pd.to_datetime(target_info['dt_str'])
            bar_c_end_time = target_dt + pd.Timedelta(days=3)
            df_3h_after_c = df_3h[df_3h['3h_period'] >= bar_c_end_time]
            
            target_3d_high = target_info['high']
            
            for _, h3_row in df_3h_after_c.iterrows():
                h3_open = h3_row['open']
                h3_close = h3_row['close']
                h3_low = h3_row['low']
                
                if h3_open <= target_3d_high and h3_close > target_3d_high:
                    is_3h_met = True
                    entry_price = h3_close
                    # 止損 = 該根 3H K棒最低點
                    stop_loss = h3_low
        # ========================

        d1_date_str = pd.to_datetime(target_info['dt_str']).strftime('%Y-%m-%d')
        
        return {
            'symbol': symbol,
            'action': action,
            'data': target_info,
            'is_3h_met': is_3h_met,
            'entry_price': entry_price,
            'stop_loss': stop_loss,
            'precision': precision,
            'd1_date': d1_date_str
        }

    except Exception as e:
        logger.warning(f"掃描異常 ({symbol}): {type(e).__name__}: {e}")
        return None

# ============================================================================
# 訊息推送模組
# ============================================================================

def send_watching_message(watching_list):
    """合併所有 3H 未成立的幣種為一則關注中訊息，按日期排列"""
    if not watching_list:
        return
    
    # 按 d1_date 分組
    date_groups = {}
    for item in watching_list:
        d = item['d1_date']
        if d not in date_groups:
            date_groups[d] = []
        date_groups[d].append(get_base_coin(item['symbol']))
    
    lines = ["👀 <b>[關注中]</b>\n"]
    for date_key in sorted(date_groups.keys()):
        coins = " · ".join(date_groups[date_key])
        lines.append(f"📅 {date_key}")
        lines.append(f"💎 {coins}\n")
    
    send_telegram_message("\n".join(lines))

def send_triggered_message(item, default_loss):
    """3H 已成立的幣種，獨立一則訊息，含倉位價值"""
    display_symbol = get_base_coin(item['symbol'])
    precision = item['precision']
    entry = item['entry_price']
    sl = item['stop_loss']
    
    # 倉位價值 = 預設虧損金額 / |(進場價 - 止損價) / 進場價|
    loss_pct = abs((entry - sl) / entry) if entry != 0 else 0
    position_value = default_loss / loss_pct if loss_pct > 0 else 0
    
    msg = (
        f"🟢 <b>[做多] 3D MA20 吞噬轉換</b>\n\n"
        f"💎 <b>交易對:</b> {display_symbol}\n"
        f"📅 <b>3D K棒起始日期:</b> {item['d1_date']}\n\n"
        f"📍 <b>進場價格:</b> <code>{entry:.{precision}f}</code>\n"
        f"🛡️ <b>止損價格:</b> <code>{sl:.{precision}f}</code>\n"
        f"💰 <b>倉位價值:</b> <code>{position_value:.2f} USDT</code>"
    )
    send_telegram_message(msg)

def send_system_settings_message(config):
    """獨立一則系統設定訊息"""
    loss = config.get("default_loss_amount", 6)
    bl = config.get("blacklist", [])
    bl_str = ", ".join(bl) if bl else "無"
    
    msg = (
        f"⚙️ <b>系統快速設定</b>\n\n"
        f"💵 <b>預設虧損金額:</b> {loss} USDT\n"
        f"🚫 <b>黑名單前綴:</b> {bl_str}\n\n"
        f"📝 <b>修改預設虧損:</b> 回覆 <code>/set_loss 10</code>\n"
        f"➕ <b>新增黑名單:</b> 回覆 <code>/add_blacklist BTC</code>\n"
        f"➖ <b>移除黑名單:</b> 回覆 <code>/remove_blacklist BTC</code>"
    )
    send_telegram_message(msg)

# ============================================================================
# Telegram 指令處理
# ============================================================================

# 全域 offset，避免重複處理同一條訊息
_tg_update_offset = 0

def poll_telegram_commands():
    """輪詢 Telegram getUpdates，處理 /set_loss 指令"""
    global _tg_update_offset
    if not TG_BOT_TOKEN:
        return
    
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/getUpdates"
    params = {"offset": _tg_update_offset, "timeout": 0, "limit": 20}
    
    try:
        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code != 200:
            return
        data = resp.json()
        if not data.get("ok"):
            return
        
        for update in data.get("result", []):
            _tg_update_offset = update["update_id"] + 1
            message = update.get("message", {})
            text = message.get("text", "").strip()
            chat_id = str(message.get("chat", {}).get("id", ""))
            
            # 僅處理來自目標 chat 的指令
            if chat_id != TG_CHAT_ID:
                continue
            
            if text.startswith("/set_loss"):
                parts = text.split()
                if len(parts) == 2:
                    try:
                        new_val = float(parts[1])
                        if new_val <= 0:
                            raise ValueError("金額必須大於 0")
                        config = load_config()
                        config["default_loss_amount"] = new_val
                        save_config(config)
                        reply = f"✅ 預設虧損金額已更新為 <b>{new_val} USDT</b>"
                        logger.info(f"⚙️ /set_loss 指令: 虧損金額更新為 {new_val}")
                    except ValueError:
                        reply = "❌ 格式錯誤，請使用: <code>/set_loss 數字</code>"
                else:
                    reply = "❌ 格式錯誤，請使用: <code>/set_loss 數字</code>"
                
                # 回覆訊息
                send_url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
                payload = {"chat_id": chat_id, "text": reply, "parse_mode": "HTML"}
                requests.post(send_url, json=payload, timeout=10)
                
            elif text.startswith("/add_blacklist"):
                parts = text.split()
                if len(parts) == 2:
                    new_bl = parts[1].upper()
                    config = load_config()
                    bl = config.get("blacklist", [])
                    if new_bl not in bl:
                        bl.append(new_bl)
                        config["blacklist"] = bl
                        save_config(config)
                        reply = f"✅ 已將 <b>{new_bl}</b> 加入黑名單"
                        logger.info(f"⚙️ 加入黑名單: {new_bl}")
                    else:
                        reply = f"⚠️ <b>{new_bl}</b> 已經在黑名單中"
                else:
                    reply = "❌ 格式錯誤，請使用: <code>/add_blacklist 幣種</code>"
                
                send_url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
                payload = {"chat_id": chat_id, "text": reply, "parse_mode": "HTML"}
                requests.post(send_url, json=payload, timeout=10)
                
            elif text.startswith("/remove_blacklist"):
                parts = text.split()
                if len(parts) == 2:
                    rm_bl = parts[1].upper()
                    config = load_config()
                    bl = config.get("blacklist", [])
                    if rm_bl in bl:
                        bl.remove(rm_bl)
                        config["blacklist"] = bl
                        save_config(config)
                        reply = f"✅ 已將 <b>{rm_bl}</b> 移出黑名單"
                        logger.info(f"⚙️ 移除黑名單: {rm_bl}")
                    else:
                        reply = f"⚠️ <b>{rm_bl}</b> 不在黑名單中"
                else:
                    reply = "❌ 格式錯誤，請使用: <code>/remove_blacklist 幣種</code>"
                
                send_url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
                payload = {"chat_id": chat_id, "text": reply, "parse_mode": "HTML"}
                requests.post(send_url, json=payload, timeout=10)
                
    except Exception as e:
        logger.warning(f"Telegram 指令輪詢異常: {e}")

# ============================================================================
# 主掃描流程
# ============================================================================

async def run_scan():
    logger.info("⏰ 開始執行 3D MA20 與 3H 條件長效追蹤掃描...")
    ex = get_exchange()
    watchlist = load_watchlist()
    config = load_config()
    default_loss = config.get("default_loss_amount", 6)
    
    try:
        try:
            markets = await ex.load_markets()
            coins = [s for s, m in markets.items() if m.get('linear') and m.get('quote') == 'USDT' and is_crypto_symbol(s, config.get("blacklist", []))]
            precisions = {s: max(0, int(round(-np.log10(markets[s].get('precision', {}).get('price', 1e-8))))) for s in coins}
        except:
            coins = ["BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT", "XRP/USDT:USDT", "DOGE/USDT:USDT", "ADA/USDT:USDT"]
            precisions = {s: 4 for s in coins}; precisions.update({"BTC/USDT:USDT": 2, "ETH/USDT:USDT": 2})

        all_results = []
        total_coins = len(coins)
        for i in range(0, total_coins, 20):
            batch = coins[i:i+20]
            tasks = [scan_for_symbol(ex, s, get_base_coin(s), precisions[s], i + idx + 1, total_coins, watchlist.get(s)) for idx, s in enumerate(batch)]
            results = await asyncio.gather(*tasks)
            
            for res in results:
                if res is None: continue
                sym = res['symbol']
                if res['action'] == 'update' or res['action'] == 'keep':
                    watchlist[sym] = res['data']
                    all_results.append(res)
                elif res['action'] == 'remove':
                    if sym in watchlist:
                        del watchlist[sym]
            
            await asyncio.sleep(0.5)
            
        # 回寫快取
        save_watchlist(watchlist)
        active_count = len(watchlist)
        
        # === 分組推送 ===
        watching_list = [r for r in all_results if not r.get('is_3h_met')]
        triggered_list = [r for r in all_results if r.get('is_3h_met')]
        
        if watching_list:
            send_watching_message(watching_list)
        
        for item in triggered_list:
            send_triggered_message(item, default_loss)
        
        # 有任何結果時才發送系統設定訊息
        if watching_list or triggered_list:
            send_system_settings_message(config)
        
        pushed_count = len(watching_list) + len(triggered_list)
        logger.info(f"✅ 掃描完成。關注中: {len(watching_list)} / 已觸發: {len(triggered_list)} / 追蹤總數: {active_count}")
        
        if not all_results:
            send_telegram_message(f"✅ <b>條件掃描完成</b>\n本次共掃描 {total_coins} 個幣種，無推播訊號。\n(當前清單追蹤中: {active_count} 個)")
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
            
            # 每次迴圈都輪詢 Telegram 指令
            poll_telegram_commands()
            
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

logger.info("🚀 啟動 3D 結構專用掃描系統 (長效推播模式)...")
bg_thread = threading.Thread(target=run_background_system, daemon=True)
bg_thread.start()

if __name__ == '__main__':
    run_flask()
