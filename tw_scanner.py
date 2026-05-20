#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# 檔名: tw_scanner.py

import asyncio
import aiohttp
import requests
import pandas as pd
import logging
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
import time

# ============================================================================
# 系統初始化
# ============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("TWScanner")

load_dotenv()

TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "")
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "")
FUGLE_API_KEY = os.getenv("FUGLE_API_KEY", "")

def load_tw_loss_amount() -> float:
    config_file = "/app/data/system_config.json"
    if os.path.exists(config_file):
        try:
            import json
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
            return float(config.get("default_tw_loss_amount", 300))
        except Exception:
            pass
    return float(os.getenv("DEFAULT_LOSS_TWD", "300"))

if not FUGLE_API_KEY:
    logger.error("❌ 缺少 FUGLE_API_KEY，請在 .env 中設定。")
    exit(1)

# ============================================================================
# Telegram 通知
# ============================================================================
def send_telegram_message(message: str) -> bool:
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        logger.warning("Telegram 配置缺失，跳過通知。")
        return False
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TG_CHAT_ID, "text": message, "parse_mode": "HTML"}
    try:
        response = requests.post(url, json=payload, timeout=10)
        return response.status_code == 200
    except Exception as e:
        logger.error(f"Telegram 連線異常: {e}")
        return False

def send_grouped_message(item_list, title):
    if not item_list:
        return
    date_groups = {}
    for item in item_list:
        d = item.get('c1_date', '未知日期')
        if d not in date_groups:
            date_groups[d] = []
        date_groups[d].append(item)

    lines = [f"<b>{title}</b>\n"]
    for date_key in sorted(date_groups.keys()):
        stock_strs = []
        for item in date_groups[date_key]:
            stock_strs.append(f"{item['symbol']} {item['name']}")
        lines.append(f"📅 {date_key}")
        lines.append(f"💎 {' · '.join(stock_strs)}\n")

    send_telegram_message("\n".join(lines))

def send_triggered_message(item):
    entry_price = item['entry_price']
    stop_loss = item['stop_loss']
    
    tw_loss = load_tw_loss_amount()
    risk_per_share = entry_price - stop_loss
    shares = int(tw_loss / risk_per_share) if risk_per_share > 0 else 0
    
    msg = (
        f"<b>🚀 台股(持倉中)</b>\n\n"
        f"💎 <b>標的:</b> {item['symbol']} {item['name']}\n"
        f"📅 <b>L1 日期:</b> <code>{item.get('l1_date', '未知')}</code>\n"
        f"📅 <b>L2 日期:</b> <code>{item.get('l2_date', '未知')}</code>\n"
        f"📅 <b>C1 日期:</b> <code>{item.get('c1_date', '未知')}</code>\n"
        f"📅 <b>C2 日期:</b> <code>{item.get('c2_date', '未知')}</code>\n"
        f"📍 <b>進場價格:</b> <code>{entry_price:.2f}</code>\n"
        f"🛡️ <b>止損價格:</b> <code>{stop_loss:.2f}</code>\n"
        f"📊 <b>股數:</b> <code>{shares:,}股</code>"
    )
    send_telegram_message(msg)

# ============================================================================
# K 棒合成工具 (按營業日順序，移植自加密貨幣架構)
# ============================================================================
def compose_nd_bars(ohlcv_1d, n):
    if not ohlcv_1d or len(ohlcv_1d) < n:
        return []
    
    # 按照年份分組
    years_data = {}
    for bar in ohlcv_1d:
        ts = bar[0]
        dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
        y = dt.year
        if y not in years_data:
            years_data[y] = []
        years_data[y].append(bar)
        
    result = []
    # 每個年份獨立切分，每 n 個營業日合成一根 nD K 棒
    for y in sorted(years_data.keys()):
        bars_of_year = sorted(years_data[y], key=lambda x: x[0])
        for i in range(0, len(bars_of_year), n):
            bars = bars_of_year[i:i+n]
            gts = bars[0][0]
            result.append([
                gts,                           # 以該 K 棒的第一個營業日作為時間戳
                bars[0][1],                    # open
                max(b[2] for b in bars),       # high
                min(b[3] for b in bars),       # low
                bars[-1][4],                   # close
                sum(b[5] for b in bars),       # vol
                bars[-1][0] + 24 * 3600 * 1000 # close_ts (最後一根營業日的結束時間)
            ])
            
    return result

def compose_18d_bars(ohlcv_1d):
    return compose_nd_bars(ohlcv_1d, 18)

def compose_49d_bars(ohlcv_1d):
    return compose_nd_bars(ohlcv_1d, 49)

# ============================================================================
# 資料獲取與掃描
# ============================================================================
def get_tw_stock_list():
    stocks = []
    logger.info("正在獲取台股上市櫃清單...")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    # 獲取上市 (TWSE) - 最多重試 5 次
    twse_stocks = []
    for attempt in range(1, 6):
        try:
            res = requests.get("https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL", headers=headers, timeout=15)
            if res.status_code == 200:
                data = res.json()
                if isinstance(data, list) and len(data) > 0:
                    for item in data:
                        code = item.get("Code", "")
                        if len(code) == 4 and not code.startswith("0") and code.isdigit():
                            twse_stocks.append({"symbol": code, "name": item.get("Name", "")})
                    if twse_stocks:
                        logger.info(f"成功獲取上市 (TWSE) 清單，共 {len(twse_stocks)} 檔標的")
                        break
            logger.warning(f"獲取上市清單第 {attempt} 次失敗 (狀態碼: {res.status_code})，準備重試...")
        except Exception as e:
            logger.warning(f"獲取上市清單第 {attempt} 次異常: {e}，準備重試...")
        time.sleep(2)

    # 獲取上櫃 (TPEx) - 最多重試 5 次
    tpex_stocks = []
    for attempt in range(1, 6):
        try:
            res = requests.get("https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes", headers=headers, timeout=15)
            if res.status_code == 200:
                data = res.json()
                if isinstance(data, list) and len(data) > 0:
                    for item in data:
                        code = item.get("SecuritiesCompanyCode", "")
                        if len(code) == 4 and not code.startswith("0") and code.isdigit():
                            tpex_stocks.append({"symbol": code, "name": item.get("CompanyName", "")})
                    if tpex_stocks:
                        logger.info(f"成功獲取上櫃 (TPEx) 清單，共 {len(tpex_stocks)} 檔標的")
                        break
            logger.warning(f"獲取上櫃清單第 {attempt} 次失敗 (狀態碼: {res.status_code})，準備重試...")
        except Exception as e:
            logger.warning(f"獲取上櫃清單第 {attempt} 次異常: {e}，準備重試...")
        time.sleep(2)

    # 組合上市櫃
    stocks.extend(twse_stocks)
    stocks.extend(tpex_stocks)

    # 防禦性檢查：如果其中一個主要清單為空，說明資料抓取嚴重不完整，應發送警報
    if not twse_stocks or not tpex_stocks:
        err_msg = f"⚠️ 台股清單獲取不完整！上市(TWSE): {len(twse_stocks)} 檔, 上櫃(TPEx): {len(tpex_stocks)} 檔。請檢查網路或政府 API 狀態。"
        logger.error(err_msg)
        send_telegram_message(f"🚨 <b>系統異常告警</b>\n\n{err_msg}")
        # 若完全沒有抓到上市股票，為了防止產出破碎的結果，在此直接拋出異常讓排程重試
        if not twse_stocks:
            raise RuntimeError("無法獲取上市 (TWSE) 股票清單，拒絕執行破碎掃描")

    logger.info(f"成功獲取 {len(stocks)} 檔普通股標的。")
    return stocks

async def fetch_historical_candles(session, symbol):
    now = datetime.now()
    year = now.year
    
    # 獲取 2 年歷史以確保狀態演進有足夠長度，並優化為 2 次請求以減輕富果 API 頻率限制
    ranges = [
        (f"{year - 1}-01-01", f"{year - 1}-12-31"),
        (f"{year}-01-01", now.strftime("%Y-%m-%d"))
    ]
    
    all_klines = []
    headers = {"X-API-KEY": FUGLE_API_KEY}
    
    for from_date_str, to_date_str in ranges:
        url = f"https://api.fugle.tw/marketdata/v1.0/stock/historical/candles/{symbol}?from={from_date_str}&to={to_date_str}&timeframe=D"
        for attempt in range(1, 4):
            try:
                async with session.get(url, headers=headers, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        candles = data.get("data", [])
                        if candles:
                            all_klines.extend(candles)
                        break
                    elif response.status == 429:
                        sleep_time = attempt * 2
                        logger.warning(f"⚠️ Fugle API 頻率限制 (429) {symbol}，等待 {sleep_time} 秒後進行第 {attempt} 次重試...")
                        await asyncio.sleep(sleep_time)
                    elif response.status == 403:
                        logger.error(f"❌ Fugle API 權限錯誤 (403)，請檢查 API Key 是否有效！")
                        return None
                    elif response.status == 404:
                        break
                    else:
                        logger.warning(f"⚠️ Fugle API 回應異常 (狀態碼: {response.status}) {symbol} [{from_date_str}]，準備重試...")
                        await asyncio.sleep(1)
            except Exception as e:
                logger.warning(f"⚠️ Fugle API 請求異常 {symbol} ({e})，進行第 {attempt} 次重試...")
                await asyncio.sleep(1)
                
    if not all_klines:
        return None
        
    all_klines.sort(key=lambda x: x["date"], reverse=True)
    return all_klines

async def scan_stock(session, stock_info, semaphore, current_idx, total):
    async with semaphore:
        # 防範 Fugle API Rate Limit
        await asyncio.sleep(2.1)
        
        symbol = stock_info["symbol"]
        name = stock_info["name"]
        
        if current_idx % 50 == 0 or current_idx == total:
            logger.info(f"📊 掃描進度: {current_idx}/{total}...")
            
        candles = await fetch_historical_candles(session, symbol)
        if not candles or len(candles) < 150:
            return None
            
        ohlcv_1d = []
        for row in reversed(candles):
            try:
                dt = pd.to_datetime(row["date"]).tz_localize(None).replace(tzinfo=timezone.utc)
                ts = int(dt.timestamp() * 1000)
                ohlcv_1d.append([ts, float(row["open"]), float(row["high"]), float(row["low"]), float(row["close"]), float(row.get("volume", 0))])
            except Exception:
                continue
                
        # 合成 K 棒
        ohlcv_49d = compose_49d_bars(ohlcv_1d)
        ohlcv_18d = compose_18d_bars(ohlcv_1d)
        
        if not ohlcv_49d or not ohlcv_18d:
            return None
            
        df_49d = pd.DataFrame(ohlcv_49d, columns=['ts', 'open', 'high', 'low', 'close', 'vol', 'close_ts'])
        df_49d = df_49d.sort_values('ts').drop_duplicates(subset=['ts']).reset_index(drop=True)

        df_18d = pd.DataFrame(ohlcv_18d, columns=['ts', 'open', 'high', 'low', 'close', 'vol', 'close_ts'])
        df_18d = df_18d.sort_values('ts').drop_duplicates(subset=['ts']).reset_index(drop=True)
        df_18d['ma_10'] = df_18d['close'].rolling(window=10).mean()

        # L3 直接使用 1D 棒
        df_1d = pd.DataFrame(ohlcv_1d, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
        df_1d['close_ts'] = df_1d['ts'] + 24 * 3600 * 1000
        df_1d = df_1d.sort_values('ts').drop_duplicates(subset=['ts']).reset_index(drop=True)
        df_1d['dt'] = pd.to_datetime(df_1d['ts'], unit='ms', utc=True)
        df_1d['ma_100'] = df_1d['close'].rolling(window=100).mean()
        
        now_utc = int(time.time() * 1000)
        df_49d_closed = df_49d[df_49d['close_ts'] <= now_utc].reset_index(drop=True)
        df_18d_closed = df_18d[df_18d['close_ts'] <= now_utc].reset_index(drop=True)
        df_1d_closed = df_1d[df_1d['close_ts'] <= now_utc].reset_index(drop=True)
        
        if len(df_1d_closed) < 105 or len(df_18d_closed) < 11:
            return None
            
        # 1. 處理 49D (L1) 事件
        l1_events = {}
        for i in range(1, len(df_49d_closed)):
            b = df_49d_closed.iloc[i]
            p = df_49d_closed.iloc[i - 1]
            t = int(b['close_ts'])
            dt_str = pd.to_datetime(b['ts'], unit='ms', utc=True).tz_convert('Asia/Taipei').strftime('%Y-%m-%d')
            if b['close'] > b['open'] and p['close'] < p['open']:
                l1_events[t] = {'type': 'found', 'dt_str': dt_str}
            elif b['close'] < b['open']:
                l1_events[t] = {'type': 'invalid'}
                
        dict_18d = {int(row['close_ts']): row for _, row in df_18d_closed.iterrows()}
        
        # 狀態變數
        l1_valid = False
        l2_valid = False
        c1_valid = False
        c2_valid = False
        
        l1_valid_ts = 0
        l2_valid_ts = 0
        
        l1_date_str = "未知"
        l2_date_str = "未知"
        c1_date_str = "未知"
        c2_date_str = "未知"
        
        entry_price = 0.0
        stop_loss = 0.0
        trigger_ts = 0

        # 主迴圈：以 1D (L3) 推進
        for _, row in df_1d_closed.iterrows():
            t = int(row['close_ts'])
            
            # L1 事件
            if t in l1_events:
                evt = l1_events[t]
                if evt['type'] == 'found':
                    l1_valid = True
                    l1_valid_ts = t
                    l1_date_str = evt['dt_str']
                    l2_valid = False
                    c1_valid = False
                    l2_valid_ts = 0
                    l2_date_str = "未知"
                    c1_date_str = "未知"
                elif evt['type'] == 'invalid':
                    l1_valid = False
                    l2_valid = False
                    c1_valid = False
                    c2_valid = False
                    l2_valid_ts = 0
                    l2_date_str = "未知"
                    c1_date_str = "未知"
                    c2_date_str = "未知"
            
            # L2 事件
            if t in dict_18d:
                b18d = dict_18d[t]
                if pd.notna(b18d['ma_10']):
                    if b18d['close'] < b18d['ma_10']:
                        l2_valid = False
                        c1_valid = False
                        c2_valid = False
                        l2_valid_ts = 0
                        l2_date_str = "未知"
                        c1_date_str = "未知"
                        c2_date_str = "未知"
                    elif l1_valid and not l2_valid:
                        if b18d['ts'] >= l1_valid_ts:
                            if b18d['close'] > b18d['open'] and b18d['close'] > b18d['ma_10']:
                                l2_valid = True
                                l2_valid_ts = t
                                l2_date_str = pd.to_datetime(b18d['ts'], unit='ms', utc=True).tz_convert('Asia/Taipei').strftime('%Y-%m-%d')
                                c1_valid = False
                                c1_date_str = "未知"
                            
            # L3 事件
            if pd.isna(row['ma_100']):
                continue
                
            if c2_valid:
                if row['low'] <= stop_loss:
                    c2_valid = False
                    c1_valid = False 
                    c1_date_str = "未知"
                    c2_date_str = "未知"
            else:
                if l2_valid:
                    if not c1_valid:
                        if row['ts'] >= l2_valid_ts:
                            if row['close'] < row['ma_100']:
                                c1_valid = True
                                c1_date_str = pd.to_datetime(row['ts'], unit='ms', utc=True).tz_convert('Asia/Taipei').strftime('%Y-%m-%d')
                    else:
                        if row['close'] > row['ma_100']:
                            _candidate_entry = float(row['close'])
                            _candidate_sl = float(row['low'])
                            _sl_distance_pct = abs(_candidate_entry - _candidate_sl) / _candidate_entry * 100 if _candidate_entry > 0 else 999
                            if _sl_distance_pct <= 10:
                                c2_valid = True
                                c2_date_str = pd.to_datetime(row['ts'], unit='ms', utc=True).tz_convert('Asia/Taipei').strftime('%Y-%m-%d')
                                entry_price = _candidate_entry
                                stop_loss = _candidate_sl
                                trigger_ts = int(row['ts'])
                            else:
                                c1_valid = False
                                c1_date_str = "未知"

        is_trigger_met = c2_valid
        if is_trigger_met:
            final_state = 'triggered'
        elif c1_valid:
            final_state = 'l3_c2_waiting'
        elif l2_valid:
            final_state = 'l3_c1_waiting'
        elif l1_valid:
            final_state = 'l2_waiting'
        else:
            final_state = 'l1_waiting'

        # 僅保留觸發 (triggered) 與 C1 成立在等 C2 (l3_c2_waiting) 的關注標的
        if final_state not in ['triggered', 'l3_c2_waiting']:
            return None

        return {
            'symbol':             symbol,
            'name':               name,
            'is_trigger_met':     is_trigger_met,
            'entry_price':        entry_price,
            'stop_loss':          stop_loss,
            'c1_date':            c1_date_str,
            'c2_date':            c2_date_str,
            'l1_date':            l1_date_str,
            'l2_date':            l2_date_str,
            'scan_state':         final_state
        }

async def main_loop():
    stocks = get_tw_stock_list()
    if not stocks:
        logger.error("無法取得台股清單，終止掃描。")
        return

    logger.info(f"🚀 開始全市場掃描，共 {len(stocks)} 檔標的...")
    
    # 限制單併發防範富果 API Rate Limit
    semaphore = asyncio.Semaphore(1)
    results = []
    
    async with aiohttp.ClientSession() as session:
        tasks = []
        for idx, stock in enumerate(stocks):
            tasks.append(scan_stock(session, stock, semaphore, idx + 1, len(stocks)))
            
        raw_results = await asyncio.gather(*tasks)
        results = [r for r in raw_results if r is not None]

    logger.info(f"✅ 掃描完成。篩選出 {len(results)} 檔標的。")

    watchlist_items = []
    triggered_items = []
    
    for r in results:
        if r['is_trigger_met']:
            triggered_items.append(r)
        else:
            watchlist_items.append(r)

    if triggered_items:
        for item in triggered_items:
            send_triggered_message(item)

    if watchlist_items:
        send_grouped_message(watchlist_items, "🔍 台股(關注中)")

    if not watchlist_items and not triggered_items:
        send_telegram_message("🔍 今日台股掃描完成：無符合條件之標的。")

if __name__ == "__main__":
    asyncio.run(main_loop())
