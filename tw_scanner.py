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
    protect_sl = item.get('protect_sl', 0.0)
    
    tw_loss = load_tw_loss_amount()
    risk_per_share = entry_price - stop_loss
    shares = int(tw_loss / risk_per_share) if risk_per_share > 0 else 0
    
    msg = (
        f"<b>🚀 台股(持倉中)</b>\n\n"
        f"💎 <b>標的:</b> {item['symbol']} {item['name']}\n"
        f"📅 <b>條件一日期:</b> <code>{item.get('c1_date', '未知')}</code>\n"
        f"📅 <b>條件二日期:</b> <code>{item.get('c2_date', '未知')}</code>\n"
        f"📍 <b>進場價格:</b> <code>{entry_price:.2f}</code>\n"
        f"🛡️ <b>止損價格:</b> <code>{stop_loss:.2f}</code>\n"
    )
    
    if protect_sl > stop_loss:
        msg += f"🛡️ <b>保護止損價格:</b> <code>{protect_sl:.2f}</code>\n"
        
    msg += f"📊 <b>股數:</b> <code>{shares:,}股</code>"
    send_telegram_message(msg)

# ============================================================================
# 3D K 棒合成工具 (移植自 main.py)
# ============================================================================
def compose_3d_bars(ohlcv_1d):
    if not ohlcv_1d or len(ohlcv_1d) < 3:
        return []
    
    from datetime import datetime, timezone
    
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
    # 每個年份獨立切分，每 3 個營業日合成一根 3D K 棒
    for y in sorted(years_data.keys()):
        bars_of_year = sorted(years_data[y], key=lambda x: x[0])
        for i in range(0, len(bars_of_year), 3):
            bars = bars_of_year[i:i+3]
            
            gts = bars[0][0]
            result.append([
                gts,                         # 以該 3D 棒的第一個營業日作為 K 棒時間
                bars[0][1],                  # open
                max(b[2] for b in bars),     # high
                min(b[3] for b in bars),     # low
                bars[-1][4],                 # close
                sum(b[5] for b in bars),     # vol
                bars[-1][0] + 24 * 3600 * 1000 # close_ts (最後一根營業日的結束時間)
            ])
            
    return result

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
    
    # Fugle API 有單次請求的時間跨度限制，因此分兩段請求：去年整年 + 今年至今
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
                        # 404 表示該段期間沒有此股票的資料 (可能尚未上市)，直接跳過不需重試
                        break
                    else:
                        logger.warning(f"⚠️ Fugle API 回應異常 (狀態碼: {response.status}) {symbol} [{from_date_str}]，準備重試...")
                        await asyncio.sleep(1)
            except Exception as e:
                logger.warning(f"⚠️ Fugle API 請求異常 {symbol} ({e})，進行第 {attempt} 次重試...")
                await asyncio.sleep(1)
                
    if not all_klines:
        return None
        
    # 確保回傳資料統一為「由新到舊」(Descending)，配合後續 scan_stock 裡的 reversed 邏輯
    all_klines.sort(key=lambda x: x["date"], reverse=True)
    return all_klines

async def scan_stock(session, stock_info, semaphore, current_idx, total):
    async with semaphore:
        # 防範 Fugle API Rate Limit (歷史行情上限 60/min，因為現在每次掃描分兩段請求，故延長為 2.1s 進行流量整形)
        await asyncio.sleep(2.1)
        
        symbol = stock_info["symbol"]
        name = stock_info["name"]
        
        if current_idx % 50 == 0 or current_idx == total:
            logger.info(f"📊 掃描進度: {current_idx}/{total}...")
            
        candles = await fetch_historical_candles(session, symbol)
        
        if not candles or len(candles) < 15:
            return None
            
            
        ohlcv_1d = []
        for row in reversed(candles):
            try:
                dt = pd.to_datetime(row["date"]).tz_localize(None).replace(tzinfo=timezone.utc)
                ts = int(dt.timestamp() * 1000)
                ohlcv_1d.append([ts, float(row["open"]), float(row["high"]), float(row["low"]), float(row["close"]), float(row.get("volume", 0))])
            except Exception as e:
                continue
                
        ohlcv_3d = compose_3d_bars(ohlcv_1d)
        if not ohlcv_3d or len(ohlcv_3d) < 15:
            return None
            
        df_3d = pd.DataFrame(ohlcv_3d, columns=['ts', 'open', 'high', 'low', 'close', 'vol', 'close_ts'])
        df_3d = df_3d.sort_values('ts').drop_duplicates(subset=['ts']).reset_index(drop=True)
        df_3d['dt'] = pd.to_datetime(df_3d['ts'], unit='ms', utc=True)
        
        # 使用 3D K 棒計算 10MA
        df_3d['sma_10'] = df_3d['close'].rolling(window=10).mean()
        
        target_info_c1 = None
        target_info_c2 = None
        state = 0
        c2_low = 0.0
        dynamic_sl = 0.0
        entry_price_scan = 0.0
        
        for i in range(10, len(df_3d)):
            bar = df_3d.iloc[i]
            prev1 = df_3d.iloc[i-1]
            prev2 = df_3d.iloc[i-2]
            
            prev1_body_high = max(prev1['open'], prev1['close'])
            prev2_body_high = max(prev2['open'], prev2['close'])
            
            if state == 0:
                # 條件一：收盤價小於前兩天實體高點 (回檔)，且這三天收盤價均大於 10MA
                c1_met = (
                    bar['close'] < prev1_body_high and bar['close'] < prev2_body_high and 
                    bar['close'] > bar['sma_10'] and prev1['close'] > prev1['sma_10'] and prev2['close'] > prev2['sma_10']
                )
                
                if c1_met:
                    state = 1
                    target_info_c1 = {
                        'dt_str': bar['dt'].isoformat(), 'ts': int(bar['ts']),
                        'close': float(bar['close']), 'high': float(bar['high']), 'low': float(bar['low'])
                    }
            elif state == 1:
                # 等待期：條件一成立後，尋找條件二 (可不連續日期)
                if bar['close'] < bar['sma_10']:
                    # 跌破 10MA，條件失效，重新尋找條件一
                    state = 0
                    
                    # 順便檢查今天是否剛好符合新的條件一
                    c1_met = (
                        bar['close'] < bar['open'] and prev1['close'] < prev1['open'] and 
                        bar['close'] < prev1_body_high and bar['close'] < prev2_body_high and 
                        bar['close'] > bar['sma_10'] and prev1['close'] > prev1['sma_10'] and prev2['close'] > prev2['sma_10']
                    )
                    if c1_met:
                        state = 1
                        target_info_c1 = {
                            'dt_str': bar['dt'].isoformat(), 'ts': int(bar['ts']),
                            'close': float(bar['close']), 'high': float(bar['high']), 'low': float(bar['low'])
                        }
                elif bar['close'] > max(prev1_body_high, prev2_body_high) and bar['close'] > bar['sma_10']:
                    # 條件二成立：收盤大於「前面兩根」的實體高點且大於10MA，止損距離 <= 10%
                    sl_distance = (bar['close'] - bar['low']) / bar['close'] if bar['close'] > 0 else 1.0
                    if sl_distance <= 0.10:
                        state = 3
                        target_info_c2 = {
                            'dt_str': bar['dt'].isoformat(), 'ts': int(bar['ts']),
                            'close': float(bar['close']), 'high': float(bar['high']), 'low': float(bar['low']),
                            'c1_dt_str': prev2['dt'].isoformat(),
                            'gap_dt_str': prev1['dt'].isoformat()
                        }
                        c2_low = float(bar['low'])
                        dynamic_sl = c2_low
                        entry_price_scan = float(bar['close'])
                    else:
                        # 止損距離過大，訊號直接失效，重新尋找條件一
                        state = 0
                        target_info_c1 = None
            elif state == 3:
                # 在主迴圈內更新保護止損：若當前 3D K 棒收盤高於前一根 3D 實體高點且低點大於進場價
                if bar['close'] > prev1_body_high and bar['low'] > entry_price_scan:
                    if bar['low'] > dynamic_sl:
                        dynamic_sl = float(bar['low'])

                # 淘汰條件：最低價跌破動態止損，或收盤跌破 10MA
                if bar['low'] <= dynamic_sl or bar['close'] < bar['sma_10']:
                    state = 0
                    dynamic_sl = 0.0
                    
                    # 重新判斷是否立即觸發新的條件一
                    c1_met = (
                        bar['close'] < prev1_body_high and bar['close'] < prev2_body_high and 
                        bar['close'] > bar['sma_10'] and prev1['close'] > prev1['sma_10'] and prev2['close'] > prev2['sma_10']
                    )
                    if c1_met:
                        state = 1
                        target_info_c1 = {
                            'dt_str': bar['dt'].isoformat(), 'ts': int(bar['ts']),
                            'close': float(bar['close']), 'high': float(bar['high']), 'low': float(bar['low'])
                        }

        if state not in [1, 3]:
            return None

        is_trigger = (state == 3)

        return {
            'symbol': symbol,
            'name': name,
            'is_trigger_met': is_trigger,
            'entry_price': float(target_info_c2['close']) if is_trigger else 0.0,
            'stop_loss': float(target_info_c2['low']) if is_trigger else 0.0,
            'protect_sl': float(dynamic_sl) if is_trigger else 0.0,
            'd1_date': "省略",
            'c1_date': pd.to_datetime(target_info_c1['dt_str']).strftime('%Y-%m-%d') if target_info_c1 else "未知",
            'c2_date': pd.to_datetime(target_info_c2['dt_str']).strftime('%Y-%m-%d') if is_trigger else "未知"
        }

async def main_loop():
    stocks = get_tw_stock_list()
    if not stocks:
        logger.error("無法取得台股清單，終止掃描。")
        return

    logger.info(f"🚀 開始全市場掃描，共 {len(stocks)} 檔標的...")
    
    # 富果歷史行情 API 限制為 60/min，在此設定為單併發以確保 Traffic Shaping 完全生效
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
