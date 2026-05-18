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
    msg = (
        f"<b>🚀 台股掃描觸發進場</b>\n\n"
        f"💎 <b>標的:</b> {item['symbol']} {item['name']}\n"
        f"📅 <b>條件一日期:</b> <code>{item.get('c1_date', '未知')}</code>\n"
        f"📅 <b>條件二日期:</b> <code>{item.get('c2_date', '未知')}</code>\n"
        f"📍 <b>收盤價格:</b> <code>{item['entry_price']:.2f}</code>\n"
        f"🛡️ <b>保護止損:</b> <code>{item['stop_loss']:.2f}</code>"
    )
    send_telegram_message(msg)

# ============================================================================
# 3D K 棒合成工具 (移植自 main.py)
# ============================================================================
def compose_3d_bars(ohlcv_1d):
    if not ohlcv_1d or len(ohlcv_1d) < 3:
        return []
    
    # 台股特製版：捨棄加密貨幣的固定 UTC 日曆分組
    # 改為直接依賴「實際交易日」，每 3 根 K 線合成一根 3D K 棒
    # 完美避開週末或國定假日導致的「單日殘缺 3D 棒」問題
    result = []
    
    for i in range(0, len(ohlcv_1d), 3):
        bars = ohlcv_1d[i:i+3]
        
        # 必須滿 3 根才合成，除非是最新的一組 (允許進行中)
        is_completed = (len(bars) == 3) or (i + 3 >= len(ohlcv_1d))
        if not is_completed:
            continue
            
        gts = bars[0][0]
        result.append([
            gts,
            bars[0][1], # open
            max(b[2] for b in bars), # high
            min(b[3] for b in bars), # low
            bars[-1][4], # close
            sum(b[5] for b in bars), # vol
            bars[-1][0] + 24 * 3600 * 1000 # close_ts (最後一根 K 的結束時間)
        ])
    return result

# ============================================================================
# 資料獲取與掃描
# ============================================================================
def get_tw_stock_list():
    stocks = []
    logger.info("正在獲取台股上市櫃清單...")
    
    # 獲取上市 (TWSE)
    try:
        res = requests.get("https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL", timeout=10)
        for item in res.json():
            code = item.get("Code", "")
            # 排除 ETF (0 開頭) 與權證等非普通股，保留 4 碼純數字普通股
            if len(code) == 4 and not code.startswith("0") and code.isdigit():
                stocks.append({"symbol": code, "name": item.get("Name", "")})
    except Exception as e:
        logger.error(f"獲取上市清單失敗: {e}")

    # 獲取上櫃 (TPEx)
    try:
        res = requests.get("https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes", timeout=10)
        for item in res.json():
            code = item.get("SecuritiesCompanyCode", "")
            if len(code) == 4 and not code.startswith("0") and code.isdigit():
                stocks.append({"symbol": code, "name": item.get("CompanyName", "")})
    except Exception as e:
        logger.error(f"獲取上櫃清單失敗: {e}")
        
    logger.info(f"成功獲取 {len(stocks)} 檔普通股標的。")
    return stocks

async def fetch_historical_candles(session, symbol):
    today_str = datetime.now().strftime("%Y-%m-%d")
    from_date_str = (datetime.now() - pd.Timedelta(days=200)).strftime("%Y-%m-%d")
    url = f"https://api.fugle.tw/marketdata/v1.0/stock/historical/candles/{symbol}?from={from_date_str}&to={today_str}&timeframe=D"
    headers = {"X-API-KEY": FUGLE_API_KEY}
    
    try:
        async with session.get(url, headers=headers, timeout=10) as response:
            if response.status != 200:
                return None
            data = await response.json()
            return data.get("data", [])
    except Exception:
        return None

async def scan_stock(session, stock_info, semaphore, current_idx, total):
    async with semaphore:
        # 防範 Fugle API Rate Limit (假設 60/min 則需較長睡眠，此處暫定 0.1s 平滑請求)
        await asyncio.sleep(0.1)
        
        symbol = stock_info["symbol"]
        name = stock_info["name"]
        
        if current_idx % 50 == 0 or current_idx == total:
            logger.info(f"📊 掃描進度: {current_idx}/{total}...")
            
        candles = await fetch_historical_candles(session, symbol)
        
        if current_idx == 1:
            logger.info(f"🔍 API 測試回應 ({symbol}): {candles[:2] if candles else '無資料或連線失敗'}")
            
        if not candles or len(candles) < 15:
            return None
            
        ohlcv_1d = []
        for row in reversed(candles):
            try:
                dt = pd.to_datetime(row["date"]).tz_localize(None).replace(tzinfo=timezone.utc)
                ts = int(dt.timestamp() * 1000)
                ohlcv_1d.append([ts, float(row["open"]), float(row["high"]), float(row["low"]), float(row["close"]), float(row.get("volume", 0))])
            except Exception as e:
                if current_idx == 1:
                    logger.warning(f"日期解析失敗: {row.get('date')} -> {e}")
                continue
                
        df_1d = pd.DataFrame(ohlcv_1d, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
        df_1d = df_1d.sort_values('ts').drop_duplicates(subset=['ts']).reset_index(drop=True)
        df_1d['dt'] = pd.to_datetime(df_1d['ts'], unit='ms', utc=True)
        
        df_1d['sma_10'] = df_1d['close'].rolling(window=10).mean()
        
        target_info_c1 = None
        target_info_c2 = None
        state = 0
        c2_low = 0.0
        dynamic_sl = 0.0
        entry_price_scan = 0.0
        
        for i in range(10, len(df_1d)):
            bar = df_1d.iloc[i]
            prev1 = df_1d.iloc[i-1]
            prev2 = df_1d.iloc[i-2]
            
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
                elif bar['close'] > max(prev1_body_high, prev2_body_high):
                    # 條件二成立：收盤大於「前面兩根」的實體高點，且止損距離 <= 10%
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
                historical_1d = df_1d.iloc[:i+1]
                ohlcv_1d_list = historical_1d[['ts', 'open', 'high', 'low', 'close', 'vol']].values.tolist()
                composed_3d = compose_3d_bars(ohlcv_1d_list)
                
                current_sl = c2_low
                entry_ts_scan = target_info_c2['ts']
                
                for idx, b_3d in enumerate(composed_3d):
                    b_close_time = b_3d[6]
                    if b_close_time > entry_ts_scan and idx >= 1:
                        prev_b = composed_3d[idx-1]
                        prev_body_high = max(prev_b[1], prev_b[4])
                        b_close = b_3d[4]
                        b_low = b_3d[3]
                        if b_close > prev_body_high and b_low > entry_price_scan:
                            if b_low > current_sl:
                                current_sl = b_low
                
                dynamic_sl = current_sl

                if bar['low'] < dynamic_sl or bar['close'] < bar['sma_10']:
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
    
    # Fugle API 平滑並發
    semaphore = asyncio.Semaphore(10)
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

    if watchlist_items:
        send_grouped_message(watchlist_items, "🔍 台股掃描：等待突破 (關注中)")
        
    if triggered_items:
        for item in triggered_items:
            send_triggered_message(item)

    if not watchlist_items and not triggered_items:
        send_telegram_message("🔍 今日台股掃描完成：無符合條件之標的。")

if __name__ == "__main__":
    import sys
    # 提供手動測試模式，僅掃描前 20 檔股票驗證連線
    if len(sys.argv) > 1 and sys.argv[1] == "--test-mode":
        logger.info("🔧 進入測試模式，僅抓取部分標的")
        old_get = get_tw_stock_list
        def test_get():
            return old_get()[:20]
        get_tw_stock_list = test_get
        
    asyncio.run(main_loop())
