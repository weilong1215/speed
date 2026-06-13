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
import uuid
import re
import json
from flask import Flask, jsonify, render_template_string
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

# 優先讀取 API_SECRET & API_PASSWORD 格式以防大小寫解析錯誤
BITGET_API_KEY = os.getenv("BITGET_API_KEY", "")
BITGET_SECRET_KEY = os.getenv("BITGET_API_SECRET", "") or os.getenv("BITGET_SECRET_KEY", "")
BITGET_PASSWORD = os.getenv("BITGET_API_PASSWORD", "") or os.getenv("BITGET_PASSWORD", "")

# 階梯停利：每階平倉剩餘倉位的 50%
TP_LADDER = [(10, 0.5), (20, 0.5), (30, 0.5)]
TP_EXPIRE_R = 10  # 下單前/掛單中過期檢查用（首階 10R）

logger.info(f"✅ 系統配置檢查: TG_TOKEN={'已設定' if TG_BOT_TOKEN else '未設定'}, TG_CHAT_ID={'已設定' if TG_CHAT_ID else '未設定'}")
logger.info(f"✅ 交易所配置檢查: API_KEY={'已設定' if BITGET_API_KEY else '未設定'}")

# ============================================================================
# 狀態持久化 (Watchlist + Active Signals)
# ============================================================================

DATA_DIR = "/app/data"
WATCHLIST_FILE = os.path.join(DATA_DIR, "watchlist.json")
ACTIVE_SIGNALS_FILE = os.path.join(DATA_DIR, "active_signals.json")
HISTORY_SIGNALS_FILE = os.path.join(DATA_DIR, "history_signals.json")
SCANNED_COINS_FILE = os.path.join(DATA_DIR, "scanned_coins.json")
HOLDINGS_FILE = os.path.join(DATA_DIR, "holdings.json")

def ensure_data_dir():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
    if not os.path.exists(WATCHLIST_FILE):
        with open(WATCHLIST_FILE, 'w') as f:
            json.dump({}, f)
    if not os.path.exists(ACTIVE_SIGNALS_FILE):
        with open(ACTIVE_SIGNALS_FILE, 'w') as f:
            json.dump({}, f)
    if not os.path.exists(HISTORY_SIGNALS_FILE):
        with open(HISTORY_SIGNALS_FILE, 'w') as f:
            json.dump({}, f)
    if not os.path.exists(SCANNED_COINS_FILE):
        with open(SCANNED_COINS_FILE, 'w') as f:
            json.dump([], f)
    if not os.path.exists(HOLDINGS_FILE):
        with open(HOLDINGS_FILE, 'w') as f:
            json.dump([], f)

def load_watchlist():
    ensure_data_dir()
    try:
        with open(WATCHLIST_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"讀取 watchlist 失敗: {e}")
        return {}

def save_watchlist(data):
    ensure_data_dir()
    try:
        with open(WATCHLIST_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
    except Exception as e:
        logger.error(f"儲存 watchlist 失敗: {e}")

def save_scanned_coins(coins: list):
    """持久化本輪掃描的完整幣種清單，供網頁 UI 強制顯示所有名單用"""
    ensure_data_dir()
    try:
        bases = sorted(set(get_base_coin(s) for s in coins))
        with open(SCANNED_COINS_FILE, 'w', encoding='utf-8') as f:
            json.dump(bases, f, ensure_ascii=False)
    except Exception as e:
        logger.error(f"儲存 scanned_coins 失敗: {e}")

def load_scanned_coins() -> list:
    ensure_data_dir()
    try:
        with open(SCANNED_COINS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return []

def save_holdings(bases: list):
    ensure_data_dir()
    try:
        with open(HOLDINGS_FILE, 'w', encoding='utf-8') as f:
            json.dump(list(set(bases)), f, ensure_ascii=False)
    except Exception as e:
        logger.error(f"儲存 holdings 失敗: {e}")

def load_holdings() -> list:
    ensure_data_dir()
    try:
        with open(HOLDINGS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return []

def load_active_signals():
    ensure_data_dir()
    try:
        with open(ACTIVE_SIGNALS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {}

def save_active_signals(data):
    ensure_data_dir()
    try:
        with open(ACTIVE_SIGNALS_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        logger.error(f"儲存訊號失敗: {e}")

def load_history_signals():
    ensure_data_dir()
    try:
        with open(HISTORY_SIGNALS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {}

def save_history_signals(data):
    ensure_data_dir()
    try:
        with open(HISTORY_SIGNALS_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        logger.error(f"儲存歷史訊號失敗: {e}")

# ============================================================================
# 系統設定持久化
# ============================================================================

CONFIG_FILE = os.path.join(DATA_DIR, "system_config.json")

def load_config():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                config = json.load(f)
            if "blacklist" not in config:
                config["blacklist"] = ["XAUT", "PAXG", "TQQQ", "SQQQ"]
            config.setdefault("coin_rank_mode", "hot")
            config.setdefault("top_coins_count", 20)
            return config
        except Exception as e:
            logger.error(f"讀取設定檔失敗: {e}")
    return {
        "total_capital": 300,
        "loss_pct": 2,
        "blacklist": ["XAUT", "PAXG", "TQQQ", "SQQQ"],
        "coin_rank_mode": "hot",
        "top_coins_count": 20,
    }

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

def get_coid(o: dict) -> str:
    return str(o.get('clientOid') or o.get('info', {}).get('clientOid') or o.get('clientOrderId') or "")

def send_telegram_message(message: str, reply_markup: dict = None) -> bool:
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        logger.warning("Telegram 配置缺失，跳過通知。")
        return False
        
    MAX_LEN = 4000
    parts = []
    while len(message) > MAX_LEN:
        cut_idx = message.rfind('\n', 0, MAX_LEN)
        if cut_idx == -1: cut_idx = MAX_LEN
        parts.append(message[:cut_idx])
        message = message[cut_idx:].lstrip()
    parts.append(message)

    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    success = True
    for part in parts:
        payload = {"chat_id": TG_CHAT_ID, "text": part, "parse_mode": "HTML"}
        if reply_markup is not None:
            payload["reply_markup"] = reply_markup
        try:
            response = requests.post(url, json=payload, timeout=10)
            if response.status_code != 200:
                logger.error(f"Telegram 發送失敗 ({response.status_code}): {response.text}")
                success = False
        except Exception as e:
            logger.error(f"Telegram 連線異常: {e}")
            success = False
            
    return success

def get_exchange():
    exchange_config = {
        'timeout': 30000,
        'enableRateLimit': True,
        'options': {'defaultType': 'swap'}
    }
    if BITGET_API_KEY and BITGET_SECRET_KEY:
        exchange_config['apiKey'] = BITGET_API_KEY
        exchange_config['secret'] = BITGET_SECRET_KEY
        if BITGET_PASSWORD:
            exchange_config['password'] = BITGET_PASSWORD
    return ccxt.bitget(exchange_config)


def _bitget_ticker_hot_score(ticker):
    """Bitget Open API 無 App「熱門」榜端點；以 tickers 的成交額×波動加權近似 App 熱門排序。"""
    vol = float(ticker.get('usdtVolume') or ticker.get('quoteVolume') or 0)
    chg = abs(float(ticker.get('change24h') or ticker.get('changeUtc24h') or 0))
    return vol * (1 + chg * 3)


_crypto_whitelist_cache = {"timestamp": 0, "symbols": set()}

def _get_crypto_whitelist():
    """獲取純加密貨幣白名單 (過濾 RWA 傳統金融股票)，快取 1 小時"""
    now = time.time()
    if now - _crypto_whitelist_cache["timestamp"] < 3600 and _crypto_whitelist_cache["symbols"]:
        return _crypto_whitelist_cache["symbols"]
    try:
        url = 'https://api.bitget.com/api/v2/mix/market/contracts?productType=USDT-FUTURES'
        res = requests.get(url, timeout=10).json()
        if res.get('code') == '00000':
            data = res.get('data') or []
            whitelist = {x['symbol'] for x in data if str(x.get('isRwa', 'NO')).upper() == 'NO'}
            if whitelist:
                _crypto_whitelist_cache["timestamp"] = now
                _crypto_whitelist_cache["symbols"] = whitelist
                logger.info(f"✅ 已更新加密貨幣白名單，共 {len(whitelist)} 個標的 (過濾 RWA)。")
            return whitelist
    except Exception as e:
        logger.warning(f"獲取加密貨幣白名單失敗: {e}")
    
    return _crypto_whitelist_cache["symbols"]

def fetch_top_bitget_symbols(limit=20, rank_mode='volume'):
    """從 Bitget USDT 永續 tickers 取 Top N。rank_mode: volume | hot"""
    try:
        url = 'https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES'
        res = requests.get(url, timeout=10).json()
        if res.get('code') != '00000':
            logger.warning(f"拉取 Bitget tickers 失敗: {res.get('msg')}")
            return []
            
        whitelist = _get_crypto_whitelist()
        data = res.get('data') or []
        if whitelist:
            data = [x for x in data if x.get('symbol') in whitelist]
            
        if rank_mode == 'hot':
            sorted_data = sorted(data, key=_bitget_ticker_hot_score, reverse=True)
            label = '熱門 (Bitget tickers 成交額×波動)'
        else:
            sorted_data = sorted(data, key=lambda x: float(x.get('quoteVolume', 0) or 0), reverse=True)
            label = '成交額'
        symbols = [x['symbol'] for x in sorted_data[:limit]]
        logger.info(f"📊 幣種榜單 [{label}] Top {limit}: {', '.join(s.replace('USDT', '') for s in symbols)}")
        return symbols
    except Exception as e:
        logger.warning(f"拉取榜單失敗: {e}")
        return []


def compose_3d_bars(ohlcv_1d):
    """將 1D OHLCV 合成 3D K 棒 (按每年 1/1 起算)
    輸入: [[ts, open, high, low, close, vol], ...]
    輸出: 同格式的 3D K 棒列表
    """
    if not ohlcv_1d or len(ohlcv_1d) < 3:
        return []

    from datetime import datetime, timezone
    PERIOD_MS = 3 * 24 * 3600 * 1000
    groups = {}
    for bar in ohlcv_1d:
        ts = bar[0]
        dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
        year_start_dt = datetime(dt.year, 1, 1, tzinfo=timezone.utc)
        year_epoch_ms = int(year_start_dt.timestamp() * 1000)

        group_idx = (ts - year_epoch_ms) // PERIOD_MS
        group_key = year_epoch_ms + group_idx * PERIOD_MS

        if group_key not in groups:
            groups[group_key] = []
        groups[group_key].append(bar)

    result = []
    for gts in sorted(groups.keys()):
        bars = sorted(groups[gts], key=lambda x: x[0])
        result.append([
            gts,
            bars[0][1],
            max(b[2] for b in bars),
            min(b[3] for b in bars),
            bars[-1][4],
            sum(b[5] for b in bars),
            gts + PERIOD_MS
        ])
    return result




def compose_18d_bars(ohlcv_1d):
    """將 1D OHLCV 合成 18D K 棒 (按每年 1/1 起算)"""
    if not ohlcv_1d or len(ohlcv_1d) < 3:
        return []

    from datetime import datetime, timezone
    PERIOD_MS = 18 * 24 * 3600 * 1000
    groups = {}
    for bar in ohlcv_1d:
        ts = bar[0]
        dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
        year_start_dt = datetime(dt.year, 1, 1, tzinfo=timezone.utc)
        year_epoch_ms = int(year_start_dt.timestamp() * 1000)

        group_idx = (ts - year_epoch_ms) // PERIOD_MS
        group_key = year_epoch_ms + group_idx * PERIOD_MS

        if group_key not in groups:
            groups[group_key] = []
        groups[group_key].append(bar)

    result = []
    for gts in sorted(groups.keys()):
        bars = sorted(groups[gts], key=lambda x: x[0])
        result.append([
            gts,
            bars[0][1],
            max(b[2] for b in bars),
            min(b[3] for b in bars),
            bars[-1][4],
            sum(b[5] for b in bars),
            gts + PERIOD_MS
        ])
    return result

def compose_3h_bars(ohlcv_1h):
    """將 1H OHLCV 合成 3H K棒 (從每天 00:00 UTC 起算，每 3 根 1H 合一根)
    輸入: [[ts, open, high, low, close, vol], ...]
    輸出: [[ts, open, high, low, close, vol, close_ts], ...]
    """
    if not ohlcv_1h or len(ohlcv_1h) < 3:
        return []

    PERIOD_MS = 3 * 3600 * 1000   # 3 小時
    DAY_MS    = 24 * 3600 * 1000  # 1 天

    groups = {}
    for bar in ohlcv_1h:
        ts = bar[0]
        day_start = (ts // DAY_MS) * DAY_MS
        slot_idx  = (ts - day_start) // PERIOD_MS
        group_key = day_start + slot_idx * PERIOD_MS

        if group_key not in groups:
            groups[group_key] = []
        groups[group_key].append(bar)

    result = []
    for gts in sorted(groups.keys()):
        bars = sorted(groups[gts], key=lambda x: x[0])
        result.append([
            gts,
            bars[0][1],
            max(b[2] for b in bars),
            min(b[3] for b in bars),
            bars[-1][4],
            sum(b[5] for b in bars),
            gts + PERIOD_MS
        ])
    return result

# ============================================================================
# 交易執行
# ============================================================================

async def check_signal_expired(exchange, symbol, direction, entry, sl, precision, trigger_ts, expire_r=None):
    """檢查訊號是否已過期（價格已達 expire_r*R 或觸發保護止損）。下單前與掛單中撤單共用。"""
    if expire_r is None:
        expire_r = TP_EXPIRE_R
    risk_per_unit = abs(entry - sl)
    if risk_per_unit == 0:
        return False, "", ""

    # 動態計算 3D 吞噬保護止損
    dynamic_sl = sl
    if trigger_ts > 0:
        try:
            _ohlcv_1d = await exchange.fetch_ohlcv(symbol, '1d', limit=200)
            if _ohlcv_1d:
                _ohlcv_3d = compose_3d_bars(_ohlcv_1d)
                if _ohlcv_3d:
                    _df_3d = pd.DataFrame(_ohlcv_3d, columns=['ts', 'open', 'high', 'low', 'close', 'vol', 'close_ts'])
                    _now_ms = int(time.time() * 1000)
                    _closed_3d = _df_3d[_df_3d['close_ts'] <= _now_ms].reset_index(drop=True)
                    
                    if len(_closed_3d) >= 2:
                        _current_sl = sl
                        for i in range(1, len(_closed_3d)):
                            _last = _closed_3d.iloc[i]
                            _prev = _closed_3d.iloc[i-1]
                            if int(_last['close_ts']) > trigger_ts:
                                _new_close = float(_last['close'])
                                _new_high = float(_last['high'])
                                _new_low = float(_last['low'])
                                _prev_open = float(_prev['open'])
                                _prev_close = float(_prev['close'])
                                
                                if direction.upper() == 'LONG':
                                    _prev_body_high = max(_prev_open, _prev_close)
                                    if _new_close > _prev_body_high and _new_low > _current_sl:
                                        _current_sl = _new_low
                                elif direction.upper() == 'SHORT':
                                    _prev_body_low = min(_prev_open, _prev_close)
                                    if _new_close < _prev_body_low and (_new_high < _current_sl or _current_sl == 0):
                                        _current_sl = _new_high
                        dynamic_sl = _current_sl
        except Exception as e:
            logger.warning(f"  過期檢查(3D止損)異常 ({symbol}): {e}")

    if dynamic_sl != sl:
        return True, f"已產生移動保護止損 (原: {sl:.{precision}f} -> 新: {dynamic_sl:.{precision}f})", 'PSL'

    tp_price = entry + expire_r * risk_per_unit if direction == 'LONG' else entry - expire_r * risk_per_unit
    l3_close_ts = trigger_ts + 3 * 3600 * 1000 if trigger_ts > 0 else int(time.time() * 1000)
    since_ts = l3_close_ts - 5 * 60 * 1000

    try:
        ohlcv_1h = await exchange.fetch_ohlcv(symbol, '1h', since=since_ts, limit=500)
        for candle in ohlcv_1h:
            c_ts = int(candle[0])
            c_high = float(candle[2])
            c_low = float(candle[3])
            if c_ts < l3_close_ts - 60000:
                continue
            dt_taiwan = pd.to_datetime(c_ts, unit='ms', utc=True).tz_convert('Asia/Taipei').strftime('%Y-%m-%d %H:%M')
            if direction == 'LONG':
                if c_high >= tp_price:
                    return True, f"歷史 1H K 棒 ({dt_taiwan}) 最高價 {c_high:.{precision}f} 已達到/超過 {expire_r}R 停利點 ({tp_price:.{precision}f})", 'TP'
                if c_low <= sl:
                    return True, f"歷史 1H K 棒 ({dt_taiwan}) 最低價 {c_low:.{precision}f} 已觸發初始止損 ({sl:.{precision}f})", 'PSL'
            else:
                if c_low <= tp_price:
                    return True, f"歷史 1H K 棒 ({dt_taiwan}) 最低價 {c_low:.{precision}f} 已達到/低於 {expire_r}R 停利點 ({tp_price:.{precision}f})", 'TP'
                if c_high >= sl:
                    return True, f"歷史 1H K 棒 ({dt_taiwan}) 最高價 {c_high:.{precision}f} 已觸發初始止損 ({sl:.{precision}f})", 'PSL'

        ticker = await exchange.fetch_ticker(symbol)
        current_price = float(ticker['last'])
        if direction == 'LONG':
            if current_price >= tp_price:
                return True, f"最新市價 {current_price:.{precision}f} 已達到/超過 {expire_r}R 停利點 ({tp_price:.{precision}f})", 'TP'
            if current_price <= sl:
                return True, f"最新市價 {current_price:.{precision}f} 已觸發初始止損 ({sl:.{precision}f})", 'PSL'
        else:
            if current_price <= tp_price:
                return True, f"最新市價 {current_price:.{precision}f} 已達到/低於 {expire_r}R 停利點 ({tp_price:.{precision}f})", 'TP'
            if current_price >= sl:
                return True, f"最新市價 {current_price:.{precision}f} 已觸發初始止損 ({sl:.{precision}f})", 'PSL'
    except Exception as e:
        logger.warning(f"  過期檢查異常 ({symbol}): {e}")
    return False, "", ""


async def place_order(exchange, symbol, direction, entry, sl, precision, fixed_loss_usdt, trigger_ts,
                      l2_high=0.0, l2_low=0.0, l2_open_ts=0, l1_18d_direction='', l2_date='', l3_date=''):
    """
    執行下單：Limit Order + 分層槓桿策略 (MAX → 20x → 10x)

    降層觸發條件:
    - MAX 策略: 使用幣種實際最大槓桿。set_leverage 失敗代表交易所資料與 API 不一致，降層。
    - 下單本身失敗 (如倉位超限) 才是觸發 20x / 10x 的主要條件。
    """
    if not BITGET_API_KEY: return None
    try:
        risk_per_unit = abs(entry - sl)
        if risk_per_unit == 0: return None
        qty_risk_ideal = fixed_loss_usdt / risk_per_unit

        expired, skip_reason, alert_type = await check_signal_expired(
            exchange, symbol, direction, entry, sl, precision, trigger_ts)
        if expired:
            tp_price = entry + TP_EXPIRE_R * risk_per_unit if direction == 'LONG' else entry - TP_EXPIRE_R * risk_per_unit
            logger.warning(f"⚠️ {symbol} 下單前監測觸發過期，跳過下單: {skip_reason}")
            if alert_type == 'TP':
                title = f"<b>🚫 跳過自動下單 (已達{TP_EXPIRE_R}R停利)</b>"
            elif alert_type == 'PSL':
                title = "<b>🚫 跳過自動下單 (已觸發保護止損)</b>"
            else:
                title = "<b>🚫 跳過自動下單</b>"
            send_telegram_message(
                f"{title}\n\n"
                f"💎 <b>交易對:</b> {get_base_coin(symbol)} [{direction}]\n"
                f"🎯 進場價格: <code>{entry:.{precision}f}</code>\n"
                f"🛡️ 保護止損: <code>{sl:.{precision}f}</code>\n"
                f"🎯 {TP_EXPIRE_R}R價格: <code>{tp_price:.{precision}f}</code>\n\n"
                f"⚠️ <b>原因:</b> {skip_reason}"
            )
            return 'skipped'

        # 1. 建構槓桿策略列表: MAX (幣種實際最大槓桿) → 20x → 10x
        # 應對 Bitget V2 API 欄位異動，同步檢查 maxLever、maxLeverage 與 limits 結構
        leverage_strategies = []
        try:
            markets = await exchange.load_markets()
            market = markets.get(symbol, {})
            info = market.get('info', {})

            max_lev = int(info.get('maxLever', info.get('maxLeverage', 0)))
            if max_lev == 0:
                max_lev = int(market.get('limits', {}).get('leverage', {}).get('max', 0) or 0)

            # 若無從取得則維持保底 20x，防止幣種資料異常時越界嘗試
            leverage_strategies.append(('MAX', max_lev if max_lev > 0 else 20))
        except Exception as e:
            logger.warning(f"  策略 MAX 獲取最大槓桿失敗: {e}，降級使用 20x")
            leverage_strategies.append(('MAX', 20))

        leverage_strategies.append(('STABLE', 20))
        leverage_strategies.append(('FINAL', 10))

        # 倉位超限/槓桿超限相關的 Bitget 錯誤碼，觸發這類錯誤時降層重試
        ORDER_RETRYABLE_CODES = ("40762", "40797", "45110", "200029")

        # 2. 執行分層下單嘗試
        # 核心保證：槓桿設定成功後才下單，任何一層策略失敗都不跳過降層機制
        last_error = None

        for strategy_name, leverage in leverage_strategies:
            try:
                # A. 倉位模式與保證金模式：失敗可容忍 (可能已是正確狀態)
                try:
                    await exchange.set_position_mode(True, symbol)
                    await exchange.set_margin_mode('cross', symbol)
                except Exception as e:
                    logger.debug(f"  策略 {strategy_name} 倉位/保證金模式設定略過: {e}")

                # B. 槓桿設定：失敗視同策略失敗，立即降層，禁止以未知槓桿下單
                try:
                    await exchange.set_leverage(leverage, symbol)
                except Exception as e:
                    err_str = str(e)
                    logger.warning(f"  策略 {strategy_name} ({leverage}x) set_leverage 失敗 (API 資料不一致): {err_str}")
                    last_error = err_str
                    # 槓桿超限類錯誤 → 降層重試；其他不可預期錯誤 → 直接終止
                    if any(code in err_str for code in ORDER_RETRYABLE_CODES) or "leverage" in err_str.lower():
                        continue
                    else:
                        logger.error(f"  策略 {strategy_name} 槓桿設定遭遇不可恢復錯誤，終止下單流程。")
                        break

                balance = await exchange.fetch_balance()
                # 優先使用全倉可用保證金 (含未實現盈虧)，fallback 回錢包可用餘額
                available = 0.0
                try:
                    raw_info = balance.get('info', {})
                    if isinstance(raw_info, dict):
                        logger.debug(f"  balance info keys: {list(raw_info.keys())}")
                        info_data = raw_info.get('data', [])
                    elif isinstance(raw_info, list):
                        info_data = raw_info
                    else:
                        info_data = []

                    search_list = info_data if isinstance(info_data, list) else [info_data] if isinstance(info_data, dict) else []
                    for acct in search_list:
                        if not isinstance(acct, dict):
                            continue
                        if acct.get('marginCoin', '').upper() == 'USDT':
                            available = float(acct.get('crossedMaxAvailable', 0))
                            logger.info(f"  全倉可用保證金 (crossedMaxAvailable): {available:.2f} USDT")
                            break

                    if available <= 0:
                        usdt_info = balance.get('USDT', {}).get('info', {})
                        if isinstance(usdt_info, dict):
                            available = float(usdt_info.get('crossedMaxAvailable', 0))

                except Exception as e:
                    logger.warning(f"  解析全倉可用保證金異常: {e}")
                if available <= 0:
                    available = float(balance.get('USDT', {}).get('free', 0))
                    logger.info(f"  Fallback 使用錢包可用餘額: {available:.2f} USDT")
                max_q = (available * 0.9) * leverage / entry
                qty = min(qty_risk_ideal, max_q)
                qty = float(exchange.amount_to_precision(symbol, qty))

                if qty * entry < 6:
                    logger.warning(f"  策略 {strategy_name} 價值不足 6 USDT，跳過")
                    continue

                actual_risk = qty * risk_per_unit
                # 0.75 容忍精度截斷：qty 為交易所最小精度取整，允許最多 25% 曝險縮水
                if actual_risk < fixed_loss_usdt * 0.75:
                    msg = f"⚠️ 資金不足以建立標準部位 ({actual_risk:.2f}/{fixed_loss_usdt})\n可用餘額: {available:.2f} USDT"
                    logger.warning(msg)
                    send_telegram_message(f"<b>⚠️ 資金不足</b>\n{get_base_coin(symbol)}\n可用: {available:.2f} USDT")
                    break

                side = 'buy' if direction == 'LONG' else 'sell'
                signal_id = f"entry_{uuid.uuid4().hex[:8]}"
                params = {
                    'hedged': True, 'holdSide': 'long' if direction == 'LONG' else 'short',
                    'clientOid': signal_id, 'stopLoss': {'triggerPrice': sl, 'type': 'market'}
                }

                try:
                    order = await exchange.create_order(symbol, 'limit', side, qty, entry, params=params)
                except Exception as e:
                    # 如果限價單失敗，檢查是否在「開倉價格與止損價格」之間，若是則改用市價單
                    try:
                        ticker = await exchange.fetch_ticker(symbol)
                        current_p = float(ticker['last'])
                        is_within_range = False
                        if side == 'buy' and sl < current_p < entry:
                            is_within_range = True
                        elif side == 'sell' and sl > current_p > entry:
                            is_within_range = True
                            
                        if is_within_range:
                            logger.warning(f"  限價單 {entry} 遭拒絕 ({e})，但市價 {current_p} 在進場與止損區間內，改以市價單進場！")
                            order = await exchange.create_order(symbol, 'market', side, qty, None, params=params)
                            entry = current_p  # 更新 entry 為當前市價，確保後續紀錄與停利計算正確
                        else:
                            raise e 
                    except Exception as inner_e:
                        raise inner_e 

                logger.info(f"✅ 下單成功: {symbol} @ {entry} (ID: {signal_id}, Strat: {strategy_name}, Lev: {leverage}x)")

                signals = load_active_signals()
                key = f"{get_base_coin(symbol)}_{direction}"
                if key not in signals:
                    signals[key] = []
                signals[key].append({
                    'signal_id': signal_id, 'symbol': symbol, 'side': side, 'direction': direction,
                    'quantity': qty, 'entry_price': entry, 'sl_price': sl,
                    'original_sl_price': sl,
                    'status': 'active', 'precision': precision, 'tp_stage': 0,
                    'l2_high': l2_high, 'l2_low': l2_low, 'l2_open_ts': l2_open_ts,
                    'l1_18d_direction': l1_18d_direction, 'l2_date': l2_date, 'l3_date': l3_date,
                    'timestamp': trigger_ts if trigger_ts > 0 else int(time.time() * 1000)
                })
                save_active_signals(signals)
                l3_display = l3_date or (pd.to_datetime(trigger_ts, unit='ms', utc=True).tz_convert('Asia/Taipei').strftime('%Y-%m-%d %H:%M:%S') if trigger_ts > 0 else '未知')
                dir_str = "🟢 LONG" if direction == "LONG" else "🔴 SHORT" if direction == "SHORT" else direction
                send_telegram_message(
                    f"<b>🤖 自動下單 ({leverage}x)</b>\n\n"
                    f"💎 {get_base_coin(symbol)} [{dir_str}]\n"
                    f"📅 L3 (3H): <code>{l3_display}</code>\n"
                    f"🎯 進場: <code>{entry:.{precision}f}</code>\n"
                    f"🛡️ 保護止損: <code>{sl:.{precision}f}</code>"
                )
                return order

            except Exception as e:
                last_error = str(e)
                if any(code in last_error for code in ORDER_RETRYABLE_CODES) or "leverage" in last_error.lower():
                    logger.warning(f"  策略 {strategy_name} ({leverage}x) 下單失敗 (倉位/槓桿超限)，降層至下一策略...")
                    continue
                else:
                    logger.error(f"  策略 {strategy_name} 觸發不可恢復下單錯誤: {last_error}")
                    break

        err_msg = last_error or "未知錯誤"
        
        friendly_reason = err_msg
        if "22047" in err_msg:
            friendly_reason = "掛單價格偏離當前市價過大，觸發交易所限價保護"
        elif "45110" in err_msg or "200029" in err_msg or "insufficient" in err_msg.lower() or "balance" in err_msg.lower():
            friendly_reason = "帳戶可用保證金不足以支付此單"
        elif "40762" in err_msg:
            friendly_reason = "下單數量或總倉位價值超過交易所上限"
        elif "40797" in err_msg:
            friendly_reason = "倉位模式 (單向/雙向) 或保證金模式衝突"
        elif "leverage" in err_msg.lower():
            friendly_reason = "該幣種不支援設定的槓桿倍數"
        else:
            import re
            match = re.search(r'"msg"\s*:\s*"([^"]+)"', err_msg)
            if match:
                friendly_reason = f"交易所拒絕: {match.group(1)}"

        logger.error(f"❌ 所有槓桿策略均已失效 ({symbol}), 最後錯誤: {err_msg}")
        send_telegram_message(
            f"<b>❌ 自動下單失敗</b>\n\n"
            f"💎 <b>交易對:</b> {get_base_coin(symbol)} [{direction}]\n"
            f"⚠️ <b>原因:</b> {friendly_reason}"
        )
        return None
    except Exception as e:
        logger.error(f"下單執行異常 ({symbol}): {e}")
        return None

# ============================================================================
# 計畫委託歷史與停利管理
# ============================================================================

async def _query_plan_order_status(exchange, symbol, client_oid):
    """查詢 Bitget 歷史計畫委託單狀態 (executed / canceled / not_found / error)。
    使用 V2 API: GET /api/v2/mix/order/orders-plan-history
    """
    try:
        product_type = 'USDT-FUTURES'
        response = await exchange.privateMixGetV2MixOrderOrdersPlanHistory({
            'symbol': symbol.replace('/', '').replace(':USDT', ''),
            'productType': product_type,
            'planType': 'normal_plan',
            'clientOid': client_oid,
        })
        data = response.get('data', {})
        entries = data.get('entrustedList', [])
        if not entries:
            return 'not_found', 0.0
        entry = entries[0]
        status = str(entry.get('planStatus', '')).lower()
        base_vol = float(entry.get('baseVolume', 0) or 0)
        if status in ('executed', 'live_executed'):
            return 'executed', base_vol
        elif 'cancel' in status:
            return 'canceled', 0.0
        else:
            return status, base_vol
    except Exception as e:
        logger.warning(f"查詢計畫委託歷史失敗 ({client_oid}): {e}")
        return 'error', 0.0


async def manage_tp_ladder(exchange, symbol, side, sig, size, saved_signals, open_orders):
    """階梯停利：10R/20R/30R 各平倉剩餘倉位 50%。"""
    try:
        tp_stage = sig.get('tp_stage', 0)
        if tp_stage >= len(TP_LADDER):
            return

        signal_id = sig.get('signal_id', str(sig.get('timestamp')))
        entry = sig['entry_price']
        original_sl = sig.get('original_sl_price', sig['sl_price'])
        risk = abs(entry - original_sl)
        if risk == 0:
            return
        direction = sig['direction']
        precision = sig.get('precision', 4)

        r_mult, close_pct = TP_LADDER[tp_stage]
        tp_coid = f"tp{int(r_mult)}_{signal_id}"
        stored_tp_order_id = sig.get('tp_order_id', '')

        if stored_tp_order_id:
            has_order = any(tp_coid in get_coid(o) for o in open_orders)
            if has_order:
                tp_order_obj = next((o for o in open_orders if tp_coid in get_coid(o)), None)
                if tp_order_obj:
                    existing_qty = float(tp_order_obj.get('amount', 0) or tp_order_obj.get('info', {}).get('size', 0) or 0)
                    ideal_qty = float(exchange.amount_to_precision(symbol, size * close_pct))
                    if existing_qty > 0 and ideal_qty > 0 and abs(existing_qty - ideal_qty) / ideal_qty > 0.02:
                        chk_tp_price = entry + r_mult * risk if direction == 'LONG' else entry - r_mult * risk
                        chk_tp_price = round(chk_tp_price, precision)
                        full_qty = float(exchange.amount_to_precision(symbol, size))
                        if ideal_qty * chk_tp_price < 6 and full_qty > 0 and abs(existing_qty - full_qty) / full_qty <= 0.02:
                            return
                        logger.info(f"🔄 TP{r_mult}R 數量不一致 ({symbol}): 掛單={existing_qty} vs 理想={ideal_qty}，撤舊掛新")
                        try:
                            await exchange.cancel_order(tp_order_obj['id'], symbol, params={'stop': True})
                        except Exception as e:
                            if "43001" not in str(e) and "does not exist" not in str(e).lower():
                                logger.error(f"撤銷舊 TP 失敗: {e}")
                                return
                        sig['tp_order_id'] = ''
                        save_active_signals(saved_signals)
                    else:
                        return
            else:
                plan_status, base_vol = await _query_plan_order_status(exchange, symbol, tp_coid)
                if plan_status == 'executed':
                    sig['tp_order_id'] = ''
                    sig['tp_stage'] = tp_stage + 1
                    save_active_signals(saved_signals)
                    logger.info(f"🎯 TP{r_mult}R 確認成交 (成交量: {base_vol})，進入下一階")
                    send_telegram_message(
                        f"<b>🎯 TP{r_mult}R 止盈成交</b>\n\n"
                        f"💎 {get_base_coin(symbol)} [{direction}]\n"
                        f"📊 <b>平倉比例:</b> {int(close_pct * 100)}% 剩餘倉位"
                    )
                elif plan_status == 'canceled':
                    logger.info(f"⚠️ TP{r_mult}R 被撤銷 ({symbol})，將自動補掛")
                    sig['tp_order_id'] = ''
                    save_active_signals(saved_signals)
                elif plan_status == 'error':
                    logger.warning(f"  TP{r_mult}R 歷史查詢 API 錯誤，保留追蹤等下輪重試")
                    return
                else:
                    logger.info(f"  TP{r_mult}R 歷史查無此單 ({symbol})，清空追蹤準備補掛")
                    sig['tp_order_id'] = ''
                    save_active_signals(saved_signals)

        if any(tp_coid in get_coid(o) for o in open_orders):
            tp_obj = next((o for o in open_orders if tp_coid in get_coid(o)), None)
            if tp_obj:
                sig['tp_order_id'] = str(tp_obj.get('id', tp_coid))
                save_active_signals(saved_signals)
            return

        tp_price = entry + r_mult * risk if direction == 'LONG' else entry - r_mult * risk
        tp_price = max(10 ** -precision, tp_price)
        tp_price = round(tp_price, precision)
        tp_qty = float(exchange.amount_to_precision(symbol, size * close_pct))

        if tp_qty <= 0 or tp_qty * tp_price < 6:
            full_qty = float(exchange.amount_to_precision(symbol, size))
            if full_qty > 0 and full_qty * tp_price >= 6:
                tp_qty = full_qty
                logger.warning(f"  TP{r_mult}R 份額價值不足 6U ({tp_qty * tp_price:.2f}U)，改為全倉平倉: {tp_qty}")
            else:
                logger.warning(f"  TP{r_mult}R 全倉價值仍不足 6U ({full_qty * tp_price:.2f}U)，停止掛單 (粉塵由 SL 保護)")
                return

        order_side = 'sell' if direction == 'LONG' else 'buy'
        tp_params = {
            'triggerPrice': tp_price, 'triggerType': 'fill_price', 'reduceOnly': True,
            'hedged': True, 'holdSide': 'long' if direction == 'LONG' else 'short',
            'clientOid': tp_coid
        }

        logger.info(f"🚀 掛出 TP{r_mult}R: {symbol} @ {tp_price:.{precision}f} | qty: {tp_qty} | ID: {tp_coid}")
        try:
            result = await exchange.create_order(symbol, 'market', order_side, tp_qty, None, params=tp_params)
            sig['tp_order_id'] = str(result.get('id', ''))
        except Exception as e:
            if '40786' in str(e):
                logger.warning(f"⚠️ TP ID 已存在 ({tp_coid})，視為已掛單。")
                sig['tp_order_id'] = tp_coid
            else:
                raise e
        save_active_signals(saved_signals)
    except Exception as e:
        logger.error(f"掛 TP 失敗: {e}")

# ============================================================================
# 倉位監控
# ============================================================================

async def monitor_positions(exchange):
    """
    倉位監控：1D 移動止損 + 階梯 TP + SL 補掛 + 掛單過期撤單 + 孤兒單清理
    """
    try:
        await exchange.load_markets()
        positions = await exchange.fetch_positions()
        active_pos = [p for p in positions if float(p.get('contracts', 0) or p.get('size', 0)) > 0]

        open_orders = []
        try:
            orders_normal = await exchange.fetch_open_orders()
            orders_plan = await exchange.fetch_open_orders(params={'stop': True})
            open_orders = orders_normal + orders_plan
        except Exception as e:
            logger.warning(f"取得掛單失敗: {e}")

        saved_signals = load_active_signals()

        pos_symbols = [p['symbol'] for p in active_pos]
        save_holdings([get_base_coin(s) for s in pos_symbols])
        logger.info(f"--- 倉位監控檢查 | 交易所持倉: {len(active_pos)} 個 ({', '.join(pos_symbols) if pos_symbols else '無'}) ---")

        # 1. 監控已成交倉位，確保 TP 與 SL 掛單存在
        for pos in active_pos:
            symbol = pos['symbol']
            side = pos['side']
            size = float(pos.get('contracts', 0) or pos.get('size', 0))
            name = get_base_coin(symbol)

            found_signal = False
            for sig_key, sig_list in list(saved_signals.items()):
                expected_key = f"{name}_{side.upper()}"
                if sig_key != expected_key:
                    continue
                for sig in sig_list:
                    if sig['status'] != 'active' or sig['direction'].lower() != side.lower():
                        continue

                    found_signal = True
                    signal_id = sig.get('signal_id', str(sig.get('timestamp')))

                    my_sl_orders = []
                    sl_price_target = float(sig['sl_price'])

                    for o in open_orders:
                        cid = get_coid(o)
                        trig_p = float(o.get('triggerPrice', 0) or o.get('info', {}).get('triggerPrice') or 0)
                        if signal_id in cid:
                            if "sl_" in cid:
                                my_sl_orders.append(o)
                        else:
                            is_sl_side = (side.lower() == 'long' and o['side'].lower() == 'sell') or \
                                         (side.lower() == 'short' and o['side'].lower() == 'buy')
                            if is_sl_side and abs(trig_p - sl_price_target) < 1e-8:
                                my_sl_orders.append(o)

                    if len(my_sl_orders) > 1:
                        for dup in my_sl_orders[1:]:
                            await exchange.cancel_order(dup['id'], symbol, params={'stop': True})

                    if len(my_sl_orders) == 1:
                        existing_trig = float(my_sl_orders[0].get('triggerPrice', 0) or my_sl_orders[0].get('info', {}).get('triggerPrice') or 0)
                        sl_order_qty = float(my_sl_orders[0].get('amount', 0) or my_sl_orders[0].get('info', {}).get('size', 0) or 0)
                        need_cancel = False

                        if abs(existing_trig - sl_price_target) > 1e-8:
                            logger.info(f"🔄 SL 價格不一致 ({symbol}): 掛單={existing_trig} vs 目標={sl_price_target}，撤銷舊單")
                            need_cancel = True

                        if not need_cancel and sl_order_qty > 0 and abs(sl_order_qty - size) / size > 0.02:
                            logger.info(f"🔄 SL 數量不一致 ({symbol}): 掛單={sl_order_qty} vs 倉位={size}，撤銷舊單")
                            need_cancel = True

                        if need_cancel:
                            try:
                                await exchange.cancel_order(my_sl_orders[0]['id'], symbol, params={'stop': True})
                            except Exception as e:
                                if "43001" not in str(e) and "does not exist" not in str(e).lower():
                                    logger.error(f"撤銷舊 SL 失敗: {e}")
                            my_sl_orders = []

                    if not my_sl_orders:
                        logger.info(f"🛡️ 補掛止損單: {symbol} @ {sl_price_target}")
                        try:
                            order_side = 'sell' if side.lower() == 'long' else 'buy'
                            sl_params = {
                                'triggerPrice': sl_price_target, 'triggerType': 'fill_price', 'reduceOnly': True,
                                'hedged': True, 'holdSide': 'long' if order_side == 'sell' else 'short',
                                'clientOid': f"sl_{signal_id}"
                            }
                            await exchange.create_order(symbol, 'market', order_side, size, None, params=sl_params)
                        except Exception as e:
                            if '40786' not in str(e):
                                logger.error(f"掛止損失敗: {e}")

                    # 3D 移動止損：最新 3D 棒吞噬前棒實體 → 止損移至該棒低/高點
                    _entry_ts = int(sig.get('timestamp', 0))
                    if _entry_ts > 0:
                        try:
                            _ohlcv_1d_mon = await exchange.fetch_ohlcv(symbol, '1d', limit=200)
                            if _ohlcv_1d_mon:
                                _ohlcv_3d_mon = compose_3d_bars(_ohlcv_1d_mon)
                                if _ohlcv_3d_mon:
                                    _df_3d_mon = pd.DataFrame(_ohlcv_3d_mon, columns=['ts', 'open', 'high', 'low', 'close', 'vol', 'close_ts'])
                                    _now_ms_mon = int(time.time() * 1000)
                                    _closed_3d_pos = _df_3d_mon[_df_3d_mon['close_ts'] <= _now_ms_mon].reset_index(drop=True)

                                    if len(_closed_3d_pos) >= 2:
                                        _last_3d = _closed_3d_pos.iloc[-1]
                                        _prev_3d = _closed_3d_pos.iloc[-2]
                                        _last_3d_close_ts = int(_last_3d['close_ts'])

                                        if _last_3d_close_ts > _entry_ts:
                                            _new_close = float(_last_3d['close'])
                                            _new_high = float(_last_3d['high'])
                                            _new_low = float(_last_3d['low'])
                                            _prev_open = float(_prev_3d['open'])
                                            _prev_close = float(_prev_3d['close'])
                                            _current_sl = float(sig['sl_price'])
                                            _new_sl = _current_sl
                                            _move_sl = False

                                            if side.lower() == 'long':
                                                _prev_body_high = max(_prev_open, _prev_close)
                                                if _new_close > _prev_body_high and _new_low > _current_sl:
                                                    _new_sl = _new_low
                                                    _move_sl = True
                                            elif side.lower() == 'short':
                                                _prev_body_low = min(_prev_open, _prev_close)
                                                if _new_close < _prev_body_low and (_new_high < _current_sl or _current_sl == 0):
                                                    _new_sl = _new_high
                                                    _move_sl = True

                                            if _move_sl:
                                                _3d_dt = pd.to_datetime(int(_last_3d['ts']), unit='ms', utc=True).tz_convert('Asia/Taipei').strftime('%Y-%m-%d')
                                                logger.info(f"🔄 3D 吞噬 ({symbol} {_3d_dt})，止損移至 {_new_sl}")
                                                sig['sl_price'] = _new_sl
                                                save_active_signals(saved_signals)
                                                history_signals = load_history_signals()
                                                base_coin = get_base_coin(symbol)
                                                if base_coin in history_signals:
                                                    for hs in history_signals[base_coin]:
                                                        if hs.get('trigger_ts') == _entry_ts:
                                                            hs['stop_loss'] = _new_sl
                                                save_history_signals(history_signals)
                                                send_telegram_message(
                                                    f"<b>🔄 3D 移動止損觸發</b>\n\n"
                                                    f"💎 <b>交易對:</b> {get_base_coin(symbol)} [{side.upper()}]\n"
                                                    f"📅 <b>觸發 K 線:</b> {_3d_dt}\n"
                                                    f"🛡️ <b>新止損價:</b> <code>{_new_sl:.4f}</code>"
                                                )
                        except Exception as _e3d:
                            logger.warning(f"3D 移動止損監控異常 ({symbol}): {_e3d}")

                    await manage_tp_ladder(exchange, symbol, side, sig, size, saved_signals, open_orders)
        # 2. 孤兒訊號清理 (倉位消失但訊號仍 active)
        for sig_key, sig_list in list(saved_signals.items()):
            for sig in sig_list:
                if sig['status'] != 'active':
                    continue
                signal_id = sig['signal_id']
                symbol = sig['symbol']
                direction = sig['direction']
                has_pos = any(p['symbol'] == symbol and p['side'].upper() == direction for p in active_pos)

                has_entry = any(str(signal_id) == get_coid(o) for o in open_orders)
                
                if has_pos:
                    sig['has_entered'] = True

                if has_entry:
                    entry_price = float(sig.get('entry_price', 0))
                    sl_price = float(sig['sl_price'])
                    trigger_ts = int(sig.get('timestamp', 0))
                    prec = int(sig.get('precision', 4))
                    expired, revoke_reason, alert_type = await check_signal_expired(
                        exchange, symbol, direction, entry_price, sl_price, prec, trigger_ts)
                    if expired:
                        logger.info(f"🚫 掛單過期 [{revoke_reason}] ({symbol})，撤銷未成交單 {signal_id}")
                        entry_orders = [o for o in open_orders if str(signal_id) == get_coid(o)]
                        for eo in entry_orders:
                            try:
                                await exchange.cancel_order(eo['id'], symbol)
                                open_orders = [o for o in open_orders if o['id'] != eo['id']]
                            except Exception as e:
                                logger.warning(f"撤銷未成交單失敗 {eo['id']}: {e}")
                                
                        # 連帶強制撤銷可能殘留的原始附加計畫止損單
                        for po in orders_plan:
                            if po['symbol'] == symbol:
                                _is_opp = (direction.upper() == 'LONG' and po['side'].lower() == 'sell') or \
                                          (direction.upper() == 'SHORT' and po['side'].lower() == 'buy')
                                _trig_p = float(po.get('triggerPrice', 0) or po.get('info', {}).get('triggerPrice', 0))
                                if _is_opp and abs(_trig_p - sl_price) < 1e-8:
                                    try:
                                        await exchange.cancel_order(po['id'], symbol, params={'stop': True})
                                        logger.info(f"🚫 連帶撤銷掛單中的附屬止損: {po['id']}")
                                    except Exception:
                                        pass
                                        
                        if alert_type == 'TP':
                            title = f"已達{TP_EXPIRE_R}R停利"
                        elif alert_type == 'PSL':
                            title = "已觸發保護止損"
                        else:
                            title = "訊號已過期"
                        send_telegram_message(
                            f"<b>🚫 訊號已淘汰 (撤銷進場)</b>\n\n"
                            f"💎 <b>交易對:</b> {get_base_coin(symbol)} [{direction}]\n"
                            f"📉 <b>狀態: {title} — {revoke_reason}，已自動撤銷進場單</b>"
                        )
                        sig['status'] = 'closed'
                        continue

                if not has_pos and not has_entry:
                    logger.info(f"🧹 偵測到歸零/孤兒訊號 {sig_key}，開始清理...")

                    orphan_orders = [o for o in open_orders if str(signal_id) in get_coid(o)]
                    for oo in orphan_orders:
                        try:
                            cid_str = get_coid(oo)
                            is_plan = cid_str.startswith("sl_") or cid_str.startswith("tp")
                            await exchange.cancel_order(oo['id'], symbol, params={'stop': True} if is_plan else {})
                            logger.info(f"🚫 已撤銷殘留單: {oo['id']} (Stop: {is_plan})")
                            open_orders = [o for o in open_orders if o['id'] != oo['id']]
                        except Exception as e:
                            if "43001" not in str(e) and "does not exist" not in str(e).lower():
                                logger.warning(f"撤銷殘留單失敗 {oo['id']}: {e}")

                    msg = (f"<b>🏁 倉位已下車</b>\n\n"
                           f"💎 <b>交易對:</b> {get_base_coin(symbol)}\n"
                           f"📉 <b>當前狀態: 倉位已清結</b>")
                    send_telegram_message(msg)

                    sig['status'] = 'closed'

            # 更新歷史紀錄
            closed_sigs = [s for s in sig_list if s['status'] == 'closed']
            if closed_sigs:
                history_signals = load_history_signals()
                if sig_key not in history_signals:
                    history_signals[sig_key] = []
                for closed_sig in closed_sigs:
                    for h_sig in history_signals[sig_key]:
                        if h_sig.get('signal_id') == closed_sig.get('signal_id'):
                            h_sig['status'] = 'closed'
                save_history_signals(history_signals)

            saved_signals[sig_key] = [s for s in sig_list if s['status'] == 'active']
            if not saved_signals[sig_key]:
                del saved_signals[sig_key]

        # 3. 盲目孤兒單清理 (訂單找名單)
        # Entry: entry_*, TP: tp10_/tp20_/tp30_*, SL: sl_*
        tp_prefix_pattern = re.compile(r'^tp\d+_')
        all_active_ids = [str(s['signal_id']) for slist in saved_signals.values() for s in slist]
        for oo in open_orders:
            co_id = get_coid(oo)
            is_sl = co_id.startswith("sl_")
            is_tp = bool(tp_prefix_pattern.match(co_id))
            is_entry = co_id.startswith("entry_")
            if is_sl or is_tp or is_entry:
                if is_sl:
                    raw_sig_id = co_id[3:]
                elif is_tp:
                    raw_sig_id = tp_prefix_pattern.sub('', co_id)
                else:
                    raw_sig_id = co_id
                if raw_sig_id not in all_active_ids:
                    symbol = oo['symbol']
                    target_direction = "SHORT" if oo['side'].lower() == "buy" else "LONG"
                    has_related_pos = any(p['symbol'] == symbol and p['side'].upper() == target_direction
                                         for p in active_pos)
                    if not has_related_pos:
                        logger.warning(f"🕵️ 發現無主孤兒單: {co_id} ({symbol})，執行盲目清理...")
                        try:
                            is_plan = not is_entry
                            await exchange.cancel_order(oo['id'], symbol, params={'stop': True} if is_plan else {})
                            open_orders = [o for o in open_orders if o['id'] != oo['id']]
                        except IndexError:
                            logger.info(f"孤兒單 {co_id} 已自行消失 (API 空回應)，視為安全。")
                        except Exception as e:
                            if "43001" not in str(e) and "does not exist" not in str(e).lower():
                                logger.error(f"盲目清理失敗 {co_id}: {e}")

        save_active_signals(saved_signals)
    except Exception as e:
        logger.error(f"監控循環異常: {e}")

# ============================================================================
# 掃描模組
# ============================================================================

async def scan_for_symbol(exchange, symbol, name, precision, current_idx=0, total_coins=0, cached_info=None):
    try:
        if current_idx > 0 and (current_idx % 50 == 0 or current_idx == total_coins):
            logger.info(f"📊 掃描進度: {current_idx}/{total_coins}...")

        now_utc = int(time.time() * 1000)

        # 拉取 1D 與 1H 資料 (改拉 1000 根以提供足夠的 18D K棒)
        ohlcv_1d = await exchange.fetch_ohlcv(symbol, '1d', limit=1000)
        if not ohlcv_1d or len(ohlcv_1d) < 18:
            logger.info(f"🔎 {symbol} 略過: 1D K線不足 ({len(ohlcv_1d) if ohlcv_1d else 0} 根，需≥18)")
            return None

        ohlcv_1h = await exchange.fetch_ohlcv(symbol, '1h', limit=1000)
        if not ohlcv_1h or len(ohlcv_1h) < 100:
            logger.info(f"🔎 {symbol} 略過: 1H K線不足 ({len(ohlcv_1h) if ohlcv_1h else 0} 根，需≥100)")
            return None

        ohlcv_18d = compose_18d_bars(ohlcv_1d)
        ohlcv_3d = compose_3d_bars(ohlcv_1d)
        ohlcv_3h = compose_3h_bars(ohlcv_1h)
        if not ohlcv_18d or not ohlcv_3d or not ohlcv_3h:
            logger.info(f"🔎 {symbol} 略過: 18D/3D/3H 合成失敗")
            return None

        df_18d = pd.DataFrame(ohlcv_18d, columns=['ts', 'open', 'high', 'low', 'close', 'vol', 'close_ts'])
        df_18d_closed = df_18d[df_18d['close_ts'] <= now_utc].reset_index(drop=True)

        df_3d = pd.DataFrame(ohlcv_3d, columns=['ts', 'open', 'high', 'low', 'close', 'vol', 'close_ts'])
        df_3d_closed = df_3d[df_3d['close_ts'] <= now_utc].reset_index(drop=True)

        df_1d = pd.DataFrame(ohlcv_1d, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])

        df_3h = pd.DataFrame(ohlcv_3h, columns=['ts', 'open', 'high', 'low', 'close', 'vol', 'close_ts'])
        df_3h_closed = df_3h[df_3h['close_ts'] <= now_utc].reset_index(drop=True)

        # 找出歷史 18D 中的最後一個吞噬 (L1 方向)
        l1_18d_direction = ""
        l1_date_str = "未知"
        for i in range(1, len(df_18d_closed)):
            _prev = df_18d_closed.iloc[i-1]
            _curr = df_18d_closed.iloc[i]
            _p_open = float(_prev['open'])
            _p_close = float(_prev['close'])
            _c_close = float(_curr['close'])
            _p_body_high = max(_p_open, _p_close)
            _p_body_low = min(_p_open, _p_close)
            
            if _c_close > _p_body_high:
                l1_18d_direction = "LONG"
                l1_date_str = pd.to_datetime(int(_curr['ts']), unit='ms', utc=True).tz_convert('Asia/Taipei').strftime('%Y-%m-%d')
            elif _c_close < _p_body_low:
                # 策略僅做多，遇到黑吞則強制重置
                l1_18d_direction = ""
                l1_date_str = pd.to_datetime(int(_curr['ts']), unit='ms', utc=True).tz_convert('Asia/Taipei').strftime('%Y-%m-%d')

        if l1_18d_direction != "LONG":
            return None

        # 單一時間軸事件推進：從舊往新找，即時監控失效條件
        list_3d = sorted(df_3d_closed.to_dict('records'), key=lambda r: r['ts'])

        # 狀態變數 (配合 L2/L3 命名)
        l2_valid = False
        l2_valid_ts = 0
        l2_open_ts = 0
        l2_high = 0.0
        l2_low = 0.0
        l2_date_str = "未知"

        l3_valid = False
        l3_direction = ""
        l3_date_str = "未知"
        
        entry_price = 0.0
        stop_loss = 0.0
        trigger_ts = 0
        simulated_pos = False
        all_historical_c2s = []
        prev_close = None

        # 主迴圈：以 3H 推進
        for _, row in df_3h_closed.iterrows():
            t = int(row['ts'])
            t_close = int(row['close_ts'])

            # 1. L2 邊界 (以最新已收盤 3D K 棒為準)
            if list_3d:
                latest_3d_row = None
                for _r in reversed(list_3d):
                    if int(_r['close_ts']) <= t_close:
                        latest_3d_row = _r
                        break
                if latest_3d_row:
                    new_open = int(latest_3d_row['ts'])
                    if new_open != l2_open_ts:
                        if simulated_pos:
                            simulated_pos = False
                            l3_valid = False
                            if all_historical_c2s and all_historical_c2s[-1]['status'] == 'active':
                                all_historical_c2s[-1]['status'] = 'closed'
                        l2_valid = True
                        l2_valid_ts = int(latest_3d_row['close_ts'])
                        l2_open_ts = new_open
                        l2_high = float(latest_3d_row['high'])
                        l2_low = float(latest_3d_row['low'])
                        l2_date_str = pd.to_datetime(l2_open_ts, unit='ms', utc=True).tz_convert('Asia/Taipei').strftime('%Y-%m-%d')
                        l3_valid = False
                        l3_direction = ""

            # 2. 止損檢查
            sl_hit_this_bar = False
            if simulated_pos:
                # 僅掃描多單，只需判斷多單止損
                is_sl = (l3_direction == 'LONG' and float(row['low']) <= stop_loss)
                if is_sl:
                    sl_hit_this_bar = True
                    simulated_pos = False
                    l3_valid = False
                    if all_historical_c2s and all_historical_c2s[-1]['status'] == 'active':
                        all_historical_c2s[-1]['status'] = 'closed'
                    latest_3d_row = None
                    for _r in reversed(list_3d):
                        if int(_r['close_ts']) <= t_close:
                            latest_3d_row = _r
                            break
                    if latest_3d_row:
                        l2_valid = True
                        l2_valid_ts = int(latest_3d_row['close_ts'])
                        l2_open_ts = int(latest_3d_row['ts'])
                        l2_high = float(latest_3d_row['high'])
                        l2_low = float(latest_3d_row['low'])
                        l2_date_str = pd.to_datetime(l2_open_ts, unit='ms', utc=True).tz_convert('Asia/Taipei').strftime('%Y-%m-%d')

            # 3. L3 觸發 (需 L1 18D 吞噬方向配合，若 l1_18d_direction 不符則不觸發)
            if not simulated_pos and l2_valid and t_close >= l2_valid_ts and l1_18d_direction:
                bar_high = float(row['high'])
                bar_low = float(row['low'])
                close_price = float(row['close'])
                if sl_hit_this_bar:
                    inside_ref = float(row['open'])
                elif prev_close is not None:
                    inside_ref = float(prev_close)
                else:
                    inside_ref = float(row['open'])

                triggered = False
                if l1_18d_direction == 'LONG':
                    is_long = bar_high > l2_high and close_price > l2_high and inside_ref <= l2_high
                    if is_long:
                        l3_direction = 'LONG'
                        entry_price = close_price
                        stop_loss = float(row['low'])
                        triggered = True

                if triggered:
                    simulated_pos = True
                    l3_valid = True
                    trigger_ts = int(row['ts'])
                    l3_date_str = pd.to_datetime(trigger_ts, unit='ms', utc=True).tz_convert('Asia/Taipei').strftime('%Y-%m-%d %H:%M:%S')
                    all_historical_c2s.append({
                        'symbol': symbol, 'l1_18d_direction': l1_18d_direction,
                        'l1_date': l1_date_str, 'l2_date': l2_date_str, 'l3_date': l3_date_str,
                        'entry_price': entry_price, 'stop_loss': stop_loss, 'trigger_ts': trigger_ts,
                        'l3_direction': l3_direction, 'precision': precision, 'l2_open_ts': l2_open_ts,
                        'status': 'active', 'has_entered': True,
                    })

            prev_close = float(row['close'])

        current_price = float(df_1d['close'].iloc[-1]) if not df_1d.empty else 0.0
        for c2 in all_historical_c2s:
            c2['current_price'] = current_price

        is_trigger_met = l3_valid and simulated_pos
        if is_trigger_met:
            final_state = 'triggered'
        elif l2_valid:
            final_state = 'l2_waiting'
        else:
            final_state = 'l2_waiting'

        cache_ts = trigger_ts if is_trigger_met else l2_valid_ts
        action = 'update' if (not cached_info or cached_info.get('ts') != cache_ts) else 'keep'
        # 過濾：只保留屬於「最新一根已收盤 3D K 線」區間內的歷史訊號
        filtered_c2s = [c2 for c2 in all_historical_c2s if c2.get('l2_open_ts') == l2_open_ts]

        if l2_valid and len(filtered_c2s) == 0:
            logger.info(
                f"🔎 {symbol} L2 H={l2_high:.{precision}f} L={l2_low:.{precision}f} "
                f"現價={current_price:.{precision}f} 本3D週期訊號=0"
            )
        elif filtered_c2s:
            last_sig = filtered_c2s[-1]
            logger.info(
                f"🔎 {symbol} L2 H={l2_high:.{precision}f} L={l2_low:.{precision}f} "
                f"本3D訊號={len(filtered_c2s)} 末筆={last_sig.get('l3_direction')} ({last_sig.get('status')})"
            )
        elif not l2_valid:
            logger.info(f"🔎 {symbol} L2 尚未成立 (3D 已收盤 K 棒不足)")

        if final_state in ['l2_waiting']:
            # L2 成立等待 L3：幣種必須保留在 watchlist，否則會被掃描器刪掉
            expected_dir = l3_direction if l3_direction else ""
            return {
                'symbol':              symbol,
                'action':              action,           # 'update' or 'keep'
                'data':                {'ts': cache_ts},
                'is_trigger_met':      False,
                'is_watchlist_eligible': False,
                'entry_price':         0.0,
                'stop_loss':           0.0,
                'trigger_ts':          0,
                'precision':           precision,
                'l1_18d_direction':    l1_18d_direction if l1_18d_direction else '未知',
                'l1_date':             l1_date_str,
                'l2_date':             l2_date_str if l2_valid else '未知',
                'l3_date':             '',
                'l3_direction':        expected_dir,
                'scan_state':          'l2_waiting',
                'l2_high':             l2_high,
                'l2_low':              l2_low,
                'l2_open_ts':          l2_open_ts,
                'historical_c2s':      filtered_c2s,
            }

        return {
            'symbol':             symbol,
            'action':             action,
            'data':               {'ts': cache_ts},
            'is_trigger_met':     is_trigger_met,
            'is_watchlist_eligible': True,
            'entry_price':        entry_price,
            'stop_loss':          stop_loss,
            'trigger_ts':         trigger_ts,
            'precision':          precision,
            'l1_18d_direction':   l1_18d_direction if l1_18d_direction else '未知',
            'l1_date':            l1_date_str,
            'l2_date':            l2_date_str,
            'l3_date':            l3_date_str,
            'l3_direction':       l3_direction,
            'scan_state':         final_state,
            'l2_high':            l2_high,
            'l2_low':             l2_low,
            'l2_open_ts':         l2_open_ts,
            'historical_c2s':     filtered_c2s,
        }

    except Exception as e:
        logger.warning(f"掃描異常 ({symbol}): {type(e).__name__}: {e}")
        return None


# ============================================================================
# 訊息推送模組
# ============================================================================

def send_grouped_message(item_list, title):
    """合併傳入的幣種清單為一則群組訊息，按 L2 (3H 突破) 日期排列"""
    if not item_list:
        return

    filtered_items = []
    for item in item_list:
        l3 = item.get('l3_date', item.get('l2_date', ''))
        if not l3 or l3 in ('未知', '未知日期', ''):
            ts = item.get('trigger_ts', 0)
            if ts > 0:
                l3 = pd.to_datetime(ts, unit='ms', utc=True).tz_convert('Asia/Taipei').strftime('%Y-%m-%d %H:%M:%S')
        if l3 and l3 not in ('未知', '未知日期', '', '外部建倉', '持續追蹤'):
            item = dict(item)
            item['l3_date'] = l3
            filtered_items.append(item)

    if not filtered_items:
        return

    date_groups = {}
    for item in filtered_items:
        raw_date = item.get('l3_date', '')
        d = raw_date[:10] if len(raw_date) >= 10 else raw_date
        if d not in date_groups:
            date_groups[d] = []
        date_groups[d].append(item)

    lines = [f"<b>{title}</b>\n"]
    for date_key in sorted(date_groups.keys()):
        coin_strs = []
        for item in date_groups[date_key]:
            base = get_base_coin(item['symbol'])
            direction = item.get('l3_direction', item.get('l2_direction', ''))
            dir_str = " 🟢" if direction == "LONG" else " 🔴" if direction == "SHORT" else ""
            
            if item.get('missed') and title != '🛑 <b>加密貨幣[未上車]</b>':
                coin_strs.append(f"{base} (未上車){dir_str}")
            else:
                coin_strs.append(f"{base}{dir_str}")
                
        coins = " · ".join(coin_strs)
        lines.append(f"📅 {date_key}")
        lines.append(f"💎 {coins}\n")

    send_telegram_message("\n".join(lines))



def send_system_settings_message(config):
    """獨立一則系統設定訊息"""
    capital = config.get("total_capital", 300)
    loss_pct = config.get("loss_pct", 2)
    blacklist = config.get("blacklist", ["XAUT", "PAXG", "TQQQ", "SQQQ"])
    bl_str = ", ".join(blacklist) if blacklist else "無"

    msg = (
        f"⚙️ <b>系統快速設定</b>\n\n"
        f"💰 <b>預設總資金:</b> {capital} USDT\n"
        f"📉 <b>每筆虧損:</b> {loss_pct}%\n"
        f"🚫 <b>黑名單:</b> {bl_str}"
    )

    reply_markup = {
        "inline_keyboard": [
            [
                {"text": "💰 修改總資金", "switch_inline_query_current_chat": "/set_capital "},
                {"text": "📉 修改虧損比例", "switch_inline_query_current_chat": "/set_loss_pct "}
            ],
            [
                {"text": "➕ 增加黑名單", "switch_inline_query_current_chat": "/add_blacklist "},
                {"text": "➖ 移除黑名單", "switch_inline_query_current_chat": "/remove_blacklist "}
            ]
        ]
    }

    send_telegram_message(msg, reply_markup=reply_markup)

# ============================================================================
# Telegram 指令處理
# ============================================================================

_tg_update_offset = 0

def poll_telegram_commands():
    """輪詢 Telegram getUpdates，處理指令與 Callback Queries"""
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

            if chat_id != str(TG_CHAT_ID) or not text:
                continue

            text = re.sub(r'^@\w+\s*', '', text).strip()

            reply = ""
            if text.startswith("/set_capital"):
                parts = text.split()
                if len(parts) >= 2:
                    try:
                        new_val = float(parts[1])
                        if new_val <= 0:
                            raise ValueError("金額必須大於 0")
                        config = load_config()
                        config["total_capital"] = new_val
                        save_config(config)
                        loss_pct = config.get("loss_pct", 2)
                        reply = f"✅ 總資金已更新為 <b>{new_val} USDT</b>，每筆虧損 <b>{loss_pct}%</b>"
                        logger.info(f"⚙️ /set_capital 指令: 總資金更新為 {new_val}")
                    except ValueError:
                        reply = "❌ 格式錯誤，請輸入大於零的數字。"
                else:
                    reply = "❌ 格式錯誤，未提供數字。"
                    
            elif text.startswith("/set_loss_pct"):
                parts = text.split()
                if len(parts) >= 2:
                    try:
                        new_val = float(parts[1])
                        if new_val <= 0 or new_val > 100:
                            raise ValueError("比例必須在 0 到 100 之間")
                        config = load_config()
                        config["loss_pct"] = new_val
                        save_config(config)
                        capital = config.get("total_capital", 300)
                        reply = f"✅ 每筆虧損比例已更新為 <b>{new_val}%</b> (總資金: {capital} USDT)"
                        logger.info(f"⚙️ /set_loss_pct 指令: 虧損比例更新為 {new_val}%")
                    except ValueError:
                        reply = "❌ 格式錯誤，請輸入大於 0 且小於等於 100 的數字。"
                else:
                    reply = "❌ 格式錯誤，未提供數字。"

            elif text.startswith("/add_blacklist"):
                parts = text.split(maxsplit=1)
                if len(parts) >= 2:
                    coin = parts[1].strip().upper()
                    config = load_config()
                    blacklist = config.get("blacklist", ["XAUT", "PAXG", "TQQQ", "SQQQ"])
                    if coin not in blacklist:
                        blacklist.append(coin)
                        config["blacklist"] = blacklist
                        save_config(config)
                        reply = f"✅ 已將 <b>{coin}</b> 加入黑名單"
                        logger.info(f"⚙️ /add_blacklist 指令: 新增 {coin}")
                    else:
                        reply = f"⚠️ <b>{coin}</b> 已經在黑名單中"
                else:
                    reply = "❌ 格式錯誤，未提供幣種名稱。"

            elif text.startswith("/remove_blacklist"):
                parts = text.split(maxsplit=1)
                if len(parts) >= 2:
                    coin = parts[1].strip().upper()
                    config = load_config()
                    blacklist = config.get("blacklist", ["XAUT", "PAXG", "TQQQ", "SQQQ"])
                    if coin in blacklist:
                        blacklist.remove(coin)
                        config["blacklist"] = blacklist
                        save_config(config)
                        reply = f"✅ 已將 <b>{coin}</b> 從黑名單移除"
                        logger.info(f"⚙️ /remove_blacklist 指令: 移除 {coin}")
                    else:
                        reply = f"⚠️ <b>{coin}</b> 不在黑名單中"
                else:
                    reply = "❌ 格式錯誤，未提供幣種名稱。"

                send_url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
                payload = {"chat_id": chat_id, "text": reply, "parse_mode": "HTML"}
                requests.post(send_url, json=payload, timeout=10)



    except Exception as e:
        logger.warning(f"Telegram 指令輪詢異常: {e}")

# ============================================================================
# 主掃描流程
# ============================================================================

async def run_scan():
    logger.info("⏰ 開始執行 極速系統...")
    ex = get_exchange()
    watchlist = load_watchlist()
    config = load_config()
    default_loss = config.get("total_capital", 300) * config.get("loss_pct", 2) / 100

    try:
        try:
            # 抓取全市場 USDT-FUTURES 非 RWA 幣種（跳過股票合約）
            whitelist = _get_crypto_whitelist()
            markets = await ex.load_markets()
            custom_blacklist = config.get("blacklist", ["XAUT", "PAXG", "TQQQ", "SQQQ"])
            exclude_bases = {"USDC", "FDUSD", "TUSD", "USDP", "BUSD", "EUR", "GBP", "DAI"}
            exclude_bases.update(custom_blacklist)
            coins = []

            skipped_symbols = []
            for ccxt_sym, m in markets.items():
                # 只取 linear USDT 永續合約
                if not (m.get('linear') and m.get('quote') == 'USDT' and m.get('settle') == 'USDT'):
                    continue
                base = ccxt_sym.split('/')[0]
                raw_bitget_sym = f"{base}USDT"
                if base in exclude_bases:
                    skipped_symbols.append(f"{base}(黑名單)")
                    continue
                # 白名單非空時，不在白名單內代表股票/RWA 合約，直接跳過
                if whitelist and raw_bitget_sym not in whitelist:
                    skipped_symbols.append(f"{base}(股票合約/RWA)")
                    continue
                coins.append(ccxt_sym)
            if skipped_symbols:
                logger.warning(f"⚠️ 過濾掉 {len(skipped_symbols)} 個幣種 (前10): {', '.join(skipped_symbols[:10])}{'...' if len(skipped_symbols) > 10 else ''}")
            logger.info(f"📊 實際掃描幣種: {len(coins)} 個 (全市場非RWA)")
            save_scanned_coins(coins)

            # 必須包含目前正在持倉與等待追蹤的幣種
            active_signals = load_active_signals()
            active_syms = set()
            for slist in active_signals.values():
                for s in slist:
                    if s['status'] == 'active':
                        active_syms.add(s['symbol'])
                        
            for sym in set(list(watchlist.keys()) + list(active_syms)):
                if sym not in coins and sym in markets:
                    coins.append(sym)
            
            if not coins:
                for s, m in markets.items():
                    if m.get('linear') and m.get('quote') == 'USDT':
                        base = s.split('/')[0]
                        if base not in exclude_bases:
                            coins.append(s)
            
            precisions = {s: max(0, int(round(-np.log10(markets[s].get('precision', {}).get('price', 1e-8))))) for s in coins}
        except Exception as e:
            logger.error(f"拉取市場資料失敗: {e}")
            coins = ["BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT", "XRP/USDT:USDT", "DOGE/USDT:USDT", "ADA/USDT:USDT"]
            precisions = {s: 4 for s in coins}; precisions.update({"BTC/USDT:USDT": 2, "ETH/USDT:USDT": 2})

        existing_positions = []
        if BITGET_API_KEY:
            try:
                positions = await ex.fetch_positions()
                existing_positions = [p for p in positions if float(p.get('contracts', 0) or p.get('size', 0)) > 0]
                save_holdings([get_base_coin(p['symbol']) for p in existing_positions])
            except Exception as e:
                logger.warning(f"拉取持倉列表失敗 (下單防重複查詢): {e}")

        all_results = []
        all_past_events = []
        latest_l1_open_ts_map = {}
        total_coins = len(coins)
        for i in range(0, total_coins, 20):
            batch = coins[i:i+20]
            tasks = [scan_for_symbol(ex, s, get_base_coin(s), precisions[s], i + idx + 1, total_coins, watchlist.get(s)) for idx, s in enumerate(batch)]
            results = await asyncio.gather(*tasks)

            for res in results:
                if res is None: continue
                sym = res['symbol']
                
                if res.get('l1_open_ts', 0) > 0:
                    latest_l1_open_ts_map[get_base_coin(sym)] = res['l1_open_ts']
                
                if 'historical_c2s' in res:
                    all_past_events.extend(res['historical_c2s'])
                
                if res.get('is_watchlist_eligible'):
                    if res['action'] == 'update' or res['action'] == 'keep':
                        old_last_trigger_ts = watchlist.get(sym, {}).get('last_trigger_ts', 0)
                        watchlist[sym] = res['data']
                        if old_last_trigger_ts > 0:
                            watchlist[sym]['last_trigger_ts'] = old_last_trigger_ts
                        all_results.append(res)
                else:
                    if sym in watchlist:
                        del watchlist[sym]

            await asyncio.sleep(0.5)

        save_watchlist(watchlist)
        active_count = len(watchlist)

        signals = load_active_signals()
        
        holding_map = {}
        for slist in signals.values():
            for s in slist:
                if s['status'] == 'active':
                    sym = s['symbol']
                    ts = s.get('timestamp', 0)
                    l3_date = s.get('l3_date', s.get('l2_date', ''))
                    if not l3_date and ts > 0:
                        l3_date = pd.to_datetime(ts, unit='ms', utc=True).tz_convert('Asia/Taipei').strftime('%Y-%m-%d %H:%M:%S')
                    holding_map[sym] = {
                        'symbol':        sym,
                        'l3_direction':  s.get('direction', ''),
                        'entry_price':   s.get('entry_price', 0.0),
                        'stop_loss':     s.get('original_sl_price', s.get('sl_price', 0.0)),
                        'precision':     s.get('precision', 4),
                        'trigger_ts':    ts,
                        'l2_open_ts':    s.get('l2_open_ts', s.get('l1_open_ts', 0)),
                        'l1_18d_direction': s.get('l1_18d_direction', ''),
                        'l2_date':       s.get('l2_date', s.get('l1_date', '')),
                        'l3_date':       l3_date,
                        'status':        'active',
                    }

        for p in existing_positions:
            sym = p['symbol']
            if sym not in holding_map:
                holding_map[sym] = {'symbol': sym, 'l2_direction': p['side'].upper(), 'status': 'active'}

        holding_items = []
        real_holding_items = []
        pending_items = []
        existing_pos_syms = set(p['symbol'] for p in existing_positions)
        real_new_triggers = []
        missed_items = []

        for sym, data in holding_map.items():
            holding_items.append(data)
            if sym in existing_pos_syms:
                real_holding_items.append(data)
            else:
                pending_items.append(data)
        
        holding_items_dict = {item['symbol']: item for item in holding_items}

        real_holding_new_triggers = []

        for item in all_results:
            sym = item['symbol']
            
            if sym in holding_items_dict:
                if item.get('is_trigger_met'):
                    cached = watchlist.get(sym, {})
                    trigger_ts = item.get('trigger_ts', 0)
                    if cached.get('last_trigger_ts') != trigger_ts or trigger_ts == 0:
                        real_holding_new_triggers.append(item)
                        if sym in watchlist:
                            watchlist[sym]['last_trigger_ts'] = trigger_ts
            elif item.get('is_trigger_met'):
                cached = watchlist.get(sym, {})
                trigger_ts = item.get('trigger_ts', 0)
                if cached.get('last_trigger_ts') == trigger_ts and trigger_ts > 0:
                    item['missed'] = True
                    missed_items.append(item)
                else:
                    real_new_triggers.append(item)

        holding_items = list(holding_items_dict.values())

        real_new_triggers_final = []
        for item in real_new_triggers:
            sym = item['symbol']
            if BITGET_API_KEY:
                order = await place_order(
                    ex, sym, item.get('l3_direction', item.get('l2_direction', 'LONG')), item['entry_price'], item['stop_loss'],
                    item['precision'], default_loss, item.get('trigger_ts', 0),
                    l2_high=item.get('l2_high', 0.0), l2_low=item.get('l2_low', 0.0), l2_open_ts=item.get('l2_open_ts', 0),
                    l1_18d_direction=item.get('l1_18d_direction', ''), l2_date=item.get('l2_date', ''), l3_date=item.get('l3_date', '')
                )
                if order == 'skipped':
                    # 標記為錯失並更新 last_trigger_ts，防止重複下單與警報
                    if sym in watchlist:
                        watchlist[sym]['last_trigger_ts'] = item.get('trigger_ts', 0)
                    save_watchlist(watchlist)
                    item['missed'] = True
                    missed_items.append(item)
                elif order:
                    if sym in watchlist:
                        watchlist[sym]['last_trigger_ts'] = item.get('trigger_ts', 0)
                    save_watchlist(watchlist)
                    real_new_triggers_final.append(item)
            else:
                real_new_triggers_final.append(item)
        real_new_triggers = real_new_triggers_final

        history_signals = load_history_signals()

        # 清除所有不屬於最新 3D 區間的舊紀錄
        for base, current_l2_ts in latest_l1_open_ts_map.items():
            if base in history_signals and current_l2_ts > 0:
                history_signals[base] = [s for s in history_signals[base] if s.get('l2_open_ts', s.get('l1_open_ts', 0)) >= current_l2_ts]

        # 只記錄掃描器推演出的 C2 事件，與實際倉位無關
        all_triggered = real_new_triggers + missed_items + all_past_events
        for item in all_triggered:
            base = get_base_coin(item['symbol'])
            if base not in history_signals:
                history_signals[base] = []

            # 以 trigger_ts 作為唯一鍵防止重複寫入
            hist_id = f"{item.get('trigger_ts', 0)}_{base}"
            
            existing = next((s for s in history_signals[base] if s.get('_hist_id') == hist_id), None)
            
            new_status = item.get('status', 'active')
            if 'missed' in item and item['missed']:
                new_status = 'missed'
                
            if not existing:
                item_copy = item.copy()
                item_copy['_hist_id'] = hist_id
                item_copy['status'] = new_status
                item_copy['has_entered'] = True
                history_signals[base].append(item_copy)
            else:
                # 同步狀態：若是 closed 則必定覆蓋；若是 active 則覆蓋舊版的 triggered/missed
                if new_status == 'closed':
                    existing['status'] = 'closed'
                elif new_status == 'active' and existing.get('status') in ['triggered', 'missed']:
                    existing['status'] = 'active'
                # 每輪掃描都刷新 current_price，確保 RR 計算準確
                existing['has_entered'] = True
                cp = item.get('current_price')
                if cp:
                    existing['current_price'] = cp
        save_history_signals(history_signals)

        signals = load_active_signals()
        
        if real_holding_items:
            send_grouped_message(real_holding_items, "💼 <b>加密貨幣[持倉中]</b>")
        if pending_items:
            send_grouped_message(pending_items, "⏳ <b>加密貨幣[掛單中]</b>")

        active_count = len(watchlist)
        logger.info(f"✅ 掃描完成。新觸發: {len(real_new_triggers)} / 持倉: {len(real_holding_items)} / 掛單: {len(pending_items)} / 持倉新訊號: {len(real_holding_new_triggers)} / 未上車: {len(missed_items)} / 追蹤總數: {active_count}")

        if not real_new_triggers and not holding_items and not missed_items:
            send_telegram_message(f"✅ <b>條件掃描完成</b>\n本次共掃描 {total_coins} 個幣種，無滿足條件標的。\n(當前追蹤觸發訊號: {active_count} 個)")

        if real_new_triggers or holding_items or real_holding_new_triggers or missed_items:
            send_system_settings_message(config)
    finally:
        await ex.close()

async def scheduler():
    last_hour = -1
    
    try:
        await run_scan()
    except Exception as e:
        logger.error(f"初始掃描異常: {e}")

    while True:
        try:
            now = datetime.utcnow()
            if now.hour % 3 == 0 and now.minute <= 10 and now.hour != last_hour:
                try:
                    await run_scan()
                except Exception as e:
                    logger.error(f"定時掃描異常: {e}")
                last_hour = now.hour

            if BITGET_API_KEY:
                ex = get_exchange()
                try:
                    await monitor_positions(ex)
                except Exception as e:
                    logger.error(f"監控週期異常: {e}")
                finally:
                    await ex.close()

        except Exception as e:
            logger.critical(f"💥 Scheduler 頂層異常 (已攔截): {e}")
        await asyncio.sleep(60)

# ============================================================================
# Flask Web UI Dashboard
# ============================================================================

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="zh-TW">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>極速掃描器 — 訊號歷史紀錄</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    background: #0a0a0f;
    color: #c9d1d9;
    font-family: 'Inter', 'Segoe UI', sans-serif;
    display: flex;
    height: 100vh;
    overflow: hidden;
  }

  /* === Sidebar === */
  .sidebar {
    width: 240px;
    flex-shrink: 0;
    background: #0d1117;
    border-right: 1px solid #21262d;
    display: flex;
    flex-direction: column;
    overflow: hidden;
  }
  .sidebar-header {
    padding: 20px 16px 12px;
    border-bottom: 1px solid #21262d;
  }
  .sidebar-header h1 { font-size: 0.95rem; font-weight: 600; color: #f0f6fc; letter-spacing: 0.03em; }
  .sidebar-header p { font-size: 0.72rem; color: #6e7681; margin-top: 4px; }
  .sidebar-search {
    padding: 10px 12px;
    border-bottom: 1px solid #21262d;
  }
  .sidebar-search input {
    width: 100%;
    background: #161b22;
    border: 1px solid #30363d;
    border-radius: 6px;
    color: #c9d1d9;
    font-size: 0.8rem;
    padding: 6px 10px;
    outline: none;
    transition: border-color 0.2s;
  }
  .sidebar-search input:focus { border-color: #58a6ff; }
  .symbol-list { flex: 1; overflow-y: auto; padding: 6px 0; }
  .symbol-item {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 9px 16px;
    cursor: pointer;
    font-size: 0.82rem;
    font-weight: 500;
    color: #8b949e;
    transition: background 0.15s, color 0.15s;
    border-left: 3px solid transparent;
  }
  .symbol-item:hover { background: #161b22; color: #c9d1d9; }
  .symbol-item.active { background: #161b22; color: #f0f6fc; border-left-color: #58a6ff; }
  .symbol-item .count {
    font-size: 0.7rem;
    background: #21262d;
    border-radius: 10px;
    padding: 2px 7px;
    color: #6e7681;
  }
  .symbol-item.has-signals .count { background: #1f3d21; color: #3fb950; }

  /* === Main === */
  .main { flex: 1; display: flex; flex-direction: column; overflow: hidden; }
  .main-header {
    padding: 18px 28px;
    border-bottom: 1px solid #21262d;
    display: flex;
    justify-content: space-between;
    align-items: center;
    background: #0d1117;
    flex-shrink: 0;
  }
  .main-header h2 { font-size: 1rem; font-weight: 600; color: #f0f6fc; }
  .main-header .meta { font-size: 0.78rem; color: #6e7681; }
  .main-header .refresh-dot {
    width: 8px; height: 8px;
    border-radius: 50%;
    background: #3fb950;
    display: inline-block;
    margin-right: 6px;
    animation: pulse 2s infinite;
  }
  @keyframes pulse { 0%,100%{opacity:1;} 50%{opacity:0.3;} }

  .signal-container { flex: 1; overflow-y: auto; padding: 24px 28px; }

  /* === Signal Cards === */
  .signal-card {
    background: #0d1117;
    border: 1px solid #21262d;
    border-radius: 10px;
    padding: 18px 20px;
    margin-bottom: 16px;
    border-left: 4px solid #30363d;
    transition: border-color 0.2s, box-shadow 0.2s;
    animation: fadeIn 0.3s ease;
  }
  @keyframes fadeIn { from{opacity:0;transform:translateY(4px);} to{opacity:1;transform:translateY(0);} }
  .signal-card:hover { box-shadow: 0 0 0 1px #30363d; }
  .signal-card.LONG { border-left-color: #3fb950; }
  .signal-card.SHORT { border-left-color: #f85149; }
  .signal-card.active { border-color: #1f3d21; }
  .signal-card.closed { border-color: #3d1f1f; }

  .card-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 14px;
    flex-wrap: wrap;
    gap: 8px;
  }
  .card-title { display: flex; align-items: center; gap: 10px; }
  .dir-badge {
    font-size: 0.8rem;
    font-weight: 700;
    padding: 3px 9px;
    border-radius: 5px;
    letter-spacing: 0.05em;
  }
  .dir-badge.LONG { background: rgba(63,185,80,0.15); color: #3fb950; border: 1px solid rgba(63,185,80,0.3); }
  .dir-badge.SHORT { background: rgba(248,81,73,0.15); color: #f85149; border: 1px solid rgba(248,81,73,0.3); }
  .status-badge {
    font-size: 0.72rem;
    font-weight: 600;
    padding: 3px 8px;
    border-radius: 4px;
    letter-spacing: 0.04em;
    text-transform: uppercase;
  }
  .status-badge.active { background: rgba(63,185,80,0.1); color: #3fb950; border: 1px solid rgba(63,185,80,0.25); }
  .status-badge.closed { background: rgba(248,81,73,0.1); color: #f85149; border: 1px solid rgba(248,81,73,0.25); }
  .status-badge.missed { background: rgba(63,185,80,0.1); color: #3fb950; border: 1px solid rgba(63,185,80,0.25); }
  .status-badge.triggered { background: rgba(88,166,255,0.1); color: #58a6ff; border: 1px solid rgba(88,166,255,0.25); }
  .card-time { font-size: 0.75rem; color: #6e7681; }

  .card-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(190px, 1fr));
    gap: 12px;
  }
  .detail-block { }
  .detail-label {
    font-size: 0.7rem;
    color: #6e7681;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    margin-bottom: 4px;
  }
  .detail-value {
    font-size: 0.88rem;
    color: #e6edf3;
    font-family: 'SFMono-Regular', Consolas, monospace;
    font-weight: 500;
  }
  .detail-value.price { color: #58a6ff; }
  .detail-value.sl { color: #f85149; }

  .empty-state {
    text-align: center;
    padding: 80px 20px;
    color: #6e7681;
  }
  .empty-state .icon { font-size: 3rem; margin-bottom: 16px; }
  .empty-state p { font-size: 0.9rem; }

  /* Scrollbar */
  ::-webkit-scrollbar { width: 6px; }
  ::-webkit-scrollbar-track { background: transparent; }
  ::-webkit-scrollbar-thumb { background: #30363d; border-radius: 3px; }
  ::-webkit-scrollbar-thumb:hover { background: #484f58; }
</style>
</head>
<body>
  <div class="sidebar">
    <div class="sidebar-header">
      <h1>⚡ 極速掃描器</h1>
      <p>訊號歷史紀錄</p>
    </div>
    <div class="sidebar-search">
      <input type="text" id="search-input" placeholder="搜尋幣種..." oninput="filterSymbols()" />
    </div>
    <div class="symbol-list" id="symbol-list"></div>
  </div>

  <div class="main">
    <div class="main-header">
      <h2 id="header-title">📡 訊號總覽</h2>
      <div id="global-stats" style="margin-top: 10px; font-size: 0.85rem; color: #8b949e; display: flex; gap: 15px; flex-wrap: wrap;"></div>
      <div class="meta"><span class="refresh-dot"></span>每 10 秒自動更新</div>
    </div>
    <div class="signal-container" id="signal-container">
      <div class="empty-state">
        <div class="icon">📡</div>
        <p>資料載入中...</p>
      </div>
    </div>
  </div>

<script>
  let currentSymbol = null;
  let allData = {};
  let allSymbols = [];
  let priceMap = {};  // base -> current_price
  let allHoldings = [];

  async function fetchData() {
    try {
      const res = await fetch('/api/data');
      const json = await res.json();
      allData = json.history || {};
      allSymbols = json.watchlist || [];
      priceMap = json.price_map || {};
      allHoldings = json.holdings || [];

      // 把 current_price 補回歷史紀錄
      Object.entries(allData).forEach(([base, sigs]) => {
        const cp = priceMap[base];
        if (cp) sigs.forEach(s => { if (!s.current_price) s.current_price = cp; s._base = base; });
        else sigs.forEach(s => { s._base = base; });
      });
      
      let activeSigs = 0;
      let closedSigs = 0;
      let holdingSigs = 0;
      let pendingSigs = 0;
      let totalRR = 0;
      Object.values(allData).forEach(sigs => {
          sigs.forEach(s => {
              if (s.status === 'closed') {
                  closedSigs++;
              } else if (s.status === 'active' || s.status === 'missed') {
                  activeSigs++;
                  if (s.status === 'active') {
                      if (allHoldings.includes(s._base)) {
                          holdingSigs++;
                      } else {
                          pendingSigs++;
                      }
                  }
              }
              const entry = parseFloat(s.entry_price);
              const sl = parseFloat(s.stop_loss);
              const cp = parseFloat(s.current_price || entry);
              if (entry > 0 && sl > 0 && Math.abs(entry - sl) > 0 && (s.status === 'active' || s.status === 'missed')) {
                  const risk = Math.abs(entry - sl);
                  let rr = 0;
                  const sdir = s.l3_direction || s.l2_direction || 'LONG';
                  if (sdir === 'LONG') rr = (cp - entry) / risk;
                  if (sdir === 'SHORT') rr = (entry - cp) / risk;
                  totalRR += rr;
              }
          });
      });
      const totalSigs = activeSigs + closedSigs;
      const winRate = totalSigs > 0 ? ((totalSigs - closedSigs) / totalSigs * 100).toFixed(1) : 0;
      const rrText = totalRR >= 0 ? `+${totalRR.toFixed(2)}` : `${totalRR.toFixed(2)}`;
      const rrColor = totalRR >= 0 ? '#3fb950' : '#f85149';
      
      document.getElementById('global-stats').innerHTML = `
        <span>📊 訊號：<strong style="color:#3fb950">${activeSigs}</strong></span>
        <span>💼 持倉中：<strong style="color:#9e6a03">${holdingSigs}</strong></span>
        <span>⏳ 掛單中：<strong style="color:#e3b341">${pendingSigs}</strong></span>
        <span>❌ 止損：<strong style="color:#f85149">${closedSigs}</strong></span>
        <span>🏆 勝率：<strong style="color:#3fb950">${winRate}%</strong></span>
        <span>💰 有效總 RR：<strong style="color:${rrColor}">${rrText}</strong></span>
      `;
      
      renderSidebar(document.getElementById('search-input').value);
      if (currentSymbol) renderMain(currentSymbol);
      else renderHome();
    } catch (e) {
      console.error('Fetch error:', e);
    }
  }

  function filterSymbols() {
    renderSidebar(document.getElementById('search-input').value);
  }

  function renderSidebar(filter = '') {
    const list = document.getElementById('symbol-list');
    const q = filter.trim().toUpperCase();

    // 只顯示有訊號紀錄的幣種（allData 中有非空 signals 的 key）
    const allSet = Object.keys(allData).filter(k => allData[k].length > 0).sort();
    const filtered = q ? allSet.filter(s => s.includes(q)) : allSet;

    let html = '<div class="symbol-item ' + (currentSymbol === null ? 'active' : '') + '" onclick="goHome()"><span>🏠 訊號總覽</span></div>';
    filtered.forEach(sym => {
      const sigs = allData[sym] || [];
      const activeClass = sym === currentSymbol ? 'active' : '';
      html += `<div class="symbol-item ${activeClass} has-signals" onclick="selectSymbol('${sym}')">
        <span>${sym}</span>
        <span class="count">${sigs.length}</span>
      </div>`;
    });
    list.innerHTML = html || '<div style="padding:12px 16px;color:#6e7681;font-size:0.8rem;">尚無訊號幣種</div>';
  }

  function goHome() {
    currentSymbol = null;
    renderSidebar(document.getElementById('search-input').value);
    renderHome();
  }

  function renderHome() {
    document.getElementById('header-title').textContent = '📡 訊號總覽';
    const container = document.getElementById('signal-container');

    // 收集所有 active 的訊號，每筆帶上幣種名
    const activeSigs = [];
    Object.entries(allData).forEach(([base, sigs]) => {
      sigs.filter(s => s.status === 'active' || s.status === 'missed').forEach(s => {
        activeSigs.push({...s, _base: base});
      });
    });

    if (activeSigs.length === 0) {
      container.innerHTML = `<div class="empty-state"><div class="icon">🔍</div><p>目前最新 3D 區間尚無有效訊號</p></div>`;
      return;
    }

    activeSigs.sort((a, b) => (b.trigger_ts || 0) - (a.trigger_ts || 0));

    let html = '';
    activeSigs.forEach((sig, idx) => {
      const dir = sig.l3_direction || sig.l2_direction || 'LONG';
      const dirText = dir === 'LONG' ? '▲ LONG' : '▼ SHORT';
      const prec = sig.precision || 4;
      const entry = parseFloat(sig.entry_price);
      const sl = parseFloat(sig.stop_loss);
      const cp = parseFloat(sig.current_price || entry);
      let rrStr = '—';
      let rrCol = '#8b949e';
      if (entry > 0 && sl > 0 && Math.abs(entry - sl) > 0) {
        const risk = Math.abs(entry - sl);
        let rr = dir === 'LONG' ? (cp - entry) / risk : (entry - cp) / risk;
        rrStr = (rr >= 0 ? '+' : '') + rr.toFixed(2) + 'R';
        rrCol = rr >= 0 ? '#3fb950' : '#f85149';
      }
      const isHolding = allHoldings.includes(sig._base) && sig.status === 'active';
      html += `
      <div class="signal-card ${dir} active" style="cursor:pointer" onclick="selectSymbol('${sig._base}')">
        <div class="card-header">
          <div class="card-title">
            <span style="color:#58a6ff;font-weight:600;font-size:0.95rem;">${sig._base}</span>
            <span class="dir-badge ${dir}">${dirText}</span>
            ${isHolding ? '<span class="status-badge" style="background-color:#9e6a03;color:#fff;">持倉中</span>' : ''}
            <span style="font-size:0.85rem;font-weight:700;color:${rrCol};margin-left:auto;">${rrStr}</span>
          </div>
          <div class="card-time">突破進場：${fmt(sig.l3_date || sig.l2_date)}</div>
        </div>
        <div class="card-grid">
          <div class="detail-block">
            <div class="detail-label">L1 (18D) 吞噬方向 (日期)</div>
            <div class="detail-value">${sig.l1_18d_direction || '—'} (${sig.l1_date || '—'})</div>
          </div>
          <div class="detail-block">
            <div class="detail-label">L2 (3D) 邊界時間</div>
            <div class="detail-value">${fmt(sig.l2_date || sig.l1_date)}</div>
          </div>
          <div class="detail-block">
            <div class="detail-label">L3 (3H) 突破進場時間</div>
            <div class="detail-value">${fmt(sig.l3_date || sig.l2_date)}</div>
          </div>
          <div class="detail-block">
            <div class="detail-label">進場價格</div>
            <div class="detail-value price">${fmt(sig.entry_price, prec)}</div>
          </div>
          <div class="detail-block">
            <div class="detail-label">止損價格</div>
            <div class="detail-value sl">${fmt(sig.stop_loss, prec)}</div>
          </div>
          <div class="detail-block">
            <div class="detail-label">即時 RR</div>
            <div class="detail-value" style="color:${rrCol};font-weight:700;">${rrStr}</div>
          </div>
        </div>
      </div>`;
    });
    container.innerHTML = html;
  }

  function selectSymbol(sym) {
    currentSymbol = sym;
    renderSidebar(document.getElementById('search-input').value);
    renderMain(sym);
  }

  function fmt(val, decimals) {
    if (val == null || val === '' || val === undefined) return '—';
    if (decimals != null) return parseFloat(val).toFixed(decimals);
    return val;
  }

  function renderMain(sym) {
    document.getElementById('header-title').textContent = `💎 ${sym}　訊號歷史紀錄 (最新 3D 區間)`;
    const container = document.getElementById('signal-container');
    const signals = allData[sym] || [];

    if (signals.length === 0) {
      container.innerHTML = `<div class="empty-state"><div class="icon">🔍</div><p>此幣種在最新 3D 區間內尚無觸發訊號紀錄</p></div>`;
      return;
    }

    // 從舊到新排列
    const sorted = [...signals].sort((a, b) => (a.trigger_ts || 0) - (b.trigger_ts || 0));

    let html = '';
    sorted.forEach((sig, idx) => {
      const dir = sig.l3_direction || sig.l2_direction || 'LONG';
      const status = sig.status || 'unknown';
      const dirText = dir === 'LONG' ? '▲ LONG' : '▼ SHORT';
      const statusMap = { active: '有效', closed: '止損', missed: '有效', triggered: '歷史紀錄' };
      const statusText = statusMap[status] || status;
      const prec = sig.precision || 4;
      const isHolding = allHoldings.includes(sym) && status === 'active';

      html += `
      <div class="signal-card ${dir} ${status}">
        <div class="card-header">
          <div class="card-title">
            <span style="color:#6e7681;font-size:0.75rem;">#${idx + 1}</span>
            <span class="dir-badge ${dir}">${dirText}</span>
            ${(status === 'closed' || status === 'triggered') ? `<span class="status-badge ${status}">${statusText}</span>` : ''}
            ${isHolding ? '<span class="status-badge" style="background-color:#9e6a03;color:#fff;">持倉中</span>' : ''}
          </div>
          <div class="card-time">突破進場：${fmt(sig.l3_date || sig.l2_date)}</div>
        </div>
        <div class="card-grid">
          <div class="detail-block">
            <div class="detail-label">L1 (18D) 吞噬方向 (日期)</div>
            <div class="detail-value">${sig.l1_18d_direction || '—'} (${sig.l1_date || '—'})</div>
          </div>
          <div class="detail-block">
            <div class="detail-label">L2 (3D) 邊界時間</div>
            <div class="detail-value">${fmt(sig.l2_date || sig.l1_date)}</div>
          </div>
          <div class="detail-block">
            <div class="detail-label">L3 (3H) 突破進場時間</div>
            <div class="detail-value">${fmt(sig.l3_date || sig.l2_date)}</div>
          </div>
          <div class="detail-block">
            <div class="detail-label">進場價格</div>
            <div class="detail-value price">${fmt(sig.entry_price, prec)}</div>
          </div>
          <div class="detail-block">
            <div class="detail-label">止損價格</div>
            <div class="detail-value sl">${fmt(sig.stop_loss, prec)}</div>
          </div>
        </div>
      </div>`;
    });
    container.innerHTML = html;
  }

  fetchData();
  setInterval(fetchData, 10000);
</script>
</body>
</html>
"""

app = Flask(__name__)

@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route('/health')
def health():
    return {"status": "ok", "service": "Speed-Scanner-Auto"}, 200

@app.route('/api/data')
def api_data():
    watchlist = load_watchlist()
    history = load_history_signals()
    
    # 清理並合併舊的 history key；同時過濾掉沒有 entry_price 的空殼持倉紀錄
    cleaned_history = {}
    for k, v in history.items():
        base = get_base_coin(k)
        if base not in cleaned_history:
            cleaned_history[base] = []
        for sig in v:
            # 空殼（沒有進場價）代表是舊版持倉殘留，直接丟棄
            if not sig.get('entry_price') or float(sig.get('entry_price', 0)) == 0:
                continue
            # 觸發時間為 0 代表是舊版 bug 造成的等待中殘留紀錄，直接丟棄
            if not sig.get('trigger_ts') or int(sig.get('trigger_ts', 0)) == 0:
                continue
            ts = sig.get('trigger_ts')
            existing = next((s for s in cleaned_history[base] if s.get('trigger_ts') == ts), None)
            if not existing:
                cleaned_history[base].append(sig)
            else:
                # 優先保留最新狀態 (active/closed)，取代舊版的 triggered
                if existing.get('status') == 'triggered' and sig.get('status') != 'triggered':
                    cleaned_history[base].remove(existing)
                    cleaned_history[base].append(sig)
    # 清掉空的 key
    cleaned_history = {k: v for k, v in cleaned_history.items() if v}
    # 建立 base -> 最新 current_price 的對照表
    # 優先從 history 中找掃描時注入的 current_price；
    # 若沒有（舊紀錄），退而使用 active_signals 的 entry_price 當佔位符
    price_map = {}
    for base, sigs in cleaned_history.items():
        for s in sigs:
            cp = s.get('current_price')
            if cp:
                price_map[base] = cp
                break

    active_sigs = load_active_signals()
    for slist in active_sigs.values():
        for s in slist:
            if s.get('status') == 'active':
                base = get_base_coin(s['symbol'])
                if base not in price_map and s.get('entry_price'):
                    # 以進場價作為臨時佔位，前端 RR 算出 0，但至少不會崩潰
                    price_map[base] = float(s['entry_price'])

    # 側欄統一用 base coin 顯示；強制合併最新輪掃描幣種，確保無訊號幣種也能呈現
    scanned_coins = load_scanned_coins()
    watchlist_coins = sorted(set(
        scanned_coins +
        [get_base_coin(k) for k in watchlist.keys()] +
        list(cleaned_history.keys())
    ))
    holdings = load_holdings()
    return jsonify({"watchlist": watchlist_coins, "history": cleaned_history, "price_map": price_map, "holdings": holdings})

def run_flask():
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, use_reloader=False)

# = [啟動模組] ===============================================================

def run_background_system():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(scheduler())

def tg_polling_background():
    while True:
        try:
            poll_telegram_commands()
        except Exception as e:
            logger.error(f"Telegram polling 異常: {e}")
        time.sleep(2)

logger.info("🚀 啟動 極速系統...")
ensure_data_dir()

# 啟動遷移：凍結缺少 original_sl_price 的既有信號
_signals = load_active_signals()
_migrated = 0
for _key, _slist in _signals.items():
    for _sig in _slist:
        if _sig.get('status') == 'active' and 'original_sl_price' not in _sig:
            _sig['original_sl_price'] = _sig['sl_price']
            _migrated += 1
if _migrated > 0:
    save_active_signals(_signals)
    logger.info(f"🔧 啟動遷移完成: {_migrated} 筆信號已凍結 original_sl_price")
bg_thread = threading.Thread(target=run_background_system, daemon=True)
bg_thread.start()

tg_thread = threading.Thread(target=tg_polling_background, daemon=True)
tg_thread.start()

if __name__ == '__main__':
    run_flask()
