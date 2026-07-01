# 檔名: main.py

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
import sys
import io
import math
from flask import Flask, jsonify, render_template_string
from datetime import datetime, timezone
from dotenv import load_dotenv


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

# 停利：單一目標 100R，全倉平倉
TP_LADDER = [(100, 1.0)]


logger.info(f"✅ 系統配置檢查: TG_TOKEN={'已設定' if TG_BOT_TOKEN else '未設定'}, TG_CHAT_ID={'已設定' if TG_CHAT_ID else '未設定'}")
logger.info(f"✅ 交易所配置檢查: API_KEY={'已設定' if BITGET_API_KEY else '未設定'}")

# ============================================================================
# 狀態持久化 (Watchlist + Active Signals)
# ============================================================================

DATA_DIR = "/app/data"
WATCHLIST_FILE      = os.path.join(DATA_DIR, "watchlist.json")
ACTIVE_SIGNALS_FILE = os.path.join(DATA_DIR, "active_signals.json")
HISTORY_SIGNALS_FILE= os.path.join(DATA_DIR, "history_signals.json")
HOLDINGS_FILE       = os.path.join(DATA_DIR, "holdings.json")
# waiting_signals / scanned_coins 已合併進 watchlist，不再獨立存檔

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
    if not os.path.exists(HOLDINGS_FILE):
        with open(HOLDINGS_FILE, 'w') as f:
            json.dump([], f)

def save_holdings(bases: list):
    ensure_data_dir()
    try:
        with open(HOLDINGS_FILE, 'w', encoding='utf-8') as f:
            json.dump(list(set(bases)), f, ensure_ascii=False)
    except Exception as e:
        logger.error(f"儲存 holdings 失敗: {e}")

def load_holdings() -> list:
    """讀取交易所真實持倉的 base coin 清單 (由 monitor_positions / run_scan 寫入)"""
    ensure_data_dir()
    try:
        with open(HOLDINGS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return []

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
                config["blacklist"] = []
            return config
        except Exception as e:
            logger.error(f"讀取設定檔失敗: {e}")
    return {
        "total_capital": 300,
        "loss_pct": 2,
        "blacklist": [],
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


def compose_3h_bars(ohlcv_1h):
    """將 1H OHLCV 合成 3H K 棒 (按 UTC 00:00, 03:00, 06:00 基準對齊)"""
    if not ohlcv_1h:
        return []

    PERIOD_MS = 3 * 3600 * 1000
    groups = {}
    for bar in ohlcv_1h:
        ts = bar[0]
        # 使用 UTC epoch 計算
        group_key = (ts // PERIOD_MS) * PERIOD_MS
        if group_key not in groups:
            groups[group_key] = []
        groups[group_key].append(bar)

    composed = []
    sorted_keys = sorted(groups.keys())
    for gk in sorted_keys:
        bars = groups[gk]
        c_open = bars[0][1]
        c_high = max(b[2] for b in bars)
        c_low = min(b[3] for b in bars)
        c_close = bars[-1][4]
        c_vol = sum(b[5] for b in bars)
        composed.append([gk, c_open, c_high, c_low, c_close, c_vol, gk + PERIOD_MS])
    return composed


def compose_18d_bars(ohlcv_1d):
    """將 1D OHLCV 合成 18D K 棒 (按每年 1/1 起算)"""
    if not ohlcv_1d or len(ohlcv_1d) < 3:
        return []

    from datetime import datetime, timezone, timezone
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


# ============================================================================
# 交易執行
# ============================================================================

async def check_signal_expired(exchange, symbol, direction, entry, sl, precision, trigger_ts):
    """
    檢查下單前/掛單中的訊號是否應被廢棄。
    踢出標準（1）PSL: 18D 紅吞已產生移動保護止損
                (2) BLACK: 18D 黑吞出現，訊號失效
                (3) SL: 1H K 棒或即時市價已觸發止損
                (4) TP: 1H K 棒或即時市價已觸發 100R 止盈
    """
    if trigger_ts == 0:
        return False, "", ""

    # 部分計算結果儲存，供後續 18D 黑吞檢查複用
    dynamic_sl = sl
    _closed_18d_for_check = None
    try:
        _ohlcv_1d = []
        _end_time = int(time.time() * 1000)
        for _pg in range(30):
            _b = await exchange.fetch_ohlcv(symbol, '1d', limit=100, params={'endTime': _end_time})
            if not _b:
                break
            _ohlcv_1d.extend(_b)
            _end_time = _b[0][0] - 1
            if len(_ohlcv_1d) >= 360:
                break
        _ohlcv_1d = sorted({b[0]: b for b in _ohlcv_1d}.values(), key=lambda x: x[0])
        if _ohlcv_1d:
            _ohlcv_18d = compose_18d_bars(_ohlcv_1d)
            if _ohlcv_18d:
                _df_18d = pd.DataFrame(_ohlcv_18d, columns=['ts', 'open', 'high', 'low', 'close', 'vol', 'close_ts'])
                _now_ms = int(time.time() * 1000)
                _closed_18d = _df_18d[_df_18d['close_ts'] <= _now_ms].reset_index(drop=True)
                _closed_18d_for_check = _closed_18d  # 儲存備用

                if len(_closed_18d) >= 2:
                    _current_sl = sl
                    for i in range(1, len(_closed_18d)):
                        _last = _closed_18d.iloc[i]
                        _prev = _closed_18d.iloc[i - 1]
                        if int(_last['close_ts']) <= trigger_ts:
                            continue
                        _new_close = float(_last['close'])
                        _new_low   = float(_last['low'])
                        _prev_open  = float(_prev['open'])
                        _prev_close = float(_prev['close'])

                        _prev_body_high = max(_prev_open, _prev_close)
                        # 低點必須 > 進場價（保本條件）且 > 当前止損
                        if _new_close > _prev_body_high and _new_low > entry and _new_low > _current_sl:
                            _current_sl = _new_low
                    dynamic_sl = _current_sl
    except Exception as e:
        logger.warning(f"  過期檢查(18D止損)異常 ({symbol}): {e}")

    if dynamic_sl != sl:
        return True, f"已產生 18D 移動保護止損 (原: {sl:.{precision}f} -> 新: {dynamic_sl:.{precision}f})", 'PSL'

    # 18D 黑吞失效檢查：如果訊號後最新封已關閉 18D K 棒出現黑吞完成形態，訊號失效
    try:
        if _closed_18d_for_check is not None and len(_closed_18d_for_check) >= 2:
            _last_b = _closed_18d_for_check.iloc[-1]
            _prev_b = _closed_18d_for_check.iloc[-2]
            if int(_last_b['close_ts']) > trigger_ts:
                _lc = float(_last_b['close'])
                _po = float(_prev_b['open'])
                _pc = float(_prev_b['close'])
                _prev_body_low = min(_po, _pc)
                if _lc < _prev_body_low:
                    _dt = pd.to_datetime(int(_last_b['ts']), unit='ms', utc=True).tz_convert('Asia/Taipei').strftime('%Y-%m-%d')
                    return True, f"18D K 棒 ({_dt}) 出現黑吞，訊號失效", 'BLACK'
    except Exception as e:
        logger.warning(f"  過期檢查(18D黑吞)異常 ({symbol}): {e}")

    # 盤中止損防護：用 1H K棒 + 即時市價確認訊號是否在盤中已觸及止損，防止重複下單
    # 訊號是在 trigger_ts (1D K棒開盤時間) 這根 K 棒「收盤後」才確立。
    # 故只檢查這根 1D K 棒收盤（+24h）之後的 K 棒，不檢查訊號當天的價格波動。
    l3_close_ts = trigger_ts + 24 * 3600 * 1000 if trigger_ts > 0 else int(time.time() * 1000)
    since_ts = l3_close_ts
    try:
        is_long = direction.lower() in ('long', 'buy', '')
        tp_100r = entry + 100 * abs(entry - sl) if is_long else entry - 100 * abs(entry - sl)
        ohlcv_1h = await exchange.fetch_ohlcv(symbol, '1h', since=since_ts, limit=500)
        for candle in ohlcv_1h:
            c_ts = int(candle[0])
            c_high = float(candle[2])
            c_low = float(candle[3])
            if c_ts < l3_close_ts:
                continue
            dt_taiwan = pd.to_datetime(c_ts, unit='ms', utc=True).tz_convert('Asia/Taipei').strftime('%Y-%m-%d %H:%M')
            if is_long:
                if c_low <= sl:
                    return True, f"歷史 1H K 棒 ({dt_taiwan}) 最低價 {c_low:.{precision}f} 已觸發初始止損 ({sl:.{precision}f})", 'SL'
                if c_high >= tp_100r:
                    return True, f"歷史 1H K 棒 ({dt_taiwan}) 最高價 {c_high:.{precision}f} 已觸發 100R 止盈 ({tp_100r:.{precision}f})", 'TP'
            else:
                if c_high >= sl:
                    return True, f"歷史 1H K 棒 ({dt_taiwan}) 最高價 {c_high:.{precision}f} 已觸發初始止損 ({sl:.{precision}f})", 'SL'
                if c_low <= tp_100r:
                    return True, f"歷史 1H K 棒 ({dt_taiwan}) 最低價 {c_low:.{precision}f} 已觸發 100R 止盈 ({tp_100r:.{precision}f})", 'TP'

        ticker = await exchange.fetch_ticker(symbol)
        current_price = float(ticker['last'])
        if is_long:
            if current_price <= sl:
                return True, f"最新市價 {current_price:.{precision}f} 已觸發初始止損 ({sl:.{precision}f})", 'SL'
            if current_price >= tp_100r:
                return True, f"最新市價 {current_price:.{precision}f} 已觸發 100R 止盈 ({tp_100r:.{precision}f})", 'TP'
        else:
            if current_price >= sl:
                return True, f"最新市價 {current_price:.{precision}f} 已觸發初始止損 ({sl:.{precision}f})", 'SL'
            if current_price <= tp_100r:
                return True, f"最新市價 {current_price:.{precision}f} 已觸發 100R 止盈 ({tp_100r:.{precision}f})", 'TP'
    except Exception as e:
        logger.warning(f"  過期檢查(盤中止損)異常 ({symbol}): {e}")

    return False, "", ""



async def place_order(exchange, symbol, direction, entry, sl, precision, fixed_loss_usdt, trigger_ts,
                      l1_18d_direction='', l1_date='', l2_date='', l2_top_date='', l2_bottom_date=''):
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
            logger.warning(f"⚠️ {symbol} 下單前監測觸發，跳過下單: {skip_reason}")
            if alert_type == 'PSL':
                title = "<b>🚫 跳過自動下單 (已產生 18D 移動保護止損)</b>"
            elif alert_type == 'BLACK':
                title = "<b>🚫 跳過自動下單 (18D 黑吞，訊號已失效)</b>"
            elif alert_type == 'TP':
                title = "<b>🚫 跳過自動下單 (盤中已觸發 100R 止盈)</b>"
            else:
                title = "<b>🚫 跳過自動下單 (盤中已觸發止損)</b>"
            send_telegram_message(
                f"{title}\n\n"
                f"💎 <b>交易對:</b> {get_base_coin(symbol)} [{direction}]\n"
                f"🎯 進場價格: <code>{entry:.{precision}f}</code>\n"
                f"🛡️ 初始止損: <code>{sl:.{precision}f}</code>\n\n"
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

                side = 'buy'
                signal_id = f"entry_{uuid.uuid4().hex[:8]}"
                params = {
                    'hedged': True, 'holdSide': 'long',
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
                        if sl < current_p < entry:
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
                key = f"{get_base_coin(symbol)}_LONG"
                if key not in signals:
                    signals[key] = []
                signals[key].append({
                    'signal_id': signal_id, 'symbol': symbol, 'side': side, 'direction': direction,
                    'quantity': qty, 'entry_price': entry, 'sl_price': sl,
                    'original_sl_price': sl,
                    'status': 'active', 'precision': precision, 'tp_stage': 0,
                    'l1_18d_direction': l1_18d_direction, 'l1_date': l1_date, 'l2_date': l2_date,
                    'timestamp': trigger_ts if trigger_ts > 0 else int(time.time() * 1000)
                })
                save_active_signals(signals)
                l2_display = l2_date or (pd.to_datetime(trigger_ts, unit='ms', utc=True).tz_convert('Asia/Taipei').strftime('%Y-%m-%d %H:%M:%S') if trigger_ts > 0 else '未知')
                dir_str = "🟢 LONG"
                send_telegram_message(
                    f"<b>🤖 自動下單 ({leverage}x)</b>\n\n"
                    f"💎 {get_base_coin(symbol)} [{dir_str}]\n"
                    f"🎯 進場: <code>{entry:.{precision}f}</code>\n"
                    f"🛡️ 止損: <code>{sl:.{precision}f}</code>"
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
    """單一停利目標：100R 全倉平倉。"""
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
                        chk_tp_price = entry + r_mult * risk
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

        tp_price = entry + r_mult * risk
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

        order_side = 'sell'
        tp_params = {
            'triggerPrice': tp_price, 'triggerType': 'fill_price', 'reduceOnly': True,
            'hedged': True, 'holdSide': 'long',
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
        # 以真實交易所倉位更新 holdings.json，供前端區分「持倉中」vs「掛單中」
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
                            is_sl_side = (o['side'].lower() == 'sell')
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
                            order_side = 'sell'
                            sl_params = {
                                'triggerPrice': sl_price_target, 'triggerType': 'fill_price', 'reduceOnly': True,
                                'hedged': True, 'holdSide': 'long',
                                'clientOid': f"sl_{signal_id}"
                            }
                            await exchange.create_order(symbol, 'market', order_side, size, None, params=sl_params)
                        except Exception as e:
                            if '40786' not in str(e):
                                logger.error(f"掛止損失敗: {e}")


                    # 18D 黑吞平倉與移動止損檢查
                    _original_sl = float(sig.get('original_sl_price', sig['sl_price']))
                    _current_sl = float(sig['sl_price'])
                    _entry_ts = int(sig.get('timestamp', 0))
                    if _entry_ts > 0:
                        try:
                            _ohlcv_1d_mon = []
                            _end_time_mon = int(time.time() * 1000)
                            for _pg in range(30):
                                _b_mon = await exchange.fetch_ohlcv(symbol, '1d', limit=100, params={'endTime': _end_time_mon})
                                if not _b_mon:
                                    break
                                _ohlcv_1d_mon.extend(_b_mon)
                                _end_time_mon = _b_mon[0][0] - 1
                                if len(_ohlcv_1d_mon) >= 360:
                                    break
                            _ohlcv_1d_mon = sorted({b[0]: b for b in _ohlcv_1d_mon}.values(), key=lambda x: x[0])
                            if _ohlcv_1d_mon:
                                _ohlcv_18d_mon = compose_18d_bars(_ohlcv_1d_mon)
                                if _ohlcv_18d_mon:
                                    _df_18d_mon = pd.DataFrame(_ohlcv_18d_mon, columns=['ts', 'open', 'high', 'low', 'close', 'vol', 'close_ts'])
                                    _now_ms_mon = int(time.time() * 1000)
                                    _closed_18d_pos = _df_18d_mon[_df_18d_mon['close_ts'] <= _now_ms_mon].reset_index(drop=True)

                                    if len(_closed_18d_pos) >= 2:
                                        _last_18d = _closed_18d_pos.iloc[-1]
                                        _prev_18d = _closed_18d_pos.iloc[-2]
                                        _last_18d_close_ts = int(_last_18d['close_ts'])

                                        if _last_18d_close_ts > _entry_ts:
                                            _new_close = float(_last_18d['close'])
                                            _new_high  = float(_last_18d['high'])
                                            _new_low   = float(_last_18d['low'])
                                            _prev_open  = float(_prev_18d['open'])
                                            _prev_close = float(_prev_18d['close'])
                                            _entry_price = float(pos.get('entryPrice') or pos.get('avgPrice') or sig.get('entry_price', 0))

                                            _prev_body_high = max(_prev_open, _prev_close)
                                            _prev_body_low = min(_prev_open, _prev_close)
                                            _18d_dt = pd.to_datetime(int(_last_18d['ts']), unit='ms', utc=True).tz_convert('Asia/Taipei').strftime('%Y-%m-%d')

                                            # 檢查 1. 18D 黑吞市價平倉 (失效出場)
                                            if _new_close < _prev_body_low:
                                                logger.info(f"🚨 18D 黑吞觸發 ({symbol} {_18d_dt})，市價平倉！")
                                                try:
                                                    await exchange.create_order(symbol, 'market', 'sell', size, None, params={'reduceOnly': True})
                                                    send_telegram_message(
                                                        f"<b>🚨 18D 黑吞平倉觸發</b>\n\n"
                                                        f"💎 <b>交易對:</b> {get_base_coin(symbol)} [{side.upper()}]\n"
                                                        f"📅 <b>觸發 K 線:</b> {_18d_dt}\n"
                                                        f"💰 <b>平倉價格:</b> <code>市價</code>"
                                                    )
                                                    continue  # 平倉後不再執行後續掛單管理
                                                except Exception as ep:
                                                    logger.error(f"18D 黑吞平倉失敗 ({symbol}): {ep}")

                                            # 檢查 2. 18D 紅吞移動止損
                                            # 若 sl_price 已不等於 original_sl_price 代表已觸發過，跳過避免反覆移動
                                            if abs(_current_sl - _original_sl) < 1e-8:
                                                _new_sl = _current_sl
                                                _move_sl = False
                                                # 18D 低點必須在進場價上方（保本）且高於當前止損
                                                if _new_close > _prev_body_high and _new_low > _entry_price and _new_low > _current_sl:
                                                    _new_sl = _new_low
                                                    _move_sl = True

                                                if _move_sl:
                                                    logger.info(f"🔄 18D 吞噬 ({symbol} {_18d_dt})，移動止損至 {_new_sl}")
                                                    sig['sl_price'] = _new_sl
                                                    save_active_signals(saved_signals)
                                                    history_signals = load_history_signals()
                                                    base_coin = get_base_coin(symbol)
                                                    if base_coin in history_signals:
                                                        for hs in history_signals[base_coin]:
                                                            if hs.get('trigger_ts') == _entry_ts:
                                                                hs['trailing_sl'] = _new_sl
                                                                hs['trailing_sl_date'] = _18d_dt
                                                    save_history_signals(history_signals)
                                                    send_telegram_message(
                                                        f"<b>🔄 18D 移動止損觸發</b>\n\n"
                                                        f"💎 <b>交易對:</b> {get_base_coin(symbol)} [{side.upper()}]\n"
                                                        f"📅 <b>觸發 K 線:</b> {_18d_dt}\n"
                                                        f"🛡️ <b>新止損價:</b> <code>{_new_sl:.4f}</code>"
                                                    )
                        except Exception as _e18d:
                            logger.warning(f"18D 監控異常 ({symbol}): {_e18d}")

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
                                _is_opp = (po['side'].lower() == 'sell')
                                _trig_p = float(po.get('triggerPrice', 0) or po.get('info', {}).get('triggerPrice', 0))
                                if _is_opp and abs(_trig_p - sl_price) < 1e-8:
                                    try:
                                        await exchange.cancel_order(po['id'], symbol, params={'stop': True})
                                        logger.info(f"🚫 連帶撤銷掛單中的附屬止損: {po['id']}")
                                    except Exception:
                                        pass
                                        
                        if alert_type == 'PSL':
                            title = "已產生 18D 移動保護止損"
                        elif alert_type == 'BLACK':
                            title = "18D 黑吞失效"
                        elif alert_type == 'TP':
                            title = "盤中已觸發 100R 止盈"
                        elif alert_type == 'SL':
                            title = "盤中已觸發止損"
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
                    target_direction = "LONG"
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


def build_18d_black_swallow_boundaries(df_18d_closed):
    """
    掃描所有已收盤的 18D K棒，找出所有黑吞棒並建立空頭界線。
    黑吞定義：收盤價 < min(前棒 open, 前棒 close)
    界線 = 此黑吞棒的最高點 (high)
    回傳:
        boundaries: list of {'level': float, 'from_ts': int, 'date': str}
        is_latest_red_swallow: bool  (最新一根是否為紅吞，抑制新進場)
    """
    boundaries = []
    is_latest_red_swallow = False

    if len(df_18d_closed) < 2:
        return boundaries, is_latest_red_swallow

    def _fmt(ts_ms):
        return pd.to_datetime(int(ts_ms), unit='ms', utc=True).tz_convert('Asia/Taipei').strftime('%Y-%m-%d')

    for i in range(1, len(df_18d_closed)):
        prev = df_18d_closed.iloc[i - 1]
        curr = df_18d_closed.iloc[i]
        p_open  = float(prev['open']);  p_close = float(prev['close'])
        c_high  = float(curr['high']);  c_close = float(curr['close'])
        c_close_ts = int(curr['close_ts'])

        p_body_low  = min(p_open, p_close)
        p_body_high = max(p_open, p_close)
        is_black = c_close < p_body_low
        is_red   = c_close > p_body_high

        if is_black:
            boundaries.append({
                'level':   c_high,
                'from_ts': c_close_ts,
                'date':    _fmt(int(curr['ts'])),
            })

        if i == len(df_18d_closed) - 1:
            is_latest_red_swallow = is_red

    return boundaries, is_latest_red_swallow




def get_status(target_ts, timeline):
    if not timeline: return True, "新幣首發", 0
    for block in timeline:
        if block['start'] <= target_ts < block['end']:
            return block['valid'], block['date'], block['start']
    return True, "新幣首發", 0


def get_swallow(c_close, p_open, p_close):
    p_body_high = max(p_open, p_close)
    p_body_low  = min(p_open, p_close)
    if c_close > p_body_high: return 'RED'
    elif c_close < p_body_low: return 'BLACK'
    return 'NONE'

def build_timeline(df_closed):
    """
    18D 紅黑吞 timeline 建立。
    修正：從第一根 18D 棒的實際日期開始計算，不再顯示「新幣首發」。
    """
    import pandas as pd
    timeline = []
    if df_closed.empty:
        return timeline

    # 初始狀態：從第一根 18D 棒的開盤時間開始（而非 epoch=0）
    first_ts_ms = int(df_closed.iloc[0]['ts'])
    current_start = int(df_closed.iloc[0]['close_ts'])
    current_date  = pd.to_datetime(first_ts_ms, unit='ms', utc=True).tz_convert('Asia/Taipei').strftime('%Y-%m-%d')
    current_valid  = True  # 假設新幣上市時預設有效（先找到紅吞再確認）

    for i in range(1, len(df_closed)):
        _prev = df_closed.iloc[i-1]
        _curr = df_closed.iloc[i]
        sw = get_swallow(float(_curr['close']), float(_prev['open']), float(_prev['close']))
        c_ts = int(_curr['close_ts'])
        if sw == 'RED':
            if not current_valid:
                # 黑吞區段結束，封存黑吞區段
                timeline.append({'start': current_start, 'end': c_ts, 'valid': False, 'date': current_date})
                current_valid  = True
                current_start  = c_ts
                current_date   = pd.to_datetime(int(_curr['ts']), unit='ms', utc=True).tz_convert('Asia/Taipei').strftime('%Y-%m-%d')
        elif sw == 'BLACK':
            if current_valid:
                # 紅吞區段結束，封存紅吞區段
                timeline.append({'start': current_start, 'end': c_ts, 'valid': True, 'date': current_date})
                current_valid  = False
                current_start  = c_ts
                current_date   = pd.to_datetime(int(_curr['ts']), unit='ms', utc=True).tz_convert('Asia/Taipei').strftime('%Y-%m-%d')

    # 最後一個區段延伸至永遠
    timeline.append({'start': current_start, 'end': float('inf'), 'valid': current_valid, 'date': current_date})
    return timeline

def build_1d_structure_global(df_1d):
    """
    日線鋸齒結構（純轉折頂底），含擺動更新規則：
    - SEEKING_TOP 階段（紅吞期間找最高點）：
        當 BLACK 吞噬確認 → 新頂 = 區間內最高點。
        若新頂 > 舊頂 → 新底 = 舊頂到新頂之間最低點（更新底部）。
    - SEEKING_BOTTOM 階段（黑吞期間找最低點）：
        當 RED 吞噬確認 → 新底 = 區間內最低點。
        若新底 < 舊底 → 新頂 = 舊底到新底之間最高點（更新頂部）。
    """
    import pandas as pd

    def _fmt(ts_ms):
        return pd.to_datetime(int(ts_ms), unit='ms', utc=True).tz_convert('Asia/Taipei').strftime('%Y-%m-%d')

    boundaries = []
    phase = 'INIT'

    # 本輪追蹤（當前 phase 內）
    run_h = -1.0;          run_h_date = ''   # 本輪最高價（SEEKING_TOP 用）
    run_l = float('inf'); run_l_date = ''   # 本輪最低價（SEEKING_BOTTOM 用）

    # 跨向追蹤（用於擺動更新）
    inter_h = -1.0;          inter_h_date = ''  # SEEKING_BOTTOM 期間記錄的最高點
    inter_l = float('inf'); inter_l_date = ''  # SEEKING_TOP 期間記錄的最低點

    # 上一個確認的頂底
    conf_top = None   # {'price', 'date'}
    conf_bot = None   # {'price', 'date'}

    def push_boundary(top, bot, c_close_ts):
        if top:
            boundaries.append({
                'level':   top['price'],
                'from_ts': c_close_ts,
                't1_date': top['date'],
                'b2_level': bot['price'] if bot else float('inf'),
                'b2_date':  bot['date']  if bot else '',
            })

    for i in range(1, len(df_1d)):
        prev = df_1d.iloc[i-1]
        curr = df_1d.iloc[i]
        c_high     = float(curr['high'])
        c_low      = float(curr['low'])
        c_close    = float(curr['close'])
        c_ts       = int(curr['ts'])
        c_close_ts = int(curr['close_ts'])
        c_date     = _fmt(c_ts)

        sw = get_swallow(c_close, float(prev['open']), float(prev['close']))

        # ──── INIT：等第一個吞噬訊號 ────
        if phase == 'INIT':
            if sw == 'RED':
                phase     = 'SEEKING_TOP'
                run_h     = c_high; run_h_date = c_date
                inter_l   = c_low;  inter_l_date = c_date
            elif sw == 'BLACK':
                phase     = 'SEEKING_BOTTOM'
                run_l     = c_low;  run_l_date = c_date
                inter_h   = c_high; inter_h_date = c_date
            continue

        # ──── SEEKING_TOP（紅吞期）────
        if phase == 'SEEKING_TOP':
            if c_high > run_h: run_h = c_high; run_h_date = c_date
            if c_low  < inter_l: inter_l = c_low; inter_l_date = c_date

            if sw == 'BLACK':
                new_top = {'price': run_h, 'date': run_h_date}

                # 若新頂高於舊頂 → 舊頂到新頂之間最低點成為新底
                if conf_top and new_top['price'] > conf_top['price']:
                    conf_bot = {'price': inter_l, 'date': inter_l_date}

                conf_top = new_top
                push_boundary(conf_top, conf_bot, c_close_ts)

                # 切換到 SEEKING_BOTTOM
                phase    = 'SEEKING_BOTTOM'
                run_l    = c_low;  run_l_date = c_date
                inter_h  = c_high; inter_h_date = c_date

        # ──── SEEKING_BOTTOM（黑吞期）────
        elif phase == 'SEEKING_BOTTOM':
            if c_low  < run_l: run_l = c_low; run_l_date = c_date
            if c_high > inter_h: inter_h = c_high; inter_h_date = c_date

            if sw == 'RED':
                new_bot = {'price': run_l, 'date': run_l_date}

                # 若新底低於舊底 → 舊底到新底之間最高點成為新頂
                if conf_bot and new_bot['price'] < conf_bot['price']:
                    candidate_top_price = inter_h
                    candidate_top_date  = inter_h_date
                    if candidate_top_price > (conf_top['price'] if conf_top else -1.0):
                        conf_top = {'price': candidate_top_price, 'date': candidate_top_date}
                        # 發布更新後的頂 + 新底配對
                        push_boundary(conf_top, new_bot, c_close_ts)

                conf_bot = new_bot

                # 切換到 SEEKING_TOP
                phase    = 'SEEKING_TOP'
                run_h    = c_high; run_h_date = c_date
                inter_l  = c_low;  inter_l_date = c_date

    return boundaries


def scan_for_symbol_logic(symbol, name, precision, ohlcv_1d, ohlcv_1h, now_utc):
    import logging
    import pandas as pd
    logger = logging.getLogger("SPEED")
    try:
        def _fmt(ts_ms, fmt='%Y-%m-%d'):
            return pd.to_datetime(int(ts_ms), unit='ms', utc=True).tz_convert('Asia/Taipei').strftime(fmt)
            
        def get_status(target_ts, timeline):
            if not timeline: return True, "新幣首發", 0
            for block in timeline:
                if block['start'] <= target_ts < block['end']:
                    return block['valid'], block['date'], block['start']
            return True, "新幣首發", 0

        # Build 18D Timeline
        from main import compose_18d_bars
        ohlcv_18d = compose_18d_bars(ohlcv_1d)
        if not ohlcv_18d: return None
        df_18d = pd.DataFrame(ohlcv_18d, columns=['ts', 'open', 'high', 'low', 'close', 'vol', 'close_ts'])
        df_18d_closed = df_18d[df_18d['close_ts'] <= now_utc].reset_index(drop=True)
        l1_timeline = build_timeline(df_18d_closed)

        # Build 1D Boundaries
        df_1d = pd.DataFrame(ohlcv_1d, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
        df_1d['close_ts'] = df_1d['ts'] + 24 * 3600 * 1000
        df_1d_closed = df_1d[df_1d['close_ts'] <= now_utc].reset_index(drop=True)
        if df_1d_closed.empty: return None
        
        all_boundaries = build_1d_structure_global(df_1d_closed)

        def get_active_boundary(c_ts):
            matched = None
            for bd in all_boundaries:
                if bd['from_ts'] <= c_ts:
                    matched = bd
            return matched

        all_historical_c2s = []
        active_signal = None

        for i in range(1, len(df_1d_closed)):
            _curr = df_1d_closed.iloc[i]
            c_high     = float(_curr['high'])
            c_low      = float(_curr['low'])
            c_close    = float(_curr['close'])
            c_open     = float(_curr['open'])
            c_date     = _fmt(int(_curr['ts']))
            c_ts       = int(_curr['ts'])
            c_close_ts = int(_curr['close_ts'])

            is_l1_valid, l1_dt_str, l1_start_ts = get_status(c_close_ts, l1_timeline)

            if active_signal:
                _closed_18d = df_18d_closed[df_18d_closed['close_ts'] <= c_ts]
                if len(_closed_18d) >= 2:
                    _last_18d = _closed_18d.iloc[-1]
                    _prev_18d = _closed_18d.iloc[-2]
                    
                    if int(_last_18d['close_ts']) > active_signal['trigger_ts']:
                        _l_close = float(_last_18d['close'])
                        _l_low = float(_last_18d['low'])
                        _p_body_high = max(float(_prev_18d['open']), float(_prev_18d['close']))
                        _p_body_low = min(float(_prev_18d['open']), float(_prev_18d['close']))
                        
                        _cur_trailing = float(active_signal.get('trailing_sl', -1.0))
                        
                        if _l_close < _p_body_low:
                            _init_sl = active_signal.get('initial_sl', active_signal['stop_loss'])
                            _risk = abs(active_signal['entry_price'] - _init_sl)
                            active_signal['status'] = 'closed'
                            active_signal['exit_type'] = 'black_swallow'
                            active_signal['real_rr'] = (_p_body_low - active_signal['entry_price']) / _risk if _risk > 0 else 0.0
                            active_signal = None
                        elif _l_close > _p_body_high and _l_low > float(active_signal['entry_price']) and (_cur_trailing < 0 or _l_low > _cur_trailing):
                            active_signal['trailing_sl'] = _l_low
                            active_signal['trailing_sl_date'] = _fmt(int(_last_18d['ts']))
                            
            if active_signal:
                _trailing = active_signal.get('trailing_sl')
                _effective_sl = _trailing if _trailing is not None else active_signal['stop_loss']
                _init_sl = active_signal.get('initial_sl', active_signal['stop_loss'])
                _base_risk = abs(active_signal['entry_price'] - _init_sl)
                
                if _base_risk > 0:
                    _cur_rr = (c_high - active_signal['entry_price']) / _base_risk
                    if _cur_rr > active_signal.get('max_rr', 0.0):
                        active_signal['max_rr'] = round(_cur_rr, 2)
                        
                if c_low <= _effective_sl:
                    _risk = abs(active_signal['entry_price'] - _init_sl)
                    active_signal['status'] = 'closed'
                    active_signal['real_rr'] = (_effective_sl - active_signal['entry_price']) / _risk if _risk > 0 else 0.0
                    active_signal['exit_type'] = 'trailing_sl' if _trailing is not None else 'stop_loss'
                    active_signal = None
                elif _base_risk > 0 and c_high >= (active_signal['entry_price'] + 100 * _base_risk):
                    active_signal['status'] = 'closed'
                    active_signal['real_rr'] = 100.0
                    active_signal['exit_type'] = 'tp100'
                    active_signal = None

            if not active_signal:
                if not is_l1_valid: continue
                
                bd = get_active_boundary(c_ts)
                if not bd: continue
                
                boundary_level = bd['level']
                if c_open < boundary_level and c_close > boundary_level:
                    new_sig = {
                        'symbol': symbol,
                        'l1_18d_direction': 'LONG',
                        'l1_date': l1_dt_str,
                        'l1_open_ts': l1_start_ts,
                        'l3_date': c_date,
                        'l2_top_date': bd['t1_date'],
                        'l2_top': bd['level'],
                        'l2_bottom_date': bd['b2_date'],
                        'l2_bottom': bd['b2_level'],
                        'entry_price': c_close,
                        'stop_loss': c_low,
                        'initial_sl': c_low,
                        'trigger_ts': c_ts,
                        'precision': precision,
                        'status': 'active',
                        'has_entered': True,
                        'real_rr': 0.0,
                        'max_rr': 0.0
                    }
                    all_historical_c2s.append(new_sig)
                    active_signal = new_sig

        current_price = float(df_1d['close'].iloc[-1]) if not df_1d.empty else 0.0
        for c2 in all_historical_c2s:
            c2['current_price'] = current_price

        is_trigger_met = active_signal is not None
        current_l1_ok, current_l1_date, current_l1_ts = get_status(now_utc, l1_timeline)
        
        current_boundary = get_active_boundary(now_utc)
        if current_boundary and not current_l1_ok:
            current_boundary = None
            
        final_state = 'triggered' if is_trigger_met else ('l2_watching' if current_boundary else 'l1_waiting')
        
        cache_ts = active_signal['trigger_ts'] if active_signal else 0
        
        return {
            'symbol':                symbol,
            'action':                'update',
            'data':                  {'ts': cache_ts},
            'is_trigger_met':        is_trigger_met,
            'is_watchlist_eligible': current_l1_ok,
            'entry_price':           active_signal['entry_price'] if active_signal else 0.0,
            'stop_loss':             active_signal['stop_loss'] if active_signal else 0.0,
            'trigger_ts':            cache_ts,
            'precision':             precision,
            'l1_18d_direction':      'LONG',
            'l1_date':               active_signal['l1_date'] if active_signal else (current_l1_date if current_l1_ok else '未知'),
            'l3_date':               active_signal['l3_date'] if active_signal else '未知',
            'scan_state':            final_state,
            'historical_c2s':        all_historical_c2s,
            'l2_top':                active_signal['l2_top'] if active_signal else (current_boundary['level'] if current_boundary else -1.0),
            'l2_top_date':           active_signal['l2_top_date'] if active_signal else (current_boundary['t1_date'] if current_boundary else '未知'),
            'l2_bottom':             active_signal['l2_bottom'] if active_signal else (current_boundary['b2_level'] if current_boundary else -1.0),
            'l2_bottom_date':        active_signal['l2_bottom_date'] if active_signal else (current_boundary['b2_date'] if current_boundary else '未知'),
        }
    except Exception as e:
        logger.warning(f"掃描異常 ({symbol}): {type(e).__name__}: {e}")
        import traceback; logger.debug(traceback.format_exc())
        return None


async def process_symbol(exchange, symbol, name, precision, current_idx, total_coins, cached_info):
    try:
        if current_idx > 0 and (current_idx % 50 == 0 or current_idx == total_coins):
            logger.info(f"📊 掃描進度: {current_idx}/{total_coins}...")

        now_utc = int(time.time() * 1000)

        # 拉取 1D 資料（最近 1000 根）
        _batch_1d = await exchange.fetch_ohlcv(symbol, '1d', limit=1000)
        ohlcv_1d  = sorted({b[0]: b for b in _batch_1d}.values(), key=lambda x: x[0]) if _batch_1d else []
        if not ohlcv_1d:
            return None

        # 拉取 1H 資料（最近 500 根 ≈ 20 天，足以偵測近期 3H 突破）
        _batch_1h = await exchange.fetch_ohlcv(symbol, '1h', limit=500)
        ohlcv_1h  = sorted({b[0]: b for b in _batch_1h}.values(), key=lambda x: x[0]) if _batch_1h else []
        if not ohlcv_1h:
            return None

        return scan_for_symbol_logic(symbol, name, precision, ohlcv_1d, ohlcv_1h, now_utc)
    except Exception as e:
        logger.warning(f"掃描異常 ({symbol}): {type(e).__name__}: {e}")
        return None


async def run_history_scan_worker():
    """背景歷史掃描任務（全量 1D + 全量 1H）"""
    try:
        logger.info("🚀 背景歷史掃描任務啟動！")
        if not BITGET_API_KEY: return
        # 每次重啟強制清除舊資料，確保策略邏輯重建
        for _f in ["history_signals_full.json", "history_signals.json", "watchlist.json"]:
            _path = os.path.join(DATA_DIR, _f)
            if os.path.exists(_path):
                try:
                    os.remove(_path)
                    logger.info(f"🗑️ 已清除舊版 {_f}，重新建立中...")
                except Exception as _del_e:
                    logger.warning(f"清除舊檔案失敗 ({_f}): {_del_e}")

        ex = ccxt.bitget({
            'apiKey': BITGET_API_KEY,
            'secret': BITGET_SECRET_KEY,
            'password': BITGET_PASSWORD,
            'enableRateLimit': True,
        })

        whitelist = _get_crypto_whitelist()
        markets   = await ex.load_markets()
        coins = []
        for ccxt_sym, m in markets.items():
            if not (m.get('linear') and m.get('quote') == 'USDT' and m.get('settle') == 'USDT'): continue
            base = ccxt_sym.split('/')[0]
            if whitelist and f"{base}USDT" not in whitelist: continue
            coins.append(ccxt_sym)

        precisions = {s: max(0, int(round(-math.log10(markets[s].get('precision', {}).get('price', 1e-8))))) for s in coins}

        all_past_events = []
        total = len(coins)
        for idx, sym in enumerate(coins):
            try:
                now_utc = int(time.time() * 1000)

                # 全量 1D（最多 30 頁）
                ohlcv_1d = []
                _end = now_utc
                for _pg in range(30):
                    if _end < 1514764800000: break
                    _b = await ex.fetch_ohlcv(sym, '1d', limit=1000, params={'until': int(_end)})
                    if not _b: break
                    ohlcv_1d.extend(_b)
                    _end = _b[0][0] - 1
                    await asyncio.sleep(0.15)
                ohlcv_1d = sorted({b[0]: b for b in ohlcv_1d}.values(), key=lambda x: x[0])

                # 全量 1H（最多 50 頁 ≈ 5.7 年歷史）
                ohlcv_1h = []
                _end = now_utc
                for _pg in range(50):
                    if _end < 1514764800000: break
                    _b = await ex.fetch_ohlcv(sym, '1h', limit=1000, params={'until': int(_end)})
                    if not _b: break
                    ohlcv_1h.extend(_b)
                    _end = _b[0][0] - 1
                    await asyncio.sleep(0.15)
                ohlcv_1h = sorted({b[0]: b for b in ohlcv_1h}.values(), key=lambda x: x[0])

                if not ohlcv_1d or not ohlcv_1h:
                    continue

                res = scan_for_symbol_logic(sym, get_base_coin(sym), precisions.get(sym, 4), ohlcv_1d, ohlcv_1h, now_utc)
                if res and 'historical_c2s' in res:
                    all_past_events.extend(res['historical_c2s'])

            except Exception as e:
                logger.error(f"歷史掃描異常 ({sym}): {e}")
            await asyncio.sleep(0.2)
            if idx % 5 == 0:
                logger.info(f"⏳ 歷史掃描進度: {idx}/{total}")

        HISTORY_FULL_FILE = os.path.join(DATA_DIR, "history_signals_full.json")
        try:
            with open(HISTORY_FULL_FILE, 'w', encoding='utf-8') as f:
                json.dump(all_past_events, f, ensure_ascii=False, indent=2)
            logger.info("✅ 背景歷史掃描任務完成並已寫入 history_signals_full.json")
        except Exception as e:
            logger.error(f"寫入歷史報表失敗: {e}")

        await ex.close()
    except Exception as e:
        logger.error(f"歷史掃描整體異常: {e}")

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


async def run_scan(ex=None):
    logger.info("⏰ 開始執行 極速系統...")
    _local_ex = False
    if ex is None:
        ex = get_exchange()
        _local_ex = True
    try:
        watchlist = load_watchlist()
        config = load_config()
        default_loss = config.get("total_capital", 300) * config.get("loss_pct", 2) / 100
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
            # scanned_coins.json 已廢除，不再額外儲存

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
            
            precisions = {s: max(0, int(round(-math.log10(markets[s].get('precision', {}).get('price', 1e-8))))) for s in coins}
        except Exception as e:
            logger.error(f"拉取市場資料失敗: {e}")
            coins = ["BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT", "XRP/USDT:USDT", "DOGE/USDT:USDT", "ADA/USDT:USDT"]
            precisions = {s: 4 for s in coins}; precisions.update({"BTC/USDT:USDT": 2, "ETH/USDT:USDT": 2})

        existing_positions = []
        if BITGET_API_KEY:
            try:
                positions = await ex.fetch_positions()
                existing_positions = [p for p in positions if float(p.get('contracts', 0) or p.get('size', 0)) > 0]
                # 以真實交易所倉位更新 holdings.json，供前端區分「持倉中」vs「掛單中」
                save_holdings([get_base_coin(p['symbol']) for p in existing_positions])
            except Exception as e:
                logger.warning(f"拉取持倉列表失敗 (下單防重複查詢): {e}")

        all_results = []
        all_past_events = []
        latest_l1_open_ts_map = {}
        total_coins = len(coins)
        for i in range(0, total_coins, 20):
            batch = coins[i:i+20]
            tasks = [process_symbol(ex, s, get_base_coin(s), precisions[s], i + idx + 1, total_coins, watchlist.get(s)) for idx, s in enumerate(batch)]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for res in results:
                if isinstance(res, Exception):
                    logger.warning(f"掃描幣種並發異常: {res}")
                    continue
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
                    l2_date = s.get('l2_date', '')
                    if not l2_date and ts > 0:
                        l2_date = pd.to_datetime(ts, unit='ms', utc=True).tz_convert('Asia/Taipei').strftime('%Y-%m-%d %H:%M:%S')
                    holding_map[sym] = {
                        'symbol':        sym,
                        'l2_direction':  s.get('direction', ''),
                        'entry_price':   s.get('entry_price', 0.0),
                        'stop_loss':     s.get('original_sl_price', s.get('sl_price', 0.0)),
                        'precision':     s.get('precision', 4),
                        'trigger_ts':    ts,
                        'l1_18d_direction': s.get('l1_18d_direction', ''),
                        'l1_date':       s.get('l1_date', ''),
                        'l2_date':       l2_date,
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

        # 同向加倉冗餘邏輯已移除

        for item in all_results:
            sym = item['symbol']
            
            if sym in holding_items_dict:
                continue
                
            if item.get('is_trigger_met'):
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
                    ex, sym, item.get('l2_direction', 'LONG'), item['entry_price'], item['stop_loss'],
                    item['precision'], default_loss, item.get('trigger_ts', 0),
                    l1_18d_direction=item.get('l1_18d_direction', ''), l1_date=item.get('l1_date', ''), l2_date=item.get('l2_date', '')
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

        # 將追蹤名單的顯示資料 (頂底、日期) 直接合併進 watchlist 條目，不再單獨存 waiting_signals.json
        for res in all_results:
            if res.get('is_watchlist_eligible'):
                sym = res['symbol']
                if sym in watchlist:
                    l2_top_val = res.get('l2_top', 0)
                    l2_bot_val = res.get('l2_bottom', 0)
                    if l2_bot_val == float('inf'): l2_bot_val = 'inf'
                    if l2_top_val == float('inf'): l2_top_val = 'inf'
                    watchlist[sym]['l1_date']        = res.get('l1_date', '')
                    watchlist[sym]['l2_date']        = res.get('l2_date', '')
                    watchlist[sym]['l2_top']         = l2_top_val
                    watchlist[sym]['l2_top_date']    = res.get('l2_top_date', '')
                    watchlist[sym]['l2_bottom']      = l2_bot_val
                    watchlist[sym]['l2_bottom_date'] = res.get('l2_bottom_date', '')
                    watchlist[sym]['current_price']  = res.get('current_price', 0)
        save_watchlist(watchlist)
        
        history_signals = load_history_signals()

        # 清除所有不屬於最新 L1 區間的舊紀錄
        for base, current_l1_ts in latest_l1_open_ts_map.items():
            if base in history_signals and current_l1_ts > 0:
                keep_sigs = []
                for s in history_signals[base]:
                    s_l1_ts = s.get('l1_open_ts', 0)
                    if s_l1_ts >= current_l1_ts:
                        keep_sigs.append(s)
                history_signals[base] = keep_sigs

        # 只記錄掃描器推演出的 C2 事件，與實際倉位無關
        all_triggered = all_past_events + real_new_triggers + missed_items
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
                
                # 同步歷史推演的動態數值 (止損被推升時，RR 與 SL 需要更新)
                if 'real_rr' in item:
                    existing['real_rr'] = item['real_rr']
                if 'trailing_sl' in item:
                    existing['trailing_sl'] = item['trailing_sl']
                if 'trailing_sl_date' in item:
                    existing['trailing_sl_date'] = item['trailing_sl_date']
                    
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
        logger.info(f"✅ 掃描完成。新觸發: {len(real_new_triggers)} / 持倉: {len(real_holding_items)} / 掛單: {len(pending_items)} / 未上車: {len(missed_items)} / 追蹤總數: {active_count}")

        if not real_new_triggers and not holding_items and not missed_items:
            send_telegram_message(f"✅ <b>條件掃描完成</b>\n本次共掃描 {total_coins} 個幣種，無滿足條件標的。\n(當前追蹤觸發訊號: {active_count} 個)")

        if real_new_triggers or holding_items or missed_items:
            send_system_settings_message(config)
    finally:
        if _local_ex and ex:
            await ex.close()
            # 讓 aiohttp 徹底釋放 TCP connection，避免 Unclosed connector 警告
            await asyncio.sleep(0.1)

async def scheduler():
    # 以日期追蹤，避免 last_hour=0 跨日不重置導致每天午夜只能觸發一次的 Bug
    last_scan_date = None
    
    global_ex = get_exchange()
    try:
        try:
            await run_scan(global_ex)
            last_scan_date = datetime.utcnow().date()
        except Exception as e:
            logger.error(f"初始掃描異常: {e}")

        while True:
            try:
                now = datetime.utcnow()
                today = now.date()
                if now.hour == 0 and now.minute <= 10 and today != last_scan_date:
                    try:
                        await run_scan(global_ex)
                    except Exception as e:
                        logger.error(f"定時掃描異常: {e}")
                    last_scan_date = today

                if BITGET_API_KEY:
                    try:
                        await monitor_positions(global_ex)
                    except Exception as e:
                        logger.error(f"監控週期異常: {e}")

            except Exception as e:
                logger.critical(f"💥 Scheduler 頂層異常 (已攔截): {e}")
            await asyncio.sleep(60)
            
    finally:
        if global_ex:
            await global_ex.close()
            await asyncio.sleep(0.25)

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
  .status-badge.active   { background: rgba(63,185,80,0.1);   color: #3fb950; border: 1px solid rgba(63,185,80,0.25); }
  .status-badge.closed   { background: rgba(248,81,73,0.1);   color: #f85149; border: 1px solid rgba(248,81,73,0.25); }
  .status-badge.missed   { background: rgba(63,185,80,0.1);   color: #3fb950; border: 1px solid rgba(63,185,80,0.25); }
  .status-badge.triggered{ background: rgba(88,166,255,0.1);  color: #58a6ff; border: 1px solid rgba(88,166,255,0.25); }
  /* 止盈 = 持倉中色系 (綠); 保護止損 = 掛單中色系 (黃) */
  .status-badge.tp       { background: rgba(63,185,80,0.15);  color: #3fb950; border: 1px solid rgba(63,185,80,0.40); }
  .status-badge.trail-sl { background: rgba(210,153,34,0.15); color: #d29922; border: 1px solid rgba(210,153,34,0.40); }
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
  let currentView = 'signals';
  let allData = {};
  let allSymbols = [];
  let priceMap = {};
  let allHoldings = [];
  let allActiveSignals = [];
  let allWaitingSignals = {};

  function switchTab(tab) {
    currentView = tab;
    currentSymbol = null;
    updateStats();
    renderSidebar(document.getElementById('search-input').value);
    if (tab === 'signals') renderHome();
    else if (tab === 'watchlist') renderWatchlistHome();
  }

  function updateStats() {
    if (currentView === 'signals') {
      let activeSigs = 0, closedSigs = 0, totalRR = 0;
      let rrCounts = { 10:0, 20:0, 30:0, 40:0, 50:0, 60:0, 70:0, 80:0, 90:0, 100:0 };
      Object.values(allData).forEach(sigs => {
        sigs.forEach(s => {
          const maxRR = s.max_rr ? parseFloat(s.max_rr) : 0;
          if (maxRR >= 100) rrCounts[100]++;
          else if (maxRR >= 90) rrCounts[90]++;
          else if (maxRR >= 80) rrCounts[80]++;
          else if (maxRR >= 70) rrCounts[70]++;
          else if (maxRR >= 60) rrCounts[60]++;
          else if (maxRR >= 50) rrCounts[50]++;
          else if (maxRR >= 40) rrCounts[40]++;
          else if (maxRR >= 30) rrCounts[30]++;
          else if (maxRR >= 20) rrCounts[20]++;
          else if (maxRR >= 10) rrCounts[10]++;

          if (s.status === 'closed') { 
            closedSigs++; 
            totalRR += s.real_rr !== undefined ? parseFloat(s.real_rr) : -1.0; 
          }
          else if (s.status === 'active' || s.status === 'missed') {
            activeSigs++;
            const entry = parseFloat(s.entry_price), sl = parseFloat(s.stop_loss);
            const cp = parseFloat(s.current_price || entry);
            if (entry > 0 && sl > 0 && Math.abs(entry - sl) > 0) {
              const risk = Math.abs(entry - sl);
              const sdir = s.l2_direction || 'LONG';
              totalRR += sdir === 'LONG' ? (cp - entry) / risk : (entry - cp) / risk;
            }
          }
        });
      });
      const totalSigs = activeSigs + closedSigs;
      const winRate = totalSigs > 0 ? ((totalSigs - closedSigs) / totalSigs * 100).toFixed(1) : 0;
      const rrText = totalRR >= 0 ? `+${totalRR.toFixed(2)}` : `${totalRR.toFixed(2)}`;
      const rrColor = totalRR >= 0 ? '#3fb950' : '#f85149';
      
      let rrStatsHtml = Object.keys(rrCounts).sort((a,b)=>parseInt(a)-parseInt(b)).map(k => {
        if (rrCounts[k] > 0) return `<span style="display:inline-block; margin-right:8px; padding:2px 6px; background:rgba(63,185,80,0.1); color:#3fb950; border-radius:4px; font-size:0.75rem;">${k}R: ${rrCounts[k]}筆</span>`;
        return '';
      }).join('');

      document.getElementById('global-stats').innerHTML = `
        <div style="width: 100%; display: flex; gap: 15px; flex-wrap: wrap; margin-bottom: 8px;">
          <span>📊 總訊號數：<strong style="color:#58a6ff">${totalSigs}</strong></span>
          <span>❌ 止損：<strong style="color:#f85149">${closedSigs}</strong></span>
          <span>🏆 勝率：<strong style="color:#3fb950">${winRate}%</strong></span>
          <span>💰 訊號總 RR：<strong style="color:${rrColor}">${rrText}</strong></span>
        </div>
        ${rrStatsHtml ? `<div style="width: 100%; display: flex; gap: 4px; flex-wrap: wrap;">🎯 目標達成：${rrStatsHtml}</div>` : ''}
      `;
    }   }

  async function fetchData() {
    try {
      const res = await fetch('/api/data');
      const json = await res.json();
      allData = json.history || {};
      allSymbols = json.watchlist || [];
      priceMap = json.price_map || {};
      allHoldings = json.holdings || [];
      allActiveSignals = json.active_signals || [];
      allWaitingSignals = json.waiting_signals || {};

      Object.entries(allData).forEach(([base, sigs]) => {
        const cp = priceMap[base];
        if (cp) sigs.forEach(s => { if (!s.current_price) s.current_price = cp; s._base = base; });
        else sigs.forEach(s => { s._base = base; });
      });

      updateStats();
      renderSidebar(document.getElementById('search-input').value);
      if (currentSymbol) renderMain(currentSymbol);
      else if (currentView === 'watchlist') renderWatchlistHome();
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

    const allSet = Object.keys(allData).filter(k => allData[k].length > 0).sort();
    const filtered = q ? allSet.filter(s => s.includes(q)) : allSet;

    let html = '';
    if (!q) {
      const sigsActive  = currentView === 'signals'   && currentSymbol === null ? 'active' : '';
      const watchActive = currentView === 'watchlist' && currentSymbol === null ? 'active' : '';
      html += `<div class="symbol-item ${sigsActive}" onclick="switchTab('signals')"><span>📡 訊號總覽</span></div>`;
      html += `<div class="symbol-item ${watchActive}" onclick="switchTab('watchlist')"><span>👀 追蹤名單</span></div>`;
      html += `<div style="height: 1px; background: #21262d; margin: 8px 0;"></div>`;
    }

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
      container.innerHTML = `<div class="empty-state"><div class="icon">🔍</div><p>目前尚無有效訊號</p></div>`;
      return;
    }

    activeSigs.sort((a, b) => (b.trigger_ts || 0) - (a.trigger_ts || 0));

    let html = '';
    activeSigs.forEach((sig, idx) => {
      const dir = sig.l2_direction || 'LONG';
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
      // 掛單中且已有保護止損 → 顯示「未上車」樣式 (灰色，語意貼近 missed)
      const hasTrailSL = sig.trailing_sl != null;
      let badge = '';
      if (sig.status === 'active') {
        if (allHoldings.includes(sig._base)) {
          badge = '<span class="dir-badge LONG">持倉中</span>';
        } else if (hasTrailSL) {
          badge = '<span class="dir-badge" style="color:#6e7681;background:rgba(110,118,129,0.1);border-color:rgba(110,118,129,0.3);">未上車</span>';
        } else {
          badge = '<span class="dir-badge" style="color:#d29922;background:rgba(210,153,34,0.1);border-color:rgba(210,153,34,0.4);">掛單中</span>';
        }
      }
      html += `
      <div class="signal-card ${dir} active" style="cursor:pointer" onclick="selectSymbol('${sig._base}')">
        <div class="card-header">
          <div class="card-title">
            <span style="color:#58a6ff;font-weight:600;font-size:0.95rem;">${sig._base}</span>
            ${badge}
            <span style="font-size:0.85rem;font-weight:700;color:${rrCol};margin-left:auto;">${rrStr}</span>
          </div>
        </div>
        <div class="card-grid">
          <div class="detail-block">
            <div class="detail-label">18D紅吞時間</div>
            <div class="detail-value">${sig.l1_date || '—'}</div>
          </div>
          <div class="detail-block">
            <div class="detail-label">1D突破時間</div>
            <div class="detail-value">${fmt(sig.l3_date)}</div>
          </div>
          <div class="detail-block">
            <div class="detail-label">1D頂點時間</div>
            <div class="detail-value">${sig.l2_top > 0 ? (sig.l2_top_date||'—') : '—'}</div>
          </div>
          <div class="detail-block">
            <div class="detail-label">界線價格(頂)</div>
            <div class="detail-value">${sig.l2_top > 0 ? fmt(sig.l2_top, prec) : '—'}</div>
          </div>
          <div class="detail-block">
            <div class="detail-label">1D底點時間</div>
            <div class="detail-value">${sig.l2_bottom_date || '—'}</div>
          </div>
          <div class="detail-block">
            <div class="detail-label">底點價格</div>
            <div class="detail-value">${sig.l2_bottom > 0 && sig.l2_bottom != Infinity ? fmt(sig.l2_bottom, prec) : '—'}</div>
          </div>
          <div class="detail-block">
            <div class="detail-label">曾達最高RR</div>
            <div class="detail-value" style="color:${(sig.max_rr||0)>=1?'#3fb950':'#8b949e'}">${sig.max_rr != null ? '+' + Number(sig.max_rr||0).toFixed(2) + 'R' : '—'}</div>
          </div>
        </div>
        <div class="card-grid" style="margin-top: 12px;">
          <div class="detail-block">
            <div class="detail-label">進場價格</div>
            <div class="detail-value price">${fmt(sig.entry_price, prec)}</div>
          </div>
          <div class="detail-block">
            <div class="detail-label">止損價格</div>
            <div class="detail-value sl">${fmt(sig.stop_loss, prec)}</div>
          </div>
          ${sig.trailing_sl ? `
          <div class="detail-block" style="border: 1px solid #d29922; border-radius: 4px; padding: 4px; background: rgba(210,153,34,0.1);">
            <div class="detail-label" style="color: #d29922;">保護止損價格</div>
            <div class="detail-value sl" style="color: #d29922;">${fmt(sig.trailing_sl, prec)} <span style="font-size:0.75rem">(${sig.trailing_sl_date || '已觸發'})</span></div>
          </div>` : ''}
        </div>
      </div>`;
    });
    container.innerHTML = html;
  }

  function selectSymbol(sym) {
    currentView = 'signals';
    currentSymbol = sym;
    updateStats();
    renderSidebar(document.getElementById('search-input').value);
    renderMain(sym);
  }

  function fmt(val, decimals) {
    if (val == null || val === '' || val === undefined) return '—';
    if (decimals != null) return parseFloat(val).toFixed(decimals);
    return val;
  }

  function renderMain(sym) {
    document.getElementById('header-title').textContent = `💎 ${sym}　訊號歷史紀錄`;
    const container = document.getElementById('signal-container');
    const signals = allData[sym] || [];

    if (signals.length === 0) {
      container.innerHTML = `<div class="empty-state"><div class="icon">🔍</div><p>此幣種尚無觸發訊號紀錄</p></div>`;
      return;
    }

    // 從舊到新排列
    const sorted = [...signals].sort((a, b) => (a.trigger_ts || 0) - (b.trigger_ts || 0));

    let html = '';
    sorted.forEach((sig, idx) => {
      const dir = sig.l2_direction || 'LONG';
      const status = sig.status || 'unknown';
      const dirText = dir === 'LONG' ? '▲ LONG' : '▼ SHORT';
      const statusMap = { active: '有效', closed: '止損', missed: '有效', triggered: '歷史紀錄' };
      let statusText = statusMap[status] || status;
      const prec = sig.precision || 4;
      // active: 持倉中(綠) / 掛單中有保護止損(灰=未上車) / 掛單中(黃)
      let badge = '';
      if (status === 'active') {
        if (allHoldings.includes(sym)) {
          badge = '<span class="dir-badge LONG">持倉中</span>';
        } else if (sig.trailing_sl != null) {
          badge = '<span class="dir-badge" style="color:#6e7681;background:rgba(110,118,129,0.1);border-color:rgba(110,118,129,0.3);">未上車</span>';
        } else {
          badge = '<span class="dir-badge" style="color:#d29922;background:rgba(210,153,34,0.1);border-color:rgba(210,153,34,0.4);">掛單中</span>';
        }
      }
      const entry = parseFloat(sig.entry_price);
      const sl = parseFloat(sig.stop_loss);   // 原始止損，用於即時 RR 計算
      const cp = parseFloat(sig.current_price || entry);
      let rrStr = '—', rrCol = '#8b949e';
      // exitType 決定 statusText & badge 色系
      let exitBadgeClass = 'closed';
      if (status === 'closed') {
        let r_val = sig.real_rr !== undefined ? parseFloat(sig.real_rr) : -1.0;
        rrStr = (r_val >= 0 ? '+' : '') + r_val.toFixed(2) + 'R';
        rrCol = r_val > 0 ? '#3fb950' : '#f85149';
        const exitType = sig.exit_type || '';
        if (exitType === 'tp100') {
          statusText = '止盈';
          exitBadgeClass = 'tp';       // 綠色 → 跟持倉中同色系
        } else if (exitType === 'trailing_sl' || r_val > 0) {
          statusText = '保護止損';
          exitBadgeClass = 'trail-sl'; // 黃色 → 跟掛單中同色系
        } else {
          statusText = '止損';
          exitBadgeClass = 'closed';   // 紅色
        }
      } else if (entry > 0 && sl > 0 && Math.abs(entry - sl) > 0) {
        const risk = Math.abs(entry - sl);
        let rr = dir === 'LONG' ? (cp - entry) / risk : (entry - cp) / risk;
        rrStr = (rr >= 0 ? '+' : '') + rr.toFixed(2) + 'R';
        rrCol = rr >= 0 ? '#3fb950' : '#f85149';
      }

      html += `
      <div class="signal-card ${dir} ${status}">
        <div class="card-header">
          <div class="card-title">
            <span style="color:#6e7681;font-size:0.75rem;">#${idx + 1}</span>
            ${(status === 'closed' || status === 'triggered') ? `<span class="status-badge ${exitBadgeClass}">${statusText}</span>` : ''}
            ${badge}
            <span style="font-size:0.85rem;font-weight:700;color:${rrCol};margin-left:auto;">${rrStr}</span>
          </div>
        </div>
        <div class="card-grid">
          <div class="detail-block">
            <div class="detail-label">18D紅吞時間</div>
            <div class="detail-value">${sig.l1_date || '—'}</div>
          </div>
          <div class="detail-block">
            <div class="detail-label">1D突破時間</div>
            <div class="detail-value">${fmt(sig.l3_date)}</div>
          </div>
          <div class="detail-block">
            <div class="detail-label">1D頂點時間</div>
            <div class="detail-value">${sig.l2_top > 0 ? (sig.l2_top_date||'—') : '—'}</div>
          </div>
          <div class="detail-block">
            <div class="detail-label">界線價格(頂)</div>
            <div class="detail-value">${sig.l2_top > 0 ? fmt(sig.l2_top, prec) : '—'}</div>
          </div>
          <div class="detail-block">
            <div class="detail-label">1D底點時間</div>
            <div class="detail-value">${sig.l2_bottom_date || '—'}</div>
          </div>
          <div class="detail-block">
            <div class="detail-label">底點價格</div>
            <div class="detail-value">${sig.l2_bottom > 0 && sig.l2_bottom != Infinity ? fmt(sig.l2_bottom, prec) : '—'}</div>
          </div>
          <div class="detail-block">
            <div class="detail-label">曾達最高RR</div>
            <div class="detail-value" style="color:${(sig.max_rr||0)>=1?'#3fb950':'#8b949e'}">${sig.max_rr != null ? '+' + Number(sig.max_rr||0).toFixed(2) + 'R' : '—'}</div>
          </div>
        </div>
        <div class="card-grid" style="margin-top: 12px;">
          <div class="detail-block">
            <div class="detail-label">進場價格</div>
            <div class="detail-value price">${fmt(sig.entry_price, prec)}</div>
          </div>
          <div class="detail-block">
            <div class="detail-label">止損價格</div>
            <div class="detail-value sl">${fmt(sig.stop_loss, prec)}</div>
          </div>
          ${sig.trailing_sl ? `
          <div class="detail-block" style="border: 1px solid #d29922; border-radius: 4px; padding: 4px; background: rgba(210,153,34,0.1);">
            <div class="detail-label" style="color: #d29922;">保護止損價格</div>
            <div class="detail-value sl" style="color: #d29922;">${fmt(sig.trailing_sl, prec)} <span style="font-size:0.75rem">(${sig.trailing_sl_date || '已觸發'})</span></div>
          </div>` : ''}
        </div>
      </div>`;
    });
    container.innerHTML = html;
  }

  
  function renderWatchlistHome() {
    document.getElementById('header-title').textContent = '👀 追蹤中名單 (等待突破)';
    document.getElementById('global-stats').innerHTML = `<span>⏳ 總計：<strong style="color:#58a6ff">${Object.keys(allWaitingSignals).length}</strong></span>`;
    const container = document.getElementById('signal-container');

    if (Object.keys(allWaitingSignals).length === 0) {
      container.innerHTML = `<div class="empty-state"><div class="icon">👀</div><p>目前無追蹤中幣種</p></div>`;
      return;
    }

    let html = '';
    // 將 waitingSignals 轉換為陣列，過濾掉沒有 L1 date 的（雖然不該發生），然後依據 L1 date 由近到遠排序
    let sortedSigs = Object.values(allWaitingSignals).sort((a, b) => {
        let da = new Date(a.l1_date || 0);
        let db = new Date(b.l1_date || 0);
        return db - da; // 新的在上面
    });
    
    sortedSigs.forEach(sig => {
      const base = sig.symbol.split('/')[0];
      const topStr = sig.l2_top > 0 ? `${sig.l2_top_date} (${fmt(sig.l2_top, 4)})` : '—';
      const botStr = sig.l2_bottom !== null && sig.l2_bottom !== 'inf' && sig.l2_bottom !== 'Infinity' && sig.l2_bottom < 9999999 ? `${sig.l2_bottom_date} (${fmt(sig.l2_bottom, 4)})` : '—';
      html += `
      <div class="signal-card LONG active">
        <div class="card-header">
          <div class="card-title">
            <span style="color:#58a6ff;font-weight:600;font-size:0.95rem;">${base}</span>
          </div>
        </div>
        <div class="card-grid">
          <div class="detail-block">
            <div class="detail-label">18D紅吞時間</div>
            <div class="detail-value">${sig.l1_date || '—'}</div>
          </div>
          <div class="detail-block">
            <div class="detail-label">1D頂點時間</div>
            <div class="detail-value">${topStr}</div>
          </div>
          <div class="detail-block">
            <div class="detail-label">1D底點確立(B2)</div>
            <div class="detail-value">${sig.l2_date || '—'}</div>
          </div>
          <div class="detail-block">
            <div class="detail-label">界線價格</div>
            <div class="detail-value">${sig.l2_top > 0 ? fmt(sig.l2_top, 4) : '—'}</div>
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



def sanitize_for_json(obj):
    if isinstance(obj, float):
        if math.isinf(obj) or math.isnan(obj):
            return str(obj)
        return obj
    elif isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_for_json(v) for v in obj]
    return obj

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

    # ── 合併背景歷史掃描結果 (history_signals_full.json) ─────────────────────
    _HISTORY_FULL_FILE = os.path.join(DATA_DIR, "history_signals_full.json")
    if os.path.exists(_HISTORY_FULL_FILE):
        try:
            with open(_HISTORY_FULL_FILE, 'r', encoding='utf-8') as _f:
                _full_list = json.load(_f)
            for _sig in _full_list:
                _base = get_base_coin(_sig.get('symbol', ''))
                if not _base: continue
                if not _sig.get('entry_price') or float(_sig.get('entry_price', 0)) == 0: continue
                if not _sig.get('trigger_ts') or int(_sig.get('trigger_ts', 0)) == 0: continue
                _ts = _sig.get('trigger_ts')
                if _base not in cleaned_history:
                    cleaned_history[_base] = []
                _existing = next((s for s in cleaned_history[_base] if s.get('trigger_ts') == _ts), None)
                if not _existing:
                    cleaned_history[_base].append(_sig)
        except Exception as _e:
            pass  # 歷史全量檔讀取失敗不影響主流程

    # ── 快速補丁：歷史訊號若 status='active' 但現價已低於止損，直接標記已止損 ──
    _now_ms = int(time.time() * 1000)
    for _base, _sigs in cleaned_history.items():
        for _sig in _sigs:
            if _sig.get('status') != 'active': continue
            _cp = float(_sig.get('current_price') or 0)
            _sl = float(_sig.get('trailing_sl') or _sig.get('stop_loss') or 0)
            _ep = float(_sig.get('entry_price') or 0)
            _isl = float(_sig.get('initial_sl') or _sig.get('stop_loss') or 0)
            if _cp <= 0 or _sl <= 0 or _ep <= 0: continue
            _risk = abs(_ep - _isl)
            if _cp <= _sl:
                _sig['status'] = 'closed'
                _sig['exit_type'] = 'trailing_sl' if _sig.get('trailing_sl') else 'stop_loss'
                _sig['real_rr'] = round((_sl - _ep) / _risk, 2) if _risk > 0 else 0.0
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

    # 側欄只顯示「有訊號紀錄」或「在追蹤名單中」的幣種，去除無意義的掃描雜訊
    watchlist_coins = sorted(set(
        [get_base_coin(k) for k in watchlist.keys()] +
        list(cleaned_history.keys())
    ))
    # holdings 讀取真實交易所倉位 (由 monitor_positions 每分鐘更新)
    holdings = load_holdings()

    # 整理 active_signals 供前端持倉總覽使用
    active_list = []
    for slist in active_sigs.values():
        for s in slist:
            if s.get('status') == 'active':
                base = get_base_coin(s['symbol'])
                active_list.append({
                    '_base': base,
                    'symbol': s['symbol'],
                    'direction': s.get('direction', ''),
                    'entry_price': s.get('entry_price', 0),
                    'sl_price': s.get('sl_price', 0),
                    'original_sl_price': s.get('original_sl_price', 0),
                    'precision': s.get('precision', 4),
                    'l1_date': s.get('l1_date', ''),
                    'l2_date': s.get('l2_date', ''),
                    'l1_18d_direction': s.get('l1_18d_direction', ''),
                    'timestamp': s.get('timestamp', 0),
                })

    # waiting_signals 從 watchlist 即時提取（已在掃描時合併進去）
    waiting_signals = {}
    for sym, wdata in watchlist.items():
        base = get_base_coin(sym)
        if 'l2_top' in wdata:
            waiting_signals[base] = {
                'symbol': sym,
                'l1_date':        wdata.get('l1_date', ''),
                'l2_date':        wdata.get('l2_date', ''),
                'l2_top':         wdata.get('l2_top', 0),
                'l2_top_date':    wdata.get('l2_top_date', ''),
                'l2_bottom':      wdata.get('l2_bottom', 0),
                'l2_bottom_date': wdata.get('l2_bottom_date', ''),
                'current_price':  wdata.get('current_price', 0),
            }

    data_to_return = {"watchlist": watchlist_coins, "history": cleaned_history, "price_map": price_map, "holdings": holdings, "active_signals": active_list, "waiting_signals": waiting_signals}
    return jsonify(sanitize_for_json(data_to_return))

def run_flask():
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, use_reloader=False)

# = [啟動模組] ===============================================================

def run_background_system():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.create_task(run_history_scan_worker())
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
