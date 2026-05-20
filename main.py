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
import uuid
import re
import json
from flask import Flask
from datetime import datetime
from dotenv import load_dotenv
import sys
import io
import tw_scanner

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

BITGET_API_KEY = os.getenv("BITGET_API_KEY", "")
BITGET_SECRET_KEY = os.getenv("BITGET_API_SECRET", "") or os.getenv("BITGET_SECRET_KEY", "")
BITGET_PASSWORD = os.getenv("BITGET_API_PASSWORD", "") or os.getenv("BITGET_PASSWORD", "")

# 無限階梯 TP：每 5R 平掉剩餘倉位的 20%
TP_STEP_R = 5
TP_CLOSE_PCT = 0.20


logger.info(f"✅ 系統配置檢查: TG_TOKEN={'已設定' if TG_BOT_TOKEN else '未設定'}, TG_CHAT_ID={'已設定' if TG_CHAT_ID else '未設定'}")
logger.info(f"✅ 交易所配置檢查: API_KEY={'已設定' if BITGET_API_KEY else '未設定'}")

# ============================================================================
# 狀態持久化 (Watchlist + Active Signals)
# ============================================================================

DATA_DIR = "/app/data"
WATCHLIST_FILE = os.path.join(DATA_DIR, "watchlist.json")
ACTIVE_SIGNALS_FILE = os.path.join(DATA_DIR, "active_signals.json")

def ensure_data_dir():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
    if not os.path.exists(WATCHLIST_FILE):
        with open(WATCHLIST_FILE, 'w') as f:
            json.dump({}, f)
    if not os.path.exists(ACTIVE_SIGNALS_FILE):
        with open(ACTIVE_SIGNALS_FILE, 'w') as f:
            json.dump({}, f)

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

# ============================================================================
# 系統設定持久化
# ============================================================================

CONFIG_FILE = os.path.join(DATA_DIR, "system_config.json")

def load_config():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                config = json.load(f)
            return config
        except Exception as e:
            logger.error(f"讀取設定檔失敗: {e}")
    return {"default_loss_amount": 6, "default_tw_loss_amount": 300}

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
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TG_CHAT_ID, "text": message, "parse_mode": "HTML"}
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
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
    if BITGET_API_KEY and BITGET_SECRET_KEY:
        exchange_config['apiKey'] = BITGET_API_KEY
        exchange_config['secret'] = BITGET_SECRET_KEY
        if BITGET_PASSWORD:
            exchange_config['password'] = BITGET_PASSWORD
    return ccxt.bitget(exchange_config)

# ============================================================================
# 3D K 棒合成工具
# ============================================================================

def compose_3d_bars(ohlcv_1d):
    """將 1D OHLCV 合成 3D K 棒 (按每年 1/1 起算，保持加密貨幣原有正常邏輯)
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

    # 合成 3D 棒
    result = []
    sorted_gts = sorted(groups.keys())
    for i, gts in enumerate(sorted_gts):
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
    """將 1D OHLCV 合成 18D K棒 (按每年 1/1 起算，與 compose_3d_bars 對齊邏輯相同)
    輸入: [[ts, open, high, low, close, vol], ...]
    輸出: [[ts, open, high, low, close, vol, close_ts], ...]
    """
    if not ohlcv_1d or len(ohlcv_1d) < 18:
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

async def place_order(exchange, symbol, direction, entry, sl, precision, fixed_loss_usdt, trigger_ts):
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
                    # Debug: 輸出 info 結構供排查
                    if isinstance(raw_info, dict):
                        logger.debug(f"  balance info keys: {list(raw_info.keys())}")
                        info_data = raw_info.get('data', [])
                    elif isinstance(raw_info, list):
                        info_data = raw_info
                    else:
                        info_data = []

                    # data 可能是 list 或 dict
                    search_list = info_data if isinstance(info_data, list) else [info_data] if isinstance(info_data, dict) else []
                    for acct in search_list:
                        if not isinstance(acct, dict):
                            continue
                        if acct.get('marginCoin', '').upper() == 'USDT':
                            available = float(acct.get('crossedMaxAvailable', 0))
                            logger.info(f"  全倉可用保證金 (crossedMaxAvailable): {available:.2f} USDT")
                            break

                    # 備選：從幣種層級的 info 取值
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
                if actual_risk < fixed_loss_usdt * 0.9:
                    msg = f"⚠️ 資金不足以建立標準部位 ({actual_risk:.2f}/{fixed_loss_usdt})\n可用餘額: {available:.2f} USDT"
                    logger.warning(msg)
                    send_telegram_message(f"<b>⚠️ 資金不足</b>\n{get_base_coin(symbol)}\n可用: {available:.2f} USDT")
                    break

                side = 'buy' if direction == 'LONG' else 'sell'
                signal_id = f"3d_{uuid.uuid4().hex[:8]}"
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
                            # 改以市價單下單，數量不變，風險只會變小
                            order = await exchange.create_order(symbol, 'market', side, qty, None, params=params)
                            entry = current_p  # 更新 entry 為當前市價，確保後續紀錄與停利計算正確
                        else:
                            raise e # 若不在區間內，拋出給外層的策略降級機制
                    except Exception as inner_e:
                        raise inner_e # 將錯誤拋出給外層

                logger.info(f"✅ 下單成功: {symbol} @ {entry} (ID: {signal_id}, Strat: {strategy_name}, Lev: {leverage}x)")

                signals = load_active_signals()
                key = f"{get_base_coin(symbol)}_{direction}"
                if key not in signals:
                    signals[key] = []
                signals[key].append({
                    'signal_id': signal_id, 'symbol': symbol, 'side': side, 'direction': direction,
                    'quantity': qty, 'entry_price': entry, 'sl_price': sl,
                    'original_sl_price': sl,
                    'tp_next_tier': 0, 'status': 'active', 'precision': precision,
                    'timestamp': trigger_ts if trigger_ts > 0 else int(time.time() * 1000)
                })
                save_active_signals(signals)
                send_telegram_message(
                    f"<b>🤖 自動下單 ({leverage}x)</b>\n\n"
                    f"💎 {get_base_coin(symbol)} [{direction}]\n"
                    f"🎯 進場: <code>{entry:.{precision}f}</code>\n"
                    f"🛑 止損: <code>{sl:.{precision}f}</code>"
                )
                return order

            except Exception as e:
                last_error = str(e)
                # 下單失敗且屬於倉位/槓桿超限類 → 降層重試 (正常降層觸發條件)
                if any(code in last_error for code in ORDER_RETRYABLE_CODES) or "leverage" in last_error.lower():
                    logger.warning(f"  策略 {strategy_name} ({leverage}x) 下單失敗 (倉位/槓桿超限)，降層至下一策略...")
                    continue
                else:
                    logger.error(f"  策略 {strategy_name} 觸發不可恢復下單錯誤: {last_error}")
                    break

        err_msg = last_error or "未知錯誤"
        
        # 將生硬的錯誤碼轉換為人類易讀的中文原因
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


async def place_add_order(exchange, symbol, direction, entry, sl, precision, fixed_loss_usdt, trigger_ts, parent_signal_id):
    """
    加倉下單：純 Limit Order，不附帶任何止盈止損。
    風險控管完全交由主單的保護止損機制。
    """
    if not BITGET_API_KEY: return None
    try:
        risk_per_unit = abs(entry - sl)
        if risk_per_unit == 0: return None
        qty_risk_ideal = fixed_loss_usdt / risk_per_unit

        leverage_strategies = []
        try:
            markets = await exchange.load_markets()
            market = markets.get(symbol, {})
            info = market.get('info', {})
            max_lev = int(info.get('maxLever', info.get('maxLeverage', 0)))
            if max_lev == 0:
                max_lev = int(market.get('limits', {}).get('leverage', {}).get('max', 0) or 0)
            leverage_strategies.append(('MAX', max_lev if max_lev > 0 else 20))
        except Exception as e:
            logger.warning(f"  加倉 MAX 獲取最大槓桿失敗: {e}，降級使用 20x")
            leverage_strategies.append(('MAX', 20))

        leverage_strategies.append(('STABLE', 20))
        leverage_strategies.append(('FINAL', 10))

        ORDER_RETRYABLE_CODES = ("40762", "40797", "45110", "200029")
        last_error = None

        for strategy_name, leverage in leverage_strategies:
            try:
                try:
                    await exchange.set_position_mode(True, symbol)
                    await exchange.set_margin_mode('cross', symbol)
                except Exception as e:
                    logger.debug(f"  加倉策略 {strategy_name} 倉位/保證金模式設定略過: {e}")

                try:
                    await exchange.set_leverage(leverage, symbol)
                except Exception as e:
                    err_str = str(e)
                    logger.warning(f"  加倉策略 {strategy_name} ({leverage}x) set_leverage 失敗: {err_str}")
                    last_error = err_str
                    if any(code in err_str for code in ORDER_RETRYABLE_CODES) or "leverage" in err_str.lower():
                        continue
                    else:
                        break

                balance = await exchange.fetch_balance()
                available = 0.0
                try:
                    raw_info = balance.get('info', {})
                    if isinstance(raw_info, dict):
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
                            break
                    if available <= 0:
                        usdt_info = balance.get('USDT', {}).get('info', {})
                        if isinstance(usdt_info, dict):
                            available = float(usdt_info.get('crossedMaxAvailable', 0))
                except Exception as e:
                    logger.warning(f"  加倉解析可用保證金異常: {e}")
                if available <= 0:
                    available = float(balance.get('USDT', {}).get('free', 0))

                max_q = (available * 0.9) * leverage / entry
                qty = min(qty_risk_ideal, max_q)
                qty = float(exchange.amount_to_precision(symbol, qty))

                # 加倉單價值不足 6 USDT → 放棄加倉
                if qty * entry < 6:
                    logger.warning(f"  加倉策略 {strategy_name} 價值不足 6 USDT，放棄加倉")
                    return None

                side = 'buy' if direction == 'LONG' else 'sell'
                add_oid = f"add_{trigger_ts}_{parent_signal_id}"
                params = {
                    'hedged': True, 'holdSide': 'long' if direction == 'LONG' else 'short',
                    'clientOid': add_oid
                }

                order = await exchange.create_order(symbol, 'limit', side, qty, entry, params=params)
                logger.info(f"✅ 加倉下單成功: {symbol} @ {entry} (ID: {add_oid}, Strat: {strategy_name}, Lev: {leverage}x)")

                send_telegram_message(
                    f"<b>📈 加倉下單 ({leverage}x)</b>\n\n"
                    f"💎 {get_base_coin(symbol)} [{direction}]\n"
                    f"🎯 進場: <code>{entry:.{precision}f}</code>\n"
                    f"🛡️ 保護止損: <code>{sl:.{precision}f}</code>\n"
                    f"📊 數量: <code>{qty}</code>"
                )
                return order

            except Exception as e:
                last_error = str(e)
                if any(code in last_error for code in ORDER_RETRYABLE_CODES) or "leverage" in last_error.lower():
                    logger.warning(f"  加倉策略 {strategy_name} ({leverage}x) 下單失敗，降層...")
                    continue
                else:
                    logger.error(f"  加倉策略 {strategy_name} 不可恢復錯誤: {last_error}")
                    break

        logger.error(f"❌ 加倉所有策略失效 ({symbol}), 錯誤: {last_error or '未知錯誤'}")
        send_telegram_message(
            f"<b>❌ 加倉下單失敗</b>\n\n"
            f"💎 <b>交易對:</b> {get_base_coin(symbol)} [{direction}]\n"
            f"⚠️ <b>原因:</b> {last_error or '未知錯誤'}"
        )
        return None
    except Exception as e:
        logger.error(f"加倉執行異常 ({symbol}): {e}")
        return None

# ============================================================================
# 無限階梯止盈管理
# ============================================================================

async def _query_plan_order_status(exchange, symbol, client_oid):
    """查詢 Bitget 歷史計畫委託單狀態 (executed / canceled / not_found / error)。
    使用 V2 API: GET /api/v2/mix/order/orders-plan-history
    回傳 (planStatus, baseVolume)：planStatus 為 'executed'|'canceled'|'not_found'|'error'，
    baseVolume 為實際成交的幣量 (僅 executed 時有效)。
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
        # 取第一筆 (clientOid 應唯一)
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
        # 區分 API 錯誤與真正查無此單，避免上層誤判導致重複掛單
        logger.warning(f"查詢計畫委託歷史失敗 ({client_oid}): {e}")
        return 'error', 0.0


async def ensure_next_tp(exchange, symbol, side, sig, size, saved_signals, open_orders):
    """
    無限階梯 TP：
    - TP1 (1R) 平掉剩餘倉位的 50%
    - TP2 開始為 5R, 10R, 15R... 依序平掉剩餘倉位的 20%
    """
    try:
        signal_id = sig.get('signal_id', str(sig.get('timestamp')))
        entry = sig['entry_price']
        # 使用原始止損計算 R，避免保護止損上移後壓縮階梯間距
        original_sl = sig.get('original_sl_price', sig['sl_price'])
        risk = abs(entry - original_sl)
        if risk == 0:
            return
        direction = sig['direction']
        precision = sig.get('precision', 4)

        next_tier = sig.get('tp_next_tier', 0)
        tp_coid = f"tp{next_tier + 1}_{signal_id}"
        stored_tp_order_id = sig.get('tp_order_id', '')

        # 動態計算當階 TP 的 R 乘數與平倉趴數
        current_r_mult = 1 if next_tier == 0 else next_tier * 5
        current_close_pct = 0.50 if next_tier == 0 else 0.20

        # === 情況 1: tp_order_id 有值 → 檢查該筆訂單是否還在掛單簿 ===
        if stored_tp_order_id:
            has_order = any(tp_coid in get_coid(o) for o in open_orders)

            if has_order:
                # TP 單仍在掛單中 → 檢查數量是否因進場單追加成交而需修正
                tp_order_obj = next((o for o in open_orders if tp_coid in get_coid(o)), None)
                if tp_order_obj:
                    existing_qty = float(tp_order_obj.get('amount', 0) or tp_order_obj.get('info', {}).get('size', 0) or 0)
                    ideal_qty = float(exchange.amount_to_precision(symbol, size * current_close_pct))
                    # 倉位變化超過 2% 時撤舊掛新 (例如進場單後續又成交了更多數量)
                    if existing_qty > 0 and ideal_qty > 0 and abs(existing_qty - ideal_qty) / ideal_qty > 0.02:
                        
                        # 防無限迴圈: 如果目前理想份額不足 6U，代表當初是觸發「微小倉位全平防護」。
                        # 此時應比對「當前全倉數量」而非「理想數量」。
                        chk_tp_price = entry + current_r_mult * risk if direction == 'LONG' else entry - current_r_mult * risk
                        chk_tp_price = round(chk_tp_price, precision)
                        full_qty = float(exchange.amount_to_precision(symbol, size))
                        
                        # 若理想份額價值不足 6U，且當前掛單數量約等於全倉數量，則視為合法，不撤單
                        if ideal_qty * chk_tp_price < 6 and full_qty > 0 and abs(existing_qty - full_qty) / full_qty <= 0.02:
                            return

                        logger.info(f"🔄 TP{next_tier + 1} 數量不一致 ({symbol}): 掛單={existing_qty} vs 理想={ideal_qty}，撤舊掛新")
                        try:
                            await exchange.cancel_order(tp_order_obj['id'], symbol, params={'stop': True})
                        except Exception as e:
                            if "43001" not in str(e) and "does not exist" not in str(e).lower():
                                logger.error(f"撤銷舊 TP 失敗: {e}")
                                return
                        sig['tp_order_id'] = ''
                        save_active_signals(saved_signals)
                        # 不 return，繼續往下走掛出新的 TP 單
                    else:
                        return  # 數量一致，等待成交

            else:
                # TP 單已從掛單簿消失 → 查詢交易所歷史狀態確認真實結果
                plan_status, base_vol = await _query_plan_order_status(exchange, symbol, tp_coid)

                if plan_status == 'executed':
                    # 真實成交 → 推進至下一階
                    executed_tier = next_tier + 1
                    executed_r_mult = 1 if next_tier == 0 else next_tier * 5
                    executed_close_pct = 50 if next_tier == 0 else 20

                    sig['tp_next_tier'] = next_tier + 1
                    sig['tp_order_id'] = ''
                    next_tier = sig['tp_next_tier']
                    tp_coid = f"tp{next_tier + 1}_{signal_id}"
                    save_active_signals(saved_signals)
                    logger.info(f"🎯 TP{executed_tier} ({executed_r_mult}R) 確認成交 (成交量: {base_vol})，推進至 Tier {next_tier + 1}")

                    send_telegram_message(
                        f"<b>🎯 TP{executed_tier} ({executed_r_mult}R) 成交</b>\n\n"
                        f"💎 {get_base_coin(symbol)} [{direction}]\n"
                        f"📊 <b>減倉比例:</b> {executed_close_pct}%\n"
                        f"📊 <b>剩餘倉位:</b> {size:.{precision}f}"
                    )
                    # 繼續往下掛出下一階 TP

                elif plan_status == 'canceled':
                    # 手動撤銷或過期 → 不推進，清空 order_id 讓下方邏輯補掛同一階
                    logger.info(f"⚠️ TP{next_tier + 1} 被撤銷 ({symbol})，將自動補掛同一階")
                    sig['tp_order_id'] = ''
                    save_active_signals(saved_signals)
                    # 繼續往下掛出同一階 TP

                elif plan_status == 'error':
                    # API 查詢本身失敗 → 保留 tp_order_id 不動，下輪重試查詢
                    logger.warning(f"  TP{next_tier + 1} 歷史查詢 API 錯誤，保留追蹤等下輪重試")
                    return

                else:
                    # not_found：訂單確實不存在 → 清空讓下方補掛，但會經過防重複檢查
                    logger.info(f"  TP{next_tier + 1} 歷史查無此單 ({symbol})，清空追蹤準備補掛")
                    sig['tp_order_id'] = ''
                    save_active_signals(saved_signals)
                    # 繼續往下走掛單，但會經過防重複檢查

        # === 情況 2: tp_order_id 無值 → 掛出當階 TP 單 ===
        # 防重複：掛單前掃描 open_orders，若同 clientOid 已存在則跳過
        existing_tp_in_orders = any(tp_coid in get_coid(o) for o in open_orders)
        if existing_tp_in_orders:
            logger.info(f"  TP{next_tier + 1} 已在掛單簿中 ({tp_coid})，跳過重複掛單")
            # 補回 tp_order_id 追蹤 (可能是之前被誤清空的)
            tp_obj = next((o for o in open_orders if tp_coid in get_coid(o)), None)
            if tp_obj:
                sig['tp_order_id'] = str(tp_obj.get('id', tp_coid))
                save_active_signals(saved_signals)
            return
        
        tp_price = entry + current_r_mult * risk if direction == 'LONG' else entry - current_r_mult * risk
        tp_price = round(tp_price, precision)
        tp_qty = float(exchange.amount_to_precision(symbol, size * current_close_pct))

        # 最小名義價值檢查 (使用 tp_price 而非 entry，因為交易所驗證的是觸發時的價值)
        if tp_qty <= 0 or tp_qty * tp_price < 6:
            # 份額不足 6U → 嘗試 100% 全平 (微小倉位權宜策略)
            full_qty = float(exchange.amount_to_precision(symbol, size))
            if full_qty > 0 and full_qty * tp_price >= 6:
                tp_qty = full_qty
                logger.info(f"  TP{next_tier + 1} {int(current_close_pct * 100)}% 份額不足 6U，改為全倉平倉: {tp_qty}")
            else:
                logger.debug(f"  TP{next_tier + 1} 全倉價值仍不足 6U，停止掛單 (粉塵由 SL 保護)")
                return

        order_side = 'sell' if direction == 'LONG' else 'buy'
        tp_params = {
            'triggerPrice': tp_price, 'triggerType': 'fill_price', 'reduceOnly': True,
            'hedged': True, 'holdSide': 'long' if direction == 'LONG' else 'short',
            'clientOid': tp_coid
        }

        logger.info(f"🚀 掛出 TP{next_tier + 1} ({current_r_mult}R): {symbol} @ {tp_price:.{precision}f} | qty: {tp_qty} | ID: {tp_coid}")
        try:
            result = await exchange.create_order(symbol, 'market', order_side, tp_qty, None, params=tp_params)
            # 記錄交易所回傳的 order_id 供後續狀態追蹤
            sig['tp_order_id'] = str(result.get('id', ''))
        except Exception as e:
            if '40786' in str(e):
                logger.warning(f"⚠️ TP ID 已存在 ({tp_coid})，視為已掛單。")
                # clientOid 重複代表已存在，標記 tp_order_id 為 coid 本書作為追蹤依據
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
    倉位監控：無限階梯 TP 管理 + SL 補掛 + 孤兒單清理
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



                    # A. 整理本訊號所屬的止損單
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

                    # B. SL 價格不一致偵測：保護止損上移後，撤銷舊單讓補掛機制用新價格重新掛出
                    if len(my_sl_orders) == 1:
                        existing_trig = float(my_sl_orders[0].get('triggerPrice', 0) or my_sl_orders[0].get('info', {}).get('triggerPrice') or 0)
                        # 取得 SL 掛單數量
                        sl_order_qty = float(my_sl_orders[0].get('amount', 0) or my_sl_orders[0].get('info', {}).get('size', 0) or 0)
                        need_cancel = False

                        # 價格不一致
                        if abs(existing_trig - sl_price_target) > 1e-8:
                            logger.info(f"🔄 SL 價格不一致 ({symbol}): 掛單={existing_trig} vs 目標={sl_price_target}，撤銷舊單")
                            need_cancel = True

                        # 數量不一致 (TP 成交後倉位縮減，容許 2% 誤差)
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

                    # C. 確保止損單掛出
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

                    # C. 無限階梯 TP 管理
                    await ensure_next_tp(exchange, symbol, side, sig, size, saved_signals, open_orders)

            if not found_signal:
                logger.warning(f"⚠️ 發現外部倉位: {symbol} ({side}) | 本地無掃描信號，系統不介入。")

        # 2. 孤兒訊號清理 (倉位消失但訊號仍 active)
        for sig_key, sig_list in list(saved_signals.items()):
            for sig in sig_list:
                if sig['status'] != 'active':
                    continue
                signal_id = sig['signal_id']
                symbol = sig['symbol']
                direction = sig['direction']
                has_pos = any(p['symbol'] == symbol and p['side'].upper() == direction for p in active_pos)

                has_entry = any(
                    str(o.get('clientOrderId') or o.get('info', {}).get('clientOid') or "") == str(signal_id)
                    for o in open_orders)

                # ==============================================================
                # 追高防護：價格已達 TP1 目標 -> 撤銷殘餘未成交進場單
                # 不論是否已有部分倉位，只要仍有進場掛單就檢查
                # ==============================================================
                if has_entry:
                    try:
                        entry_ts = int(sig.get('timestamp', 0))
                        ohlcv_1d = await exchange.fetch_ohlcv(symbol, '1d', limit=200)
                        ticker = await exchange.fetch_ticker(symbol)
                        current_price = float(ticker['last'])
                        original_sl = float(sig.get('original_sl_price', sig['sl_price']))
                        entry_price = float(sig['entry_price'])
                        risk = abs(entry_price - original_sl)

                        if risk > 0:
                            is_runaway = False
                            # 1. 檢查當前即時市價是否已達 TP1
                            if direction == 'LONG' and current_price >= entry_price + TP_STEP_R * risk:
                                is_runaway = True
                            elif direction == 'SHORT' and current_price <= entry_price - TP_STEP_R * risk:
                                is_runaway = True

                            # 2. 檢查條件二成立之後的所有歷史 K 棒 high/low 是否曾達到 TP1
                            if not is_runaway and len(ohlcv_1d) > 0:
                                for candle in ohlcv_1d:
                                    candle_ts = int(candle[0])
                                    if candle_ts > entry_ts:
                                        c_high = float(candle[2])
                                        c_low = float(candle[3])
                                        if direction == 'LONG' and c_high >= entry_price + 1 * risk:
                                            is_runaway = True
                                            break
                                        elif direction == 'SHORT' and c_low <= entry_price - 1 * risk:
                                            is_runaway = True
                                            break

                            if is_runaway:
                                logger.info(f"🏃 價格已達 TP1 ({symbol})，撤銷未成交進場單 {signal_id}")
                                entry_orders = [o for o in open_orders if str(o.get('clientOrderId') or o.get('info', {}).get('clientOid') or "") == str(signal_id)]
                                for eo in entry_orders:
                                    try:
                                        await exchange.cancel_order(eo['id'], symbol)
                                        open_orders = [o for o in open_orders if o['id'] != eo['id']]
                                    except Exception as e:
                                        logger.warning(f"撤銷未成交單失敗 {eo['id']}: {e}")

                                # 有部分倉位時僅撤進場單，訊號保持 active 讓 TP/SL 繼續管理
                                if has_pos:
                                    send_telegram_message(
                                        f"<b>🏃 價格已達 TP1 (撤銷殘餘進場單)</b>\n\n"
                                        f"💎 <b>交易對:</b> {get_base_coin(symbol)} [{direction}]\n"
                                        f"📉 <b>狀態: 部分成交，殘餘進場單已撤銷，倉位繼續由 TP/SL 管理</b>"
                                    )
                                else:
                                    # 完全未成交 → 關閉訊號
                                    send_telegram_message(
                                        f"<b>🏃 價格已達 TP1 (錯失進場)</b>\n\n"
                                        f"💎 <b>交易對:</b> {get_base_coin(symbol)} [{direction}]\n"
                                        f"📉 <b>狀態: 已自動撤銷未成交之進場單</b>"
                                    )
                                    sig['status'] = 'closed'
                                continue

                        # 訊號淘汰條件撤單：若尚未進場，且3D線收盤跌破 10MA 或最低點低於止損價，則作廢訊號撤單
                        if not is_runaway:
                            try:
                                ohlcv_3d = compose_3d_bars(ohlcv_1d)
                                if ohlcv_3d and len(ohlcv_3d) >= 15:
                                    df_3d = pd.DataFrame(ohlcv_3d, columns=['ts', 'open', 'high', 'low', 'close', 'vol', 'close_ts'])
                                    df_3d['sma_10'] = df_3d['close'].rolling(window=10).mean()
                                    
                                    now_utc_3d_fl = int(time.time() * 1000)
                                    closed_3d = df_3d[df_3d['close_ts'] <= now_utc_3d_fl]
                                    
                                    if len(closed_3d) > 0:
                                        last_closed = closed_3d.iloc[-1]
                                        last_close_time = int(last_closed['close_ts'])
                                        
                                        # 只檢查訊號產生之後的 K 棒
                                        if last_close_time > entry_ts:
                                            # 如果收盤低於 10MA，或最低價跌破/觸及原止損 (C2_low)
                                            if last_closed['close'] < last_closed['sma_10'] or last_closed['low'] <= float(sig['original_sl_price']):
                                                logger.info(f"🚫 淘汰條件已成立 (跌破10MA或最低價) ({symbol})，撤銷未成交單 {signal_id}")
                                                entry_orders = [o for o in open_orders if str(o.get('clientOrderId') or o.get('info', {}).get('clientOid') or "") == str(signal_id)]
                                                for eo in entry_orders:
                                                    try:
                                                        await exchange.cancel_order(eo['id'], symbol)
                                                        open_orders = [o for o in open_orders if o['id'] != eo['id']]
                                                    except Exception as e:
                                                        logger.warning(f"撤銷未成交單失敗 {eo['id']}: {e}")
                                                send_telegram_message(
                                                    f"<b>🚫 訊號已淘汰 (撤銷進場)</b>\n\n"
                                                    f"💎 <b>交易對:</b> {get_base_coin(symbol)} [{direction}]\n"
                                                    f"📉 <b>狀態: 3D線跌破 10MA 或止損價，已自動撤銷進場單</b>"
                                                )
                                                sig['status'] = 'closed'
                                                continue
                            except Exception as e:
                                logger.warning(f"檢查訊號淘汰撤單異常 ({symbol}): {e}")

                        # 保護止損產生檢查：若尚未進場但 3D K 棒已滿足保護止損上移條件，撤銷掛單
                        if not is_runaway and sig.get('status') == 'active':
                            try:
                                ohlcv_1d_3d = ohlcv_1d
                                composed_3d = compose_3d_bars(ohlcv_1d_3d)
                                if composed_3d and len(composed_3d) >= 2:
                                    df_3d = pd.DataFrame(composed_3d, columns=['ts', 'open', 'high', 'low', 'close', 'vol', 'close_ts'])
                                    now_utc_3d = int(time.time() * 1000)
                                    closed_3d = df_3d[df_3d['close_ts'] <= now_utc_3d]

                                    has_generated_sl = False
                                    for idx in range(1, len(closed_3d)):
                                        curr_b = closed_3d.iloc[idx]
                                        prev_b = closed_3d.iloc[idx-1]
                                        if curr_b['close_ts'] > entry_ts:
                                            prev_body_high = max(prev_b['open'], prev_b['close'])
                                            if curr_b['close'] > prev_body_high and curr_b['low'] > entry_price:
                                                has_generated_sl = True
                                                break

                                    if has_generated_sl:
                                                logger.info(f"🛡️ 保護止損已產生 ({symbol})，撤銷未成交單 {signal_id}")
                                                entry_orders = [o for o in open_orders if str(o.get('clientOrderId') or o.get('info', {}).get('clientOid') or "") == str(signal_id)]
                                                for eo in entry_orders:
                                                    try:
                                                        await exchange.cancel_order(eo['id'], symbol)
                                                        open_orders = [o for o in open_orders if o['id'] != eo['id']]
                                                    except Exception as e:
                                                        logger.warning(f"撤銷未成交單失敗 {eo['id']}: {e}")
                                                send_telegram_message(
                                                    f"<b>🛡️ 保護止損已產生 (撤銷進場)</b>\n\n"
                                                    f"💎 <b>交易對:</b> {get_base_coin(symbol)} [{direction}]\n"
                                                    f"📉 <b>狀態: 掛單未成交前已產生保護止損，已自動撤銷進場單</b>"
                                                )
                                                sig['status'] = 'closed'
                                                continue
                            except Exception as e:
                                logger.warning(f"檢查保護止損產生撤單異常 ({symbol}): {e}")

                    except Exception as e:
                        logger.warning(f"檢查未成交單價格異常 ({symbol}): {e}")
                # ==============================================================

                if not has_pos and not has_entry:
                    logger.info(f"🧹 偵測到歸零/孤兒訊號 {sig_key}，開始清理...")

                    # 清理殘留掛單
                    orphan_orders = [o for o in open_orders
                                     if signal_id in str(o.get('clientOrderId') or
                                                         o.get('info', {}).get('clientOid') or "")]
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

            saved_signals[sig_key] = [s for s in sig_list if s['status'] == 'active']
            if not saved_signals[sig_key]:
                del saved_signals[sig_key]

        # 3. 盲目孤兒單清理 (訂單找名單)
        # Entry 格式: 3d_{signal_id}, TP 格式: tp{n}_{signal_id}, SL 格式: sl_{signal_id}, Add 格式: add_{ts}_{signal_id}
        tp_prefix_pattern = re.compile(r'^tp\d+_')
        add_prefix_pattern = re.compile(r'^add_\d+_')
        all_active_ids = [str(s['signal_id']) for slist in saved_signals.values() for s in slist]
        for oo in open_orders:
            co_id = str(oo.get('clientOrderId') or oo.get('info', {}).get('clientOid') or "")
            is_sl = co_id.startswith("sl_")
            is_tp = bool(tp_prefix_pattern.match(co_id))
            is_entry = co_id.startswith("3d_")
            is_add = bool(add_prefix_pattern.match(co_id))
            if is_sl or is_tp or is_entry or is_add:
                if is_sl:
                    raw_sig_id = co_id[3:]
                elif is_tp:
                    raw_sig_id = tp_prefix_pattern.sub('', co_id)
                elif is_add:
                    # add_{trigger_ts}_{parent_signal_id} → 取 parent_signal_id
                    parts = co_id.split('_')
                    raw_sig_id = '_'.join(parts[2:]) if len(parts) >= 4 else co_id[4:]
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
                            is_plan = not is_entry and not is_add
                            await exchange.cancel_order(oo['id'], symbol, params={'stop': True} if is_plan else {})
                            open_orders = [o for o in open_orders if o['id'] != oo['id']]
                        except IndexError:
                            logger.info(f"孤兒單 {co_id} 已自行消失 (API 空回應)，視為安全。")
                        except Exception as e:
                            if "43001" not in str(e) and "does not exist" not in str(e).lower():
                                logger.error(f"盲目清理失敗 {co_id}: {e}")

        # 4. 未成交加倉單監測 (日K跌破 10MA → 撤銷)
        add_orders_by_symbol = {}
        for oo in open_orders:
            co_id = str(oo.get('clientOrderId') or oo.get('info', {}).get('clientOid') or "")
            if co_id.startswith("add_"):
                sym = oo['symbol']
                if sym not in add_orders_by_symbol:
                    add_orders_by_symbol[sym] = []
                add_orders_by_symbol[sym].append(oo)

        for sym, add_orders in add_orders_by_symbol.items():
            try:
                ohlcv_1d_add = await exchange.fetch_ohlcv(sym, '1d', limit=200)
                ohlcv_3d_add = compose_3d_bars(ohlcv_1d_add)
                if not ohlcv_3d_add or len(ohlcv_3d_add) < 15:
                    continue
                df_3d_add = pd.DataFrame(ohlcv_3d_add, columns=['ts', 'open', 'high', 'low', 'close', 'vol', 'close_ts'])
                df_3d_add['sma_10'] = df_3d_add['close'].rolling(window=10).mean()
                now_utc_3d_add = int(time.time() * 1000)
                closed_3d_add = df_3d_add[df_3d_add['close_ts'] <= now_utc_3d_add]

                if len(closed_3d_add) > 0:
                    last_bar_add = closed_3d_add.iloc[-1]
                    if pd.notna(last_bar_add['sma_10']) and last_bar_add['close'] < last_bar_add['sma_10']:
                        for ao in add_orders:
                            ao_coid = str(ao.get('clientOrderId') or ao.get('info', {}).get('clientOid') or "")
                            try:
                                await exchange.cancel_order(ao['id'], sym)
                                open_orders = [o for o in open_orders if o['id'] != ao['id']]
                                logger.info(f"🚫 加倉單已作廢 (跌破10MA): {ao_coid} ({sym})")
                            except Exception as e:
                                if "43001" not in str(e) and "does not exist" not in str(e).lower():
                                    logger.warning(f"撤銷加倉單失敗 {ao_coid}: {e}")
                        send_telegram_message(
                            f"<b>🚫 加倉單已作廢 (跌破10MA)</b>\n\n"
                            f"💎 <b>交易對:</b> {get_base_coin(sym)}\n"
                            f"📉 <b>狀態:</b> 3D線收盤跌破 10MA，已自動撤銷未成交加倉單"
                        )
            except Exception as e:
                logger.warning(f"監測加倉單異常 ({sym}): {e}")

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

        # 拉取 1D 與 1H 資料
        ohlcv_1d = await exchange.fetch_ohlcv(symbol, '1d', limit=500)
        if not ohlcv_1d or len(ohlcv_1d) < 36:
            return None

        ohlcv_1h = await exchange.fetch_ohlcv(symbol, '1h', limit=700)
        if not ohlcv_1h or len(ohlcv_1h) < 100:
            return None

        # 合成各層級 K 棒
        ohlcv_18d = compose_18d_bars(ohlcv_1d)
        ohlcv_3d = compose_3d_bars(ohlcv_1d)
        ohlcv_3h = compose_3h_bars(ohlcv_1h)

        if not ohlcv_18d or not ohlcv_3d or not ohlcv_3h:
            return None

        df_18d = pd.DataFrame(ohlcv_18d, columns=['ts', 'open', 'high', 'low', 'close', 'vol', 'close_ts'])
        df_18d_closed = df_18d[df_18d['close_ts'] <= now_utc].reset_index(drop=True)

        df_3d = pd.DataFrame(ohlcv_3d, columns=['ts', 'open', 'high', 'low', 'close', 'vol', 'close_ts'])
        df_3d['sma_10'] = df_3d['close'].rolling(window=10).mean()
        df_3d_closed = df_3d[df_3d['close_ts'] <= now_utc].reset_index(drop=True)

        df_3h = pd.DataFrame(ohlcv_3h, columns=['ts', 'open', 'high', 'low', 'close', 'vol', 'close_ts'])
        df_3h['sma_100'] = df_3h['close'].rolling(window=100).mean()
        df_3h_closed = df_3h[df_3h['close_ts'] <= now_utc].reset_index(drop=True)

        # =========================================================
        # 單一時間軸事件推進：從舊往新找，即時監控失效條件
        # =========================================================
        # 預先計算 L1 的狀態轉換事件
        l1_events = {}
        for i in range(1, len(df_18d_closed)):
            b = df_18d_closed.iloc[i]
            p = df_18d_closed.iloc[i - 1]
            t = int(b['close_ts'])
            dt_str = pd.to_datetime(b['ts'], unit='ms', utc=True).strftime('%Y-%m-%d')
            if b['close'] > b['open'] and p['close'] < p['open']:
                l1_events[t] = {'type': 'found', 'dt_str': dt_str}
            elif b['close'] < b['open']:
                l1_events[t] = {'type': 'invalid'}

        # 將 3D 資料轉為 Dict 方便依時間查詢
        dict_3d = {int(row['close_ts']): row for _, row in df_3d_closed.iterrows()}

        # 狀態變數
        l1_valid = False
        l2_valid = False
        c1_valid = False
        c2_valid = False
        
        l1_date_str = "未知"
        l2_date_str = "未知"
        c1_date_str = "未知"
        c2_date_str = "未知"
        
        entry_price = 0.0
        stop_loss = 0.0
        trigger_ts = 0

        # 主迴圈：以 3H (最高解析度) 推進
        for _, row in df_3h_closed.iterrows():
            t = int(row['close_ts'])
            
            # 1. 處理 18D (L1) 事件
            if t in l1_events:
                evt = l1_events[t]
                if evt['type'] == 'found':
                    l1_valid = True
                    l1_date_str = evt['dt_str']
                    # 新 L1 出現，重置 L2 與 C1
                    l2_valid = False
                    c1_valid = False
                    l2_date_str = "未知"
                    c1_date_str = "未知"
                elif evt['type'] == 'invalid':
                    l1_valid = False
                    l2_valid = False
                    c1_valid = False
            
            # 2. 處理 3D (L2) 事件
            if t in dict_3d:
                b3d = dict_3d[t]
                if pd.notna(b3d['sma_10']):
                    if b3d['close'] < b3d['sma_10']:
                        # 跌破 10MA，L2 失效，連帶 C1 也失效
                        l2_valid = False
                        c1_valid = False
                    elif l1_valid and not l2_valid:
                        # 只有在 L1 成立，且目前沒有 L2 時，才尋找新 L2
                        if b3d['close'] > b3d['open'] and b3d['close'] > b3d['sma_10']:
                            l2_valid = True
                            l2_date_str = pd.to_datetime(b3d['ts'], unit='ms', utc=True).strftime('%Y-%m-%d')
                            c1_valid = False
                            c1_date_str = "未知"
                            
            # 3. 處理 3H (L3) 事件
            if pd.isna(row['sma_100']):
                continue
                
            if c2_valid:
                # [持倉中] 只監控止損。背景的 L1/L2 失效不會中斷當前持倉。
                if row['low'] <= stop_loss:
                    c2_valid = False
                    # 跌破止損後，若背景 L1 與 L2 依然有效，就只會退回尋找 C1
                    c1_valid = False 
                    c1_date_str = "未知"
                    c2_date_str = "未知"
            else:
                # [非持倉]
                if l2_valid:
                    if not c1_valid:
                        # 尋找 C1
                        if row['close'] < row['sma_100']:
                            c1_valid = True
                            c1_date_str = pd.to_datetime(row['ts'], unit='ms', utc=True).strftime('%Y-%m-%d %H:%M:%S')
                    else:
                        # 尋找 C2
                        if row['close'] > row['sma_100']:
                            c2_valid = True
                            c2_date_str = pd.to_datetime(row['ts'], unit='ms', utc=True).strftime('%Y-%m-%d %H:%M:%S')
                            entry_price = float(row['close'])
                            stop_loss = float(row['low'])
                            trigger_ts = int(row['ts'])

        # 迴圈結束，判定最終狀態
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

        # 根據最終狀態決定是否保留或通知
        cache_ts = trigger_ts if is_trigger_met else (list(l1_events.keys())[-1] if l1_events else 0)
        action = 'update' if (not cached_info or cached_info.get('ts') != cache_ts) else 'keep'
        if final_state == 'l1_waiting':
            action = 'remove'

        if action == 'remove':
            return {'symbol': symbol, 'action': 'remove'}

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
            'c1_date':            c1_date_str,
            'c2_date':            c2_date_str,
            'l1_date':            l1_date_str,
            'l2_date':            l2_date_str,
            'scan_state':         final_state,
        }

    except Exception as e:
        logger.warning(f"掃描異常 ({symbol}): {type(e).__name__}: {e}")
        return None


# ============================================================================
# 訊息推送模組
# ============================================================================

def send_grouped_message(item_list, title):
    """合併傳入的幣種清單為一則群組訊息，按日期排列"""
    if not item_list:
        return

    # 按 l1_date 分組
    date_groups = {}
    for item in item_list:
        d = item.get('l1_date', '未知日期')
        if d not in date_groups:
            date_groups[d] = []
        date_groups[d].append(item)

    lines = [f"<b>{title}</b>\n"]
    for date_key in sorted(date_groups.keys()):
        coin_strs = []
        for item in date_groups[date_key]:
            base = get_base_coin(item['symbol'])
            if item.get('missed'):
                coin_strs.append(f"{base} (未上車)")
            else:
                coin_strs.append(base)
        coins = " · ".join(coin_strs)
        lines.append(f"📅 {date_key}")
        lines.append(f"💎 {coins}\n")

    send_telegram_message("\n".join(lines))

def send_triggered_message(item, default_loss):
    """3H 已成立的幣種，獨立一則訊息，含倉位價值與成立日期"""
    display_symbol = get_base_coin(item['symbol'])
    precision = item['precision']
    entry = item['entry_price']
    sl = item['stop_loss']
    l1_d = item.get('l1_date', '未知')
    l2_d = item.get('l2_date', '未知')
    c1_d = item.get('c1_date', '未知')
    c2_d = item.get('c2_date', '未知')

    # 倉位價值 = 預設虧損金額 / |(進場價 - 止損價) / 進場價|
    loss_pct = abs((entry - sl) / entry) if entry != 0 else 0
    position_value = default_loss / loss_pct if loss_pct > 0 else 0

    msg = (
        f"💎 <b>交易對:</b> {display_symbol}\n\n"
        f"📅 <b>L1 (18日) 日期:</b> <code>{l1_d}</code>\n"
        f"📅 <b>L2 (3日) 日期:</b> <code>{l2_d}</code>\n"
        f"📅 <b>C1 (3H) 成立時間:</b> <code>{c1_d}</code>\n"
        f"📅 <b>C2 (3H) 觸發時間:</b> <code>{c2_d}</code>\n"
        f"📍 <b>進場價格:</b> <code>{entry:.{precision}f}</code>\n"
        f"🛡️ <b>止損價格:</b> <code>{sl:.{precision}f}</code>\n"
        f"💰 <b>倉位價值:</b> <code>{position_value:.2f} USDT</code>"
    )
    send_telegram_message(msg)

def send_system_settings_message(config):
    """獨立一則系統設定訊息"""
    loss = config.get("default_loss_amount", 6)
    tw_loss = config.get("default_tw_loss_amount", 300)

    msg = (
        f"⚙️ <b>系統快速設定</b>\n\n"
        f"💵 <b>加密貨幣預設虧損:</b> {loss} USDT\n"
        f"💵 <b>台股預設虧損:</b> {int(tw_loss):,} TWD"
    )

    reply_markup = {
        "inline_keyboard": [
            [
                {"text": "📝 修改加密貨幣虧損", "switch_inline_query_current_chat": "/set_loss "},
                {"text": "📝 修改台股虧損", "switch_inline_query_current_chat": "/set_tw_loss "}
            ]
        ]
    }

    send_telegram_message(msg, reply_markup=reply_markup)

# ============================================================================
# Telegram 指令處理
# ============================================================================

# 全域 offset，避免重複處理同一條訊息
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

            # 僅處理來自目標 chat 的訊息
            if chat_id != str(TG_CHAT_ID) or not text:
                continue

            # 支援 inline_query_current_chat 自動帶入的 @botname 前綴
            text = re.sub(r'^@\w+\s*', '', text).strip()

            # 處理指令
            reply = ""
            if text.startswith("/set_loss"):
                parts = text.split()
                if len(parts) >= 2:
                    try:
                        new_val = float(parts[1])
                        if new_val <= 0:
                            raise ValueError("金額必須大於 0")
                        config = load_config()
                        config["default_loss_amount"] = new_val
                        save_config(config)
                        reply = f"✅ 加密貨幣預設虧損已更新為 <b>{new_val} USDT</b>"
                        logger.info(f"⚙️ /set_loss 指令: 虧損金額更新為 {new_val}")
                    except ValueError:
                        reply = "❌ 格式錯誤，請輸入大於零的數字。"
                else:
                    reply = "❌ 格式錯誤，未提供數字。"

                send_url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
                payload = {"chat_id": chat_id, "text": reply, "parse_mode": "HTML"}
                requests.post(send_url, json=payload, timeout=10)

            elif text.startswith("/set_tw_loss"):
                parts = text.split()
                if len(parts) >= 2:
                    try:
                        new_val = float(parts[1])
                        if new_val <= 0:
                            raise ValueError("金額必須大於 0")
                        config = load_config()
                        config["default_tw_loss_amount"] = new_val
                        save_config(config)
                        reply = f"✅ 台股預設虧損已更新為 <b>{int(new_val):,} TWD</b>"
                        logger.info(f"⚙️ /set_tw_loss 指令: 台股虧損更新為 {new_val}")
                    except ValueError:
                        reply = "❌ 格式錯誤，請輸入大於零的數字。"
                else:
                    reply = "❌ 格式錯誤，未提供數字。"

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
    default_loss = config.get("default_loss_amount", 6)

    try:
        try:
            markets = await ex.load_markets()
            coins = [s for s, m in markets.items() if m.get('linear') and m.get('quote') == 'USDT']
            precisions = {s: max(0, int(round(-np.log10(markets[s].get('precision', {}).get('price', 1e-8))))) for s in coins}
        except:
            coins = ["BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT", "XRP/USDT:USDT", "DOGE/USDT:USDT", "ADA/USDT:USDT"]
            precisions = {s: 4 for s in coins}; precisions.update({"BTC/USDT:USDT": 2, "ETH/USDT:USDT": 2})

        # 一次性拉取持倉列表，供下單前判斷用
        existing_positions = []
        if BITGET_API_KEY:
            try:
                positions = await ex.fetch_positions()
                existing_positions = [p for p in positions if float(p.get('contracts', 0) or p.get('size', 0)) > 0]
            except Exception as e:
                logger.warning(f"拉取持倉列表失敗 (下單防重複查詢): {e}")

        all_results = []
        total_coins = len(coins)
        for i in range(0, total_coins, 20):
            batch = coins[i:i+20]
            tasks = [scan_for_symbol(ex, s, get_base_coin(s), precisions[s], i + idx + 1, total_coins, watchlist.get(s)) for idx, s in enumerate(batch)]
            results = await asyncio.gather(*tasks)

            for res in results:
                if res is None: continue
                sym = res['symbol']
                
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

        # 回寫快取 (此處先寫一次，下方下單後可能再更新 last_trigger_ts)
        save_watchlist(watchlist)
        active_count = len(watchlist)

        # === 分組歸類與下單邏輯 ===
        signals = load_active_signals()
        
        # 收集當前所有已持倉或有 active 掛單的幣種
        holding_map = {}
        for slist in signals.values():
            for s in slist:
                if s['status'] == 'active':
                    sym = s['symbol']
                    ts = s.get('timestamp', 0)
                    dt_str = datetime.fromtimestamp(ts/1000).strftime('%Y-%m-%d') if ts > 0 else '持續追蹤'
                    holding_map[sym] = {'symbol': sym, 'l1_date': dt_str}
                    
        for p in existing_positions:
            if p['side'].upper() == 'LONG':
                sym = p['symbol']
                if sym not in holding_map:
                    holding_map[sym] = {'symbol': sym, 'l1_date': '外部建倉'}

        holding_items = []
        real_watching = []
        real_new_triggers = []

        for sym, data in holding_map.items():
            holding_items.append(data)
        
        holding_items_dict = {item['symbol']: item for item in holding_items}

        real_holding_new_triggers = []

        for item in all_results:
            sym = item['symbol']
            
            if sym in holding_items_dict:
                # 持倉中不修改原始日期
                # 檢查是否有新訊號觸發（僅通知不下單）
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
                # 若同一觸發已處理過，不再進場，視為錯失後持續關注
                if cached.get('last_trigger_ts') == trigger_ts and trigger_ts > 0:
                    item['missed'] = True
                    real_watching.append(item)
                else:
                    real_new_triggers.append(item)
            else:
                real_watching.append(item)

        holding_items = list(holding_items_dict.values())

        # === 執行下單 ===
        for item in real_new_triggers:
            sym = item['symbol']
            if BITGET_API_KEY:
                order = await place_order(
                    ex, sym, 'LONG', item['entry_price'], item['stop_loss'],
                    item['precision'], default_loss, item.get('trigger_ts', 0)
                )
                # 若下單成功 (回傳 dict) 或遇到不可恢復之交易所限制 (回傳 FATAL_REJECTED)
                # 則標記該訊號已處理。若是資金不足或網路異常 (回傳 None)，則保留明天重試機會
                if order:
                    if sym in watchlist:
                        watchlist[sym]['last_trigger_ts'] = item.get('trigger_ts', 0)
                    save_watchlist(watchlist)

        # 下單後重新載入 active_signals，避免用掃描開始時的舊快照覆蓋 place_order 新寫入的訊號
        signals = load_active_signals()
        # === 排序與推播 ===
        # 1. 新進場訊號 (Triggered)
        for item in real_new_triggers:
            send_triggered_message(item, default_loss)
            
        # 2. 持倉中 (Holding)
        if holding_items:
            send_grouped_message(holding_items, "💼 <b>加密貨幣[持倉中]</b>")
            
        # 3. 關注中 (Watching)
        if real_watching:
            send_grouped_message(real_watching, "👀 <b>加密貨幣[關注中]</b>")

        active_count = len(watchlist)
        logger.info(f"✅ 掃描完成。新觸發: {len(real_new_triggers)} / 持倉: {len(holding_items)} / 持倉新訊號: {len(real_holding_new_triggers)} / 關注: {len(real_watching)} / 追蹤總數: {active_count}")

        if not real_new_triggers and not holding_items and not real_watching:
            send_telegram_message(f"✅ <b>條件掃描完成</b>\n本次共掃描 {total_coins} 個幣種，無滿足條件標的。\n(當前清單追蹤中: {active_count} 個)")

        # 4. 系統設定 (System Settings)
        if real_new_triggers or holding_items or real_watching or real_holding_new_triggers:
            send_system_settings_message(config)
    finally:
        await ex.close()

async def run_tw_scanner_background():
    try:
        logger.info("🚀 啟動背景台股掃描任務...")
        await tw_scanner.main_loop()
        config = load_config()
        send_system_settings_message(config)
        logger.info("✅ 背景台股掃描任務與設定發送完成。")
    except Exception as e:
        logger.error(f"💥 背景台股掃描任務異常: {e}")

async def scheduler():
    last_day = -1
    last_tw_day = -1
    
    # 初始執行：加密貨幣
    try:
        await run_scan()
    except Exception as e:
        logger.error(f"初始掃描異常: {e}")

    # 初始執行：台股 (無條件強制掃描) - 改為背景任務非阻塞
    asyncio.create_task(run_tw_scanner_background())

    while True:
        try:
            now = datetime.utcnow()
            # 加密貨幣每日 UTC 00:00~00:10 觸發一次掃描
            if now.hour == 0 and now.minute <= 10 and now.day != last_day:
                try:
                    await run_scan()
                except Exception as e:
                    logger.error(f"定時掃描異常: {e}")
                last_day = now.day

            # 台股每日 UTC 06:30~06:40 (台灣時間 14:30) 觸發一次掃描 - 改為背景任務非阻塞
            if now.hour == 6 and now.minute >= 30 and now.minute <= 40 and now.day != last_tw_day:
                asyncio.create_task(run_tw_scanner_background())
                last_tw_day = now.day

            # 倉位監控 (每個迴圈週期執行)
            if BITGET_API_KEY:
                ex = get_exchange()
                try:
                    await monitor_positions(ex)
                except Exception as e:
                    logger.error(f"監控週期異常: {e}")
                finally:
                    await ex.close()

        except Exception as e:
            # 頂層防護：確保背景執行緒在任何罕見異常下不會死亡
            logger.critical(f"💥 Scheduler 頂層異常 (已攔截): {e}")
        await asyncio.sleep(60)

# ============================================================================
# Minimal Flask for Zeabur Health Check
# ============================================================================

app = Flask(__name__)
@app.route('/')
@app.route('/health')
def health(): return {"status": "ok", "service": "3D-Scanner-Auto"}, 200

def run_flask():
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)

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
