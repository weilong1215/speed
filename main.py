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
    return {"default_loss_amount": 6}

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
    """將 1D OHLCV 合成 3D K 棒 (按固定週期對齊)
    輸入: [[ts, open, high, low, close, vol], ...]
    輸出: 同格式的 3D K 棒列表，僅包含完整的 3 天週期
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
        
        # 判斷是否為已完成的 3D 週期：
        # 1. 滿 3 天
        # 2. 或是跨年時的殘餘天數 (即它不是整個陣列的最後一組)
        is_completed = (len(bars) >= 3) or (i < len(sorted_gts) - 1)
        
        if not is_completed:
            continue
            
        result.append([
            gts,
            bars[0][1],
            max(b[2] for b in bars),
            min(b[3] for b in bars),
            bars[-1][4],
            sum(b[5] for b in bars),
            bars[-1][0] + 24 * 3600 * 1000  # 第 7 個元素：實際收盤時間 (最後一根 1D 棒結束時)
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

                order = await exchange.create_order(symbol, 'limit', side, qty, entry, params=params)
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
    無限階梯 TP：每 5R 平掉剩餘倉位的 20%。
    嚴格依賴交易所訂單狀態判定推進 (方案 A)：
    - tp_order_id 有值且仍在掛單 → 檢查數量是否需要修正，否則等待成交。
    - tp_order_id 有值但已從掛單消失 → 查詢歷史狀態：
        executed → 推進至下一階。
        canceled → 清空 tp_order_id，下一輪自動補掛同一階。
    - tp_order_id 無值 → 首次掛出或補掛。
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

        # === 情況 1: tp_order_id 有值 → 檢查該筆訂單是否還在掛單簿 ===
        if stored_tp_order_id:
            has_order = any(tp_coid in get_coid(o) for o in open_orders)

            if has_order:
                # TP 單仍在掛單中 → 檢查數量是否因進場單追加成交而需修正
                tp_order_obj = next((o for o in open_orders if tp_coid in get_coid(o)), None)
                if tp_order_obj:
                    existing_qty = float(tp_order_obj.get('amount', 0) or tp_order_obj.get('info', {}).get('size', 0) or 0)
                    ideal_qty = float(exchange.amount_to_precision(symbol, size * TP_CLOSE_PCT))
                    # 倉位變化超過 2% 時撤舊掛新 (例如進場單後續又成交了更多數量)
                    if existing_qty > 0 and ideal_qty > 0 and abs(existing_qty - ideal_qty) / ideal_qty > 0.02:
                        
                        # 防無限迴圈: 如果目前理想 20% 份額不足 6U，代表當初是觸發「微小倉位全平防護」。
                        # 此時應比對「當前全倉數量」而非「理想 20% 數量」。
                        chk_tp_price = entry + (next_tier + 1) * TP_STEP_R * risk if direction == 'LONG' else entry - (next_tier + 1) * TP_STEP_R * risk
                        chk_tp_price = round(chk_tp_price, precision)
                        full_qty = float(exchange.amount_to_precision(symbol, size))
                        
                        # 若理想 20% 價值不足 6U，且當前掛單數量約等於全倉數量，則視為合法，不撤單
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
                    sig['tp_next_tier'] = next_tier + 1
                    sig['tp_order_id'] = ''
                    next_tier = sig['tp_next_tier']
                    tp_coid = f"tp{next_tier + 1}_{signal_id}"
                    save_active_signals(saved_signals)
                    logger.info(f"🎯 TP{next_tier} 確認成交 (成交量: {base_vol})，推進至 Tier {next_tier + 1}")

                    send_telegram_message(
                        f"<b>🎯 TP{next_tier} 成交</b>\n\n"
                        f"💎 {get_base_coin(symbol)} [{direction}]\n"
                        f"📊 剩餘倉位: {size:.{precision}f}"
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
        tp_price = entry + (next_tier + 1) * TP_STEP_R * risk if direction == 'LONG' \
            else entry - (next_tier + 1) * TP_STEP_R * risk
        tp_price = round(tp_price, precision)
        tp_qty = float(exchange.amount_to_precision(symbol, size * TP_CLOSE_PCT))

        # 最小名義價值檢查 (使用 tp_price 而非 entry，因為交易所驗證的是觸發時的價值)
        if tp_qty <= 0 or tp_qty * tp_price < 6:
            # 20% 份額不足 6U → 嘗試 100% 全平 (微小倉位權宜策略)
            full_qty = float(exchange.amount_to_precision(symbol, size))
            if full_qty > 0 and full_qty * tp_price >= 6:
                tp_qty = full_qty
                logger.info(f"  TP{next_tier + 1} 20% 份額不足 6U，改為全倉平倉: {tp_qty}")
            else:
                logger.debug(f"  TP{next_tier + 1} 全倉價值仍不足 6U，停止掛單 (粉塵由 SL 保護)")
                return

        order_side = 'sell' if direction == 'LONG' else 'buy'
        tp_params = {
            'triggerPrice': tp_price, 'triggerType': 'fill_price', 'reduceOnly': True,
            'hedged': True, 'holdSide': 'long' if direction == 'LONG' else 'short',
            'clientOid': tp_coid
        }

        logger.info(f"🚀 掛出 TP{next_tier + 1}: {symbol} @ {tp_price:.{precision}f} | qty: {tp_qty} | ID: {tp_coid}")
        try:
            result = await exchange.create_order(symbol, 'market', order_side, tp_qty, None, params=tp_params)
            # 記錄交易所回傳的 order_id 供後續狀態追蹤
            sig['tp_order_id'] = str(result.get('id', ''))
        except Exception as e:
            if '40786' in str(e):
                logger.warning(f"⚠️ TP ID 已存在 ({tp_coid})，視為已掛單。")
                # clientOid 重複代表已存在，標記 tp_order_id 為 coid 本身作為追蹤依據
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

                    # === 跌破10MA 無保護止損 → 市價平倉 ===
                    # 止損未上移 (sl_price == original_sl_price) 且日線收盤跌破 10MA → 不值得繼續持有
                    if float(sig['sl_price']) == float(sig.get('original_sl_price', sig['sl_price'])):
                        try:
                            ohlcv_1d_chk = await exchange.fetch_ohlcv(symbol, '1d', limit=20)
                            if ohlcv_1d_chk and len(ohlcv_1d_chk) >= 15:
                                df_1d_chk = pd.DataFrame(ohlcv_1d_chk, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
                                df_1d_chk['sma_10'] = df_1d_chk['close'].rolling(window=10).mean()
                                now_utc_fl = int(pd.Timestamp.now(tz='UTC').floor('1d').timestamp() * 1000)
                                closed_1d_chk = df_1d_chk[df_1d_chk['ts'] < now_utc_fl]

                                if len(closed_1d_chk) > 0:
                                    last_bar = closed_1d_chk.iloc[-1]
                                    if pd.notna(last_bar['sma_10']) and last_bar['close'] < last_bar['sma_10']:
                                        precision = sig.get('precision', 4)
                                        logger.info(
                                            f"🚨 跌破10MA且無保護止損 ({symbol})，執行市價平倉 | "
                                            f"收盤={last_bar['close']:.{precision}f} < 10MA={last_bar['sma_10']:.{precision}f}"
                                        )

                                        # 撤銷所有相關掛單 (SL + TP)
                                        related_orders = [o for o in open_orders
                                                          if signal_id in str(o.get('clientOrderId') or o.get('info', {}).get('clientOid') or "")]
                                        for ro in related_orders:
                                            try:
                                                cid_str = get_coid(ro)
                                                is_plan = cid_str.startswith("sl_") or cid_str.startswith("tp")
                                                await exchange.cancel_order(ro['id'], symbol, params={'stop': True} if is_plan else {})
                                            except Exception as e:
                                                if "43001" not in str(e) and "does not exist" not in str(e).lower():
                                                    logger.warning(f"撤銷掛單失敗 {ro['id']}: {e}")

                                        # 市價平倉 (reduce-only 確保不開反向單)
                                        order_side = 'sell' if side.lower() == 'long' else 'buy'
                                        close_params = {
                                            'reduceOnly': True, 'hedged': True,
                                            'holdSide': side.lower()
                                        }
                                        await exchange.create_order(symbol, 'market', order_side, size, None, params=close_params)

                                        sig['status'] = 'closed'
                                        save_active_signals(saved_signals)

                                        send_telegram_message(
                                            f"<b>🚨 跌破10MA 市價出場</b>\n\n"
                                            f"💎 {get_base_coin(symbol)} [{sig['direction']}]\n"
                                            f"📉 <b>原因:</b> 日線收盤跌破 10MA 且無保護止損\n"
                                            f"📊 <b>平倉數量:</b> {size}"
                                        )
                                        logger.info(f"✅ 跌破10MA市價平倉完成: {symbol}")
                                        continue
                        except Exception as e:
                            logger.warning(f"檢查10MA平倉異常 ({symbol}): {e}")

                    else:
                        # === 已保護止損：日線跌破10MA / 3D看跌吞噬 → 部分平倉 20% ===
                        try:
                            partial_log = sig.get('partial_close_log', {})
                            precision = sig.get('precision', 4)
                            current_price_pc = 0

                            ohlcv_1d_pc = await exchange.fetch_ohlcv(symbol, '1d', limit=20)
                            if ohlcv_1d_pc and len(ohlcv_1d_pc) >= 15:
                                df_1d_pc = pd.DataFrame(ohlcv_1d_pc, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
                                df_1d_pc['sma_10'] = df_1d_pc['close'].rolling(window=10).mean()
                                now_utc_pc = int(pd.Timestamp.now(tz='UTC').floor('1d').timestamp() * 1000)
                                closed_1d_pc = df_1d_pc[df_1d_pc['ts'] < now_utc_pc]

                                # --- 條件 1: 日線收盤跌破 10MA ---
                                if len(closed_1d_pc) > 0:
                                    last_bar_pc = closed_1d_pc.iloc[-1]
                                    bar_ts_1d = int(last_bar_pc['ts'])
                                    log_1d = partial_log.get('1d_10ma', [])

                                    if (pd.notna(last_bar_pc['sma_10'])
                                            and last_bar_pc['close'] < last_bar_pc['sma_10']
                                            and bar_ts_1d not in log_1d):
                                        if current_price_pc == 0:
                                            ticker_pc = await exchange.fetch_ticker(symbol)
                                            current_price_pc = float(ticker_pc['last'])
                                        close_qty = float(exchange.amount_to_precision(symbol, size * TP_CLOSE_PCT))

                                        # 20% 不足 6U → 全倉平倉
                                        if close_qty <= 0 or close_qty * current_price_pc < 6:
                                            close_qty = float(exchange.amount_to_precision(symbol, size))
                                            close_label = "全倉"
                                        else:
                                            close_label = "20%"

                                        if close_qty > 0 and close_qty * current_price_pc >= 6:
                                            order_side_pc = 'sell' if side.lower() == 'long' else 'buy'
                                            await exchange.create_order(symbol, 'market', order_side_pc, close_qty, None, params={
                                                'reduceOnly': True, 'hedged': True, 'holdSide': side.lower()
                                            })
                                            if '1d_10ma' not in partial_log:
                                                partial_log['1d_10ma'] = []
                                            partial_log['1d_10ma'].append(bar_ts_1d)
                                            sig['partial_close_log'] = partial_log
                                            save_active_signals(saved_signals)
                                            size = size - close_qty

                                            logger.info(
                                                f"📉 日線跌破10MA減倉 ({symbol}): {close_label} = {close_qty} | "
                                                f"收盤={last_bar_pc['close']:.{precision}f} < 10MA={last_bar_pc['sma_10']:.{precision}f}"
                                            )
                                            send_telegram_message(
                                                f"<b>📉 日線跌破10MA 減倉 ({close_label})</b>\n\n"
                                                f"💎 {get_base_coin(symbol)} [{sig['direction']}]\n"
                                                f"📊 <b>平倉數量:</b> {close_qty}\n"
                                                f"📊 <b>剩餘倉位:</b> {size:.{precision}f}"
                                            )

                                # --- 條件 2: 3D 看跌吞噬 (收盤 < 前一根實體低點) ---
                                if size > 0:
                                    composed_3d_pc = compose_3d_bars(ohlcv_1d_pc)
                                    if composed_3d_pc and len(composed_3d_pc) >= 2:
                                        df_3d_pc = pd.DataFrame(composed_3d_pc, columns=['ts', 'open', 'high', 'low', 'close', 'vol', 'close_ts'])
                                        now_utc_3d_pc = int(time.time() * 1000)
                                        closed_3d_pc = df_3d_pc[df_3d_pc['close_ts'] <= now_utc_3d_pc]

                                        if len(closed_3d_pc) >= 2:
                                            last_3d = closed_3d_pc.iloc[-1]
                                            prev_3d = closed_3d_pc.iloc[-2]
                                            bar_ts_3d = int(last_3d['close_ts'])
                                            log_3d = partial_log.get('3d_engulf', [])
                                            prev_body_low = min(prev_3d['open'], prev_3d['close'])

                                            if last_3d['close'] < prev_body_low and bar_ts_3d not in log_3d:
                                                if current_price_pc == 0:
                                                    ticker_pc = await exchange.fetch_ticker(symbol)
                                                    current_price_pc = float(ticker_pc['last'])
                                                close_qty = float(exchange.amount_to_precision(symbol, size * TP_CLOSE_PCT))

                                                if close_qty <= 0 or close_qty * current_price_pc < 6:
                                                    close_qty = float(exchange.amount_to_precision(symbol, size))
                                                    close_label = "全倉"
                                                else:
                                                    close_label = "20%"

                                                if close_qty > 0 and close_qty * current_price_pc >= 6:
                                                    order_side_pc = 'sell' if side.lower() == 'long' else 'buy'
                                                    await exchange.create_order(symbol, 'market', order_side_pc, close_qty, None, params={
                                                        'reduceOnly': True, 'hedged': True, 'holdSide': side.lower()
                                                    })
                                                    if '3d_engulf' not in partial_log:
                                                        partial_log['3d_engulf'] = []
                                                    partial_log['3d_engulf'].append(bar_ts_3d)
                                                    sig['partial_close_log'] = partial_log
                                                    save_active_signals(saved_signals)
                                                    size = size - close_qty

                                                    logger.info(
                                                        f"📉 3D看跌吞噬減倉 ({symbol}): {close_label} = {close_qty} | "
                                                        f"收盤={last_3d['close']} < 前根實體低={prev_body_low}"
                                                    )
                                                    send_telegram_message(
                                                        f"<b>📉 3D看跌吞噬 減倉 ({close_label})</b>\n\n"
                                                        f"💎 {get_base_coin(symbol)} [{sig['direction']}]\n"
                                                        f"📊 <b>平倉數量:</b> {close_qty}\n"
                                                        f"📊 <b>剩餘倉位:</b> {size:.{precision}f}"
                                                    )
                        except Exception as e:
                            logger.warning(f"檢查部分平倉條件異常 ({symbol}): {e}")

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
                # 追高防護：價格已達 5R 目標 -> 撤銷殘餘未成交進場單
                # 不論是否已有部分倉位，只要仍有進場掛單就檢查
                # ==============================================================
                if has_entry:
                    try:
                        ticker = await exchange.fetch_ticker(symbol)
                        current_price = float(ticker['last'])
                        original_sl = float(sig.get('original_sl_price', sig['sl_price']))
                        entry_price = float(sig['entry_price'])
                        risk = abs(entry_price - original_sl)

                        if risk > 0:
                            is_runaway = False
                            if direction == 'LONG' and current_price >= entry_price + 5 * risk:
                                is_runaway = True
                            elif direction == 'SHORT' and current_price <= entry_price - 5 * risk:
                                is_runaway = True

                            if is_runaway:
                                logger.info(f"🏃 價格已達 5R ({symbol})，撤銷未成交進場單 {signal_id}")
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
                                        f"<b>🏃 價格已達 5R (撤銷殘餘進場單)</b>\n\n"
                                        f"💎 <b>交易對:</b> {get_base_coin(symbol)} [{direction}]\n"
                                        f"📉 <b>狀態: 部分成交，殘餘進場單已撤銷，倉位繼續由 TP/SL 管理</b>"
                                    )
                                else:
                                    # 完全未成交 → 關閉訊號
                                    send_telegram_message(
                                        f"<b>🏃 價格已達 5R (錯失進場)</b>\n\n"
                                        f"💎 <b>交易對:</b> {get_base_coin(symbol)} [{direction}]\n"
                                        f"📉 <b>狀態: 已自動撤銷未成交之進場單</b>"
                                    )
                                    sig['status'] = 'closed'
                                continue

                        # 訊號淘汰條件撤單：若尚未進場，且日線收盤跌破 10MA 或最低點低於止損價，則作廢訊號撤單
                        if not is_runaway:
                            try:
                                entry_ts = int(sig.get('timestamp', 0))
                                ohlcv_1d = await exchange.fetch_ohlcv(symbol, '1d', limit=20)
                                if ohlcv_1d and len(ohlcv_1d) >= 15:
                                    df_1d = pd.DataFrame(ohlcv_1d, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
                                    df_1d['sma_10'] = df_1d['close'].rolling(window=10).mean()
                                    
                                    now_utc_1d_fl = int(pd.Timestamp.now(tz='UTC').floor('1d').timestamp() * 1000)
                                    closed_1d = df_1d[df_1d['ts'] < now_utc_1d_fl]
                                    
                                    if len(closed_1d) > 0:
                                        last_closed = closed_1d.iloc[-1]
                                        last_close_time = int(last_closed['ts']) + 86400000
                                        
                                        # 只檢查訊號產生之後的 K 棒
                                        if last_close_time > entry_ts:
                                            # 如果收盤低於 10MA，或最低價跌破原止損 (C2_low)
                                            if last_closed['close'] < last_closed['sma_10'] or last_closed['low'] < float(sig['original_sl_price']):
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
                                                    f"📉 <b>狀態: 日K跌破 10MA 或止損價，已自動撤銷進場單</b>"
                                                )
                                                sig['status'] = 'closed'
                                                continue
                            except Exception as e:
                                logger.warning(f"檢查訊號淘汰撤單異常 ({symbol}): {e}")

                        # 保護止損產生檢查：若尚未進場但 3D K 棒已滿足保護止損上移條件，撤銷掛單
                        if not is_runaway and sig.get('status') == 'active':
                            try:
                                entry_ts = int(sig.get('timestamp', 0))
                                ohlcv_1d_3d = await exchange.fetch_ohlcv(symbol, '1d', limit=30)
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
                ohlcv_1d_add = await exchange.fetch_ohlcv(sym, '1d', limit=20)
                if not ohlcv_1d_add or len(ohlcv_1d_add) < 15:
                    continue
                df_1d_add = pd.DataFrame(ohlcv_1d_add, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
                df_1d_add['sma_10'] = df_1d_add['close'].rolling(window=10).mean()
                now_utc_add = int(pd.Timestamp.now(tz='UTC').floor('1d').timestamp() * 1000)
                closed_1d_add = df_1d_add[df_1d_add['ts'] < now_utc_add]

                if len(closed_1d_add) > 0:
                    last_bar_add = closed_1d_add.iloc[-1]
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
                            f"📉 <b>狀態:</b> 日K收盤跌破 10MA，已自動撤銷未成交加倉單"
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

        ohlcv_1d = await exchange.fetch_ohlcv(symbol, '1d', limit=100)

        if not ohlcv_1d: return None
        df = pd.DataFrame(ohlcv_1d, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
        df = df.sort_values('ts').drop_duplicates(subset=['ts']).reset_index(drop=True)
        df['dt'] = pd.to_datetime(df['ts'], unit='ms', utc=True)

        # 排除未收盤的當日 K 棒
        now_utc_1d_fl = int(pd.Timestamp.now(tz='UTC').floor('1d').timestamp() * 1000)
        df_1d = df[df['ts'] < now_utc_1d_fl].reset_index(drop=True)

        if len(df_1d) < 15: return None

        # 計算日線 10MA
        df_1d['sma_10'] = df_1d['close'].rolling(window=10).mean()

        target_info_c1 = None
        target_info_c2 = None
        target_info_gap = None
        state = 0
        c2_low = 0.0
        # 動態保護止損追蹤（掃描器內部模擬 3D K 棒，使用固定 epoch 對齊）
        dynamic_sl = 0.0
        entry_price_scan = 0.0
        scan_3d_groups = {}   # {group_key: [bar_dict, ...]}
        scan_3d_last = None   # 上一根已完成的 3D 棒
        EPOCH_MS = 1483228800000  # 2017-01-01 00:00 UTC
        PERIOD_MS = 3 * 24 * 3600 * 1000
        
        # 1. 由舊到新執行狀態機掃描
        # State 0: 尋找條件一
        # State 1: 條件一成立，彈性等待條件二 (只要守穩 10MA 即可)
        # State 3: 條件二成立，監控淘汰條件
        for i in range(10, len(df_1d)):
            bar = df_1d.iloc[i]
            prev1 = df_1d.iloc[i-1]
            prev2 = df_1d.iloc[i-2]
            
            prev1_body_high = max(prev1['open'], prev1['close'])
            prev2_body_high = max(prev2['open'], prev2['close'])
            
            if state == 0:
                # 條件一：收盤價小於前兩根實體高點 (回檔)，且這三天收盤價均大於 10MA
                c1_met = (
                    bar['close'] < prev1_body_high and bar['close'] < prev2_body_high and 
                    bar['close'] > bar['sma_10'] and prev1['close'] > prev1['sma_10'] and prev2['close'] > prev2['sma_10']
                )
                
                if c1_met:
                    state = 1
                    target_info_c1 = {
                        'dt_str': bar['dt'].isoformat(),
                        'ts': int(bar['ts']),
                        'close': float(bar['close']),
                        'high': float(bar['high']),
                        'low': float(bar['low'])
                    }
                    target_info_c2 = None
            elif state == 1:
                # 彈性等待期：條件一成立後，尋找條件二 (可不連續日期)
                if bar['close'] < bar['sma_10']:
                    # 跌破 10MA，條件失效，重新尋找條件一
                    state = 0
                    target_info_c1 = None
                    target_info_gap = None
                    
                    # 順便檢查今天是否剛好符合新的條件一
                    c1_met = (
                        bar['close'] < prev1_body_high and bar['close'] < prev2_body_high and 
                        bar['close'] > bar['sma_10'] and prev1['close'] > prev1['sma_10'] and prev2['close'] > prev2['sma_10']
                    )
                    if c1_met:
                        state = 1
                        target_info_c1 = {
                            'dt_str': bar['dt'].isoformat(),
                            'ts': int(bar['ts']),
                            'close': float(bar['close']),
                            'high': float(bar['high']),
                            'low': float(bar['low'])
                        }
                elif bar['close'] > max(prev1_body_high, prev2_body_high):
                    # 條件二成立：收盤大於「前面兩根」的實體高點，且止損距離 <= 10%
                    sl_distance = (bar['close'] - bar['low']) / bar['close'] if bar['close'] > 0 else 1.0
                    if sl_distance <= 0.10:
                        state = 3
                        target_info_c2 = {
                            'dt_str': bar['dt'].isoformat(),
                            'ts': int(bar['ts']),
                            'close': float(bar['close']),
                            'high': float(bar['high']),
                            'low': float(bar['low'])
                        }
                        c2_low = float(bar['low'])
                        dynamic_sl = c2_low
                        entry_price_scan = float(bar['close'])
                    else:
                        # 止損距離過大，訊號直接失效，重新尋找條件一
                        state = 0
                        target_info_c1 = None
                        target_info_gap = None
            elif state == 3:
                # 1. 計算目前的 3D K 棒 (使用全部歷史資料到今天為止，確保與實盤 100% 同步)
                historical_1d = df_1d.iloc[:i+1]
                ohlcv_1d_list = historical_1d[['ts', 'open', 'high', 'low', 'close', 'vol']].values.tolist()
                composed_3d = compose_3d_bars(ohlcv_1d_list)
                
                # 從 C2 開始重新推演 dynamic_sl
                current_sl = c2_low
                entry_ts_scan = target_info_c2['ts']
                
                # 過濾出在 C2 之後才「收盤」的 3D K 棒
                for idx, b_3d in enumerate(composed_3d):
                    b_close_time = b_3d[6]
                    if b_close_time > entry_ts_scan and idx >= 1:
                        prev_b = composed_3d[idx-1]
                        prev_body_high = max(prev_b[1], prev_b[4]) # open, close
                        b_close = b_3d[4]
                        b_low = b_3d[3]
                        if b_close > prev_body_high and b_low > entry_price_scan:
                            if b_low > current_sl:
                                current_sl = b_low
                
                dynamic_sl = current_sl

                # 2. 條件二已成立，使用最新保護止損監控淘汰條件
                if bar['low'] < dynamic_sl or bar['close'] < bar['sma_10']:
                    state = 0
                    target_info_c1 = None
                    target_info_c2 = None
                    target_info_gap = None
                    dynamic_sl = 0.0
                    
                    # 重新判斷是否立即觸發新的條件一
                    c1_met = (
                        bar['close'] < prev1_body_high and bar['close'] < prev2_body_high and 
                        bar['close'] > bar['sma_10'] and prev1['close'] > prev1['sma_10'] and prev2['close'] > prev2['sma_10']
                    )
                    if c1_met:
                        state = 1
                        target_info_c1 = {
                            'dt_str': bar['dt'].isoformat(),
                            'ts': int(bar['ts']),
                            'close': float(bar['close']),
                            'high': float(bar['high']),
                            'low': float(bar['low'])
                        }

        # State 0: 無有效訊號
        if state == 0:
            return {'symbol': symbol, 'action': 'remove'}

        # State 1: 條件一成立，等待條件二 (關注中)
        # State 3: 條件二已成立 (觸發進場)
        is_watchlist_eligible = True
        target_info = target_info_c1 if state == 1 else target_info_c2
        
        action = 'update'
        if cached_info and target_info['ts'] == cached_info.get('ts', 0):
            action = 'keep'

        is_trigger_met = False
        entry_price = 0.0
        stop_loss = 0.0
        trigger_ts = 0

        if state == 3:
            is_trigger_met = True
            entry_price = float(target_info_c2['close'])
            stop_loss = float(target_info_c2['low'])
            trigger_ts = int(target_info_c2['ts'])

        c1_date_str = pd.to_datetime(target_info_c1['dt_str']).strftime('%Y-%m-%d') if target_info_c1 else "未知"
        c2_date_str = pd.to_datetime(target_info_c2['dt_str']).strftime('%Y-%m-%d') if target_info_c2 else "未知"
        gap_date_str = "省略"

        return {
            'symbol': symbol,
            'action': action,
            'data': target_info,
            'is_trigger_met': is_trigger_met,
            'is_watchlist_eligible': is_watchlist_eligible,
            'entry_price': entry_price,
            'stop_loss': stop_loss,
            'trigger_ts': trigger_ts,
            'precision': precision,
            'd1_date': gap_date_str,
            'c1_date': c1_date_str,
            'c2_date': c2_date_str
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

    # 按 c1_date 分組
    date_groups = {}
    for item in item_list:
        d = item.get('c1_date', '未知日期')
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
            elif 'protect_sl' in item:
                psl = item['protect_sl']
                psl_str = f"{int(psl)}" if isinstance(psl, float) and psl.is_integer() else f"{psl}"
                coin_strs.append(f"{base} (保護止損：{psl_str})")
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
    c1_d = item.get('c1_date', '未知')
    c2_d = item.get('c2_date', '未知')

    # 倉位價值 = 預設虧損金額 / |(進場價 - 止損價) / 進場價|
    loss_pct = abs((entry - sl) / entry) if entry != 0 else 0
    position_value = default_loss / loss_pct if loss_pct > 0 else 0

    msg = (
        f"💎 <b>交易對:</b> {display_symbol}\n\n"
        f"📅 <b>條件一日期:</b> <code>{c1_d}</code>\n"
        f"📅 <b>條件二日期:</b> <code>{c2_d}</code>\n"
        f"📍 <b>進場價格:</b> <code>{entry:.{precision}f}</code>\n"
        f"🛡️ <b>止損價格:</b> <code>{sl:.{precision}f}</code>\n"
        f"💰 <b>倉位價值:</b> <code>{position_value:.2f} USDT</code>"
    )
    send_telegram_message(msg)

def send_holding_trigger_message(item, default_loss, protect_sl):
    """持倉中偵測到加倉訊號，顯示加倉計算資訊"""
    display_symbol = get_base_coin(item['symbol'])
    precision = item['precision']
    entry = item['entry_price']
    c1_d = item.get('c1_date', '未知')
    c2_d = item.get('c2_date', '未知')

    half_loss = default_loss / 2
    risk = abs(entry - protect_sl)
    position_value = (half_loss / risk) * entry if risk > 0 else 0

    msg = (
        f"<b>📈 [持倉中] 加倉訊號</b>\n\n"
        f"💎 <b>交易對:</b> {display_symbol}\n\n"
        f"📅 <b>條件一日期:</b> <code>{c1_d}</code>\n"
        f"📅 <b>條件二日期:</b> <code>{c2_d}</code>\n"
        f"📍 <b>進場價格:</b> <code>{entry:.{precision}f}</code>\n"
        f"🛡️ <b>保護止損:</b> <code>{protect_sl:.{precision}f}</code>\n"
        f"💰 <b>加倉價值:</b> <code>{position_value:.2f} USDT</code>\n"
        f"💵 <b>風險金額:</b> <code>{half_loss:.1f} USDT</code>"
    )
    send_telegram_message(msg)

def send_system_settings_message(config):
    """獨立一則系統設定訊息"""
    loss = config.get("default_loss_amount", 6)

    msg = (
        f"⚙️ <b>系統快速設定</b>\n\n"
        f"💵 <b>預設虧損金額:</b> {loss} USDT"
    )

    reply_markup = {
        "inline_keyboard": [
            [{"text": "📝 修改預設虧損", "switch_inline_query_current_chat": "/set_loss "}]
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
                        reply = f"✅ 預設虧損金額已更新為 <b>{new_val} USDT</b>"
                        logger.info(f"⚙️ /set_loss 指令: 虧損金額更新為 {new_val}")
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
                    holding_map[sym] = {'symbol': sym, 'c1_date': dt_str}
                    
        for p in existing_positions:
            if p['side'].upper() == 'LONG':
                sym = p['symbol']
                if sym not in holding_map:
                    holding_map[sym] = {'symbol': sym, 'c1_date': '外部建倉'}

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

        # === 處理持倉保護止損 (持久化 + 實際更新 SL 單) ===
        if holding_items and BITGET_API_KEY:
            for item in holding_items:
                sym = item['symbol']
                try:
                    entry_price = 0
                    entry_time = 0
                    matched_sig = None
                    for slist in signals.values():
                        for s in slist:
                            if s['symbol'] == sym and s['status'] == 'active':
                                entry_price = float(s['entry_price'])
                                entry_time = int(s.get('timestamp', 0))
                                matched_sig = s
                                break
                    if entry_price > 0 and matched_sig:
                        current_sl = float(matched_sig['sl_price'])
                        batch_1d = await ex.fetch_ohlcv(sym, '1d', limit=30)
                        batch = compose_3d_bars(batch_1d)
                        if batch:
                            df_3d = pd.DataFrame(batch, columns=['ts', 'open', 'high', 'low', 'close', 'vol', 'close_ts'])
                            now_utc_3d = int(time.time() * 1000)
                            closed_3d = df_3d[df_3d['close_ts'] <= now_utc_3d]
                            original_sl = float(matched_sig.get('original_sl_price', 0))
                            candidate_sl = original_sl
                            
                            for idx in range(1, len(closed_3d)):
                                curr_b = closed_3d.iloc[idx]
                                prev_b = closed_3d.iloc[idx-1]
                                if curr_b['close_ts'] > entry_time:
                                    prev_body_high = max(prev_b['open'], prev_b['close'])
                                    if curr_b['close'] > prev_body_high and curr_b['low'] > entry_price:
                                        if curr_b['low'] > candidate_sl:
                                            candidate_sl = curr_b['low']
                            
                            # 若重新推演出的正確止損不等於當前檔案記錄，強制更新/修復
                            if candidate_sl != current_sl:
                                precision = matched_sig.get('precision', 4)
                                old_sl = current_sl
                                matched_sig['sl_price'] = candidate_sl
                                save_active_signals(signals)
                                
                                if candidate_sl > old_sl:
                                    logger.info(f"🛡️ 保護止損上移: {sym} | {old_sl:.{precision}f} → {candidate_sl:.{precision}f}")
                                    send_telegram_message(
                                        f"🛡️ <b>保護止損已上移</b>\n\n"
                                        f"💎 {get_base_coin(sym)}\n"
                                        f"📍 <code>{old_sl:.{precision}f}</code> → <code>{candidate_sl:.{precision}f}</code>"
                                    )
                                else:
                                    logger.info(f"🔧 保護止損向下修復(修正舊算法殘留錯誤): {sym} | {old_sl:.{precision}f} → {candidate_sl:.{precision}f}")
                                    
                            # 只有當保護止損確實大於初始止損時，才在推播中顯示
                            if candidate_sl > original_sl:
                                item['protect_sl'] = float(candidate_sl)
                except Exception as e:
                    logger.warning(f"獲取保護止損失敗 ({sym}): {e}")

        # === 排序與推播 ===
        # 1. 新進場訊號 (Triggered)
        for item in real_new_triggers:
            send_triggered_message(item, default_loss)

        # 1.5 持倉中加倉訊號
        for item in real_holding_new_triggers:
            sym = item['symbol']
            matched_sig = None
            for slist in signals.values():
                for s in slist:
                    if s['symbol'] == sym and s['status'] == 'active':
                        matched_sig = s
                        break
                if matched_sig:
                    break

            if not matched_sig:
                continue

            protect_sl = float(matched_sig['sl_price'])
            original_sl = float(matched_sig.get('original_sl_price', matched_sig['sl_price']))

            # 只有保護止損已上移時才執行加倉 (止損未上移代表風險期，不適合加倉)
            if protect_sl <= original_sl:
                logger.info(f"⏭️ 跳過加倉 ({sym}): 保護止損尚未上移")
                continue

            trigger_ts = item.get('trigger_ts', 0)
            added_list = matched_sig.get('added_signals', [])
            if trigger_ts in added_list:
                logger.info(f"⏭️ 跳過加倉 ({sym}): trigger_ts={trigger_ts} 已處理過")
                continue

            # 推播加倉訊號資訊
            send_holding_trigger_message(item, default_loss, protect_sl)

            if BITGET_API_KEY:
                half_loss = default_loss / 2
                parent_signal_id = matched_sig['signal_id']
                order = await place_add_order(
                    ex, sym, 'LONG', item['entry_price'], protect_sl,
                    item['precision'], half_loss, trigger_ts, parent_signal_id
                )
                if order:
                    if 'added_signals' not in matched_sig:
                        matched_sig['added_signals'] = []
                    matched_sig['added_signals'].append(trigger_ts)
                    save_active_signals(signals)
            
        # 2. 持倉中 (Holding)
        if holding_items:
            send_grouped_message(holding_items, "💼 <b>[持倉中]</b>")
            
        # 3. 關注中 (Watching)
        if real_watching:
            send_grouped_message(real_watching, "👀 <b>[關注中]</b>")

        # 4. 系統設定 (System Settings)
        if real_new_triggers or holding_items or real_watching or real_holding_new_triggers:
            send_system_settings_message(config)

        active_count = len(watchlist)
        logger.info(f"✅ 掃描完成。新觸發: {len(real_new_triggers)} / 持倉: {len(holding_items)} / 持倉新訊號: {len(real_holding_new_triggers)} / 關注: {len(real_watching)} / 追蹤總數: {active_count}")

        if not real_new_triggers and not holding_items and not real_watching:
            send_telegram_message(f"✅ <b>條件掃描完成</b>\n本次共掃描 {total_coins} 個幣種，無滿足條件標的。\n(當前清單追蹤中: {active_count} 個)")
    finally:
        await ex.close()

async def scheduler():
    last_day = -1
    last_tw_day = -1
    
    # 初始執行：加密貨幣
    try:
        await run_scan()
    except Exception as e:
        logger.error(f"初始掃描異常: {e}")

    # 初始執行：台股 (無條件強制掃描)
    try:
        await tw_scanner.main_loop()
    except Exception as e:
        logger.error(f"台股初始掃描異常: {e}")

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

            # 台股每日 UTC 06:30~06:40 (台灣時間 14:30) 觸發一次掃描
            if now.hour == 6 and now.minute >= 30 and now.minute <= 40 and now.day != last_tw_day:
                try:
                    await tw_scanner.main_loop()
                except Exception as e:
                    logger.error(f"台股定時掃描異常: {e}")
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
