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

def is_crypto_symbol(symbol: str, blacklist: list) -> bool:
    if not blacklist:
        return True
    base = symbol.split('/')[0]
    return not any(base == p or base.startswith(p) for p in blacklist)

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
# 交易執行
# ============================================================================

async def place_order(exchange, symbol, direction, entry, sl, precision, fixed_loss_usdt):
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
                available = float(balance.get('USDT', {}).get('free', 0))
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
                    'timestamp': int(time.time() * 1000)
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

        logger.error(f"❌ 所有槓桿策略均已失效 ({symbol}), 最後錯誤: {last_error}")
        return None
    except Exception as e:
        logger.error(f"下單執行異常 ({symbol}): {e}")
        return None

# ============================================================================
# 無限階梯止盈管理
# ============================================================================

async def ensure_next_tp(exchange, symbol, side, sig, size, saved_signals, open_orders):
    """
    無限階梯 TP：每 5R 平掉剩餘倉位的 20%。
    同一時間只存在一張 TP 觸發單，成交後自動推進至下一階。
    當 TP 數量低於最小名義價值時停止，剩餘粉塵由 SL 保護。
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

        # 檢查當前階 TP 單是否仍在掛單中
        has_order = any(tp_coid in get_coid(o) for o in open_orders)

        if has_order:
            return  # 等待成交

        # TP 單不存在：判斷是已成交還是從未掛出
        # 依原始數量推算該階成交後的預期剩餘
        original_qty = sig['quantity']
        expected_remaining = original_qty * (0.80 ** (next_tier + 1))

        if size <= expected_remaining * 1.05:
            # 倉位已縮減至預期水準 → 上一階已成交，推進至下一階
            sig['tp_next_tier'] = next_tier + 1
            next_tier = sig['tp_next_tier']
            tp_coid = f"tp{next_tier + 1}_{signal_id}"
            save_active_signals(saved_signals)
            logger.info(f"🎯 TP Tier {next_tier} 偵測已成交，推進至 Tier {next_tier + 1}")

            # 通知
            send_telegram_message(
                f"<b>🎯 TP{next_tier} 成交</b>\n\n"
                f"💎 {get_base_coin(symbol)} [{direction}]\n"
                f"📊 剩餘倉位: {size:.{precision}f}"
            )

        # 計算當階 TP 價格與數量
        tp_price = entry + (next_tier + 1) * TP_STEP_R * risk if direction == 'LONG' \
            else entry - (next_tier + 1) * TP_STEP_R * risk
        tp_price = round(tp_price, precision)
        tp_qty = float(exchange.amount_to_precision(symbol, size * TP_CLOSE_PCT))

        # 最小名義價值檢查
        if tp_qty <= 0 or tp_qty * entry < 5:
            logger.debug(f"  TP{next_tier + 1} 數量低於最小門檻，停止掛單 (粉塵由 SL 保護)")
            return

        order_side = 'sell' if direction == 'LONG' else 'buy'
        tp_params = {
            'triggerPrice': tp_price, 'triggerType': 'fill_price', 'reduceOnly': True,
            'hedged': True, 'holdSide': 'long' if direction == 'LONG' else 'short',
            'clientOid': tp_coid
        }

        logger.info(f"🚀 掛出 TP{next_tier + 1}: {symbol} @ {tp_price:.{precision}f} | qty: {tp_qty} | ID: {tp_coid}")
        try:
            await exchange.create_order(symbol, 'market', order_side, tp_qty, None, params=tp_params)
        except Exception as e:
            if '40786' in str(e):
                logger.warning(f"⚠️ TP ID 已存在 ({tp_coid})，視為已掛單。")
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
                            if is_sl_side and abs(trig_p - sl_price_target) < 1e-6:
                                my_sl_orders.append(o)

                    if len(my_sl_orders) > 1:
                        for dup in my_sl_orders[1:]:
                            await exchange.cancel_order(dup['id'], symbol, params={'stop': True})

                    # B. SL 價格不一致偵測：保護止損上移後，撤銷舊單讓補掛機制用新價格重新掛出
                    if len(my_sl_orders) == 1:
                        existing_trig = float(my_sl_orders[0].get('triggerPrice', 0) or my_sl_orders[0].get('info', {}).get('triggerPrice') or 0)
                        if abs(existing_trig - sl_price_target) > 1e-6:
                            logger.info(f"🔄 SL 價格不一致 ({symbol}): 掛單={existing_trig} vs 目標={sl_price_target}，撤銷舊單")
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

                has_entry = any(str(o.get('clientOid') or o.get('clientOrderId')) == str(signal_id)
                                for o in open_orders)

                # ==============================================================
                # 新增追高防護：尚未進場，但價格已達 5R 目標 -> 撤銷掛單
                # ==============================================================
                if not has_pos and has_entry:
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

                                send_telegram_message(
                                    f"<b>🏃 價格已達 5R (錯失進場)</b>\n\n"
                                    f"💎 <b>交易對:</b> {get_base_coin(symbol)} [{direction}]\n"
                                    f"📉 <b>狀態: 已自動撤銷未成交之進場單</b>"
                                )
                                sig['status'] = 'closed'
                                continue
                    except Exception as e:
                        logger.warning(f"檢查未成交單價格異常 ({symbol}): {e}")
                # ==============================================================

                if not has_pos and not has_entry:
                    logger.info(f"🧹 偵測到歸零/孤兒訊號 {sig_key}，開始溯源清理...")

                    close_reason = None
                    try:
                        ticker = await exchange.fetch_ticker(symbol)
                        current_price = float(ticker['last'])
                        sl_price = float(sig['sl_price'])
                        entry_price = float(sig['entry_price'])
                        tp_next = sig.get('tp_next_tier', 0)
                        risk = abs(entry_price - sl_price)

                        # 最近一階 TP 價格
                        if tp_next > 0 and risk > 0:
                            last_tp_price = entry_price + tp_next * TP_STEP_R * risk if direction == 'LONG' \
                                else entry_price - tp_next * TP_STEP_R * risk
                        else:
                            last_tp_price = None

                        eps = 0.003
                        is_sl_hit = (direction == 'LONG' and current_price <= sl_price * (1 + eps)) or \
                                    (direction == 'SHORT' and current_price >= sl_price * (1 - eps))

                        is_tp_region = False
                        if last_tp_price:
                            is_tp_region = (direction == 'LONG' and current_price >= last_tp_price * (1 - eps)) or \
                                           (direction == 'SHORT' and current_price <= last_tp_price * (1 + eps))

                        if is_tp_region:
                            close_reason = "🎉 TP 止盈達成"
                        elif is_sl_hit:
                            close_reason = "🛑 止損出場"
                        else:
                            close_reason = "⚠️ 外部干預 (異常平倉)"
                    except Exception as e:
                        logger.warning(f"溯源價格查詢失敗 ({symbol}): {e}")
                        close_reason = "⚠️ 倉位已消失 (無法判定原因)"

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

                    if close_reason:
                        msg = (f"<b>{close_reason}</b>\n\n"
                               f"💎 <b>交易對:</b> {get_base_coin(symbol)}\n"
                               f"📉 <b>當前狀態: 倉位已清結</b>")
                        send_telegram_message(msg)

                    sig['status'] = 'closed'

            saved_signals[sig_key] = [s for s in sig_list if s['status'] == 'active']
            if not saved_signals[sig_key]:
                del saved_signals[sig_key]

        # 3. 盲目孤兒單清理 (訂單找名單)
        # Entry 格式: 3d_{signal_id}, TP 格式: tp{n}_{signal_id}, SL 格式: sl_{signal_id}
        tp_prefix_pattern = re.compile(r'^tp\d+_')
        all_active_ids = [str(s['signal_id']) for slist in saved_signals.values() for s in slist]
        for oo in open_orders:
            co_id = str(oo.get('clientOrderId') or oo.get('info', {}).get('clientOid') or "")
            is_sl = co_id.startswith("sl_")
            is_tp = bool(tp_prefix_pattern.match(co_id))
            is_entry = co_id.startswith("3d_")
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
        latest_closed_3d = df_3d.iloc[-1]

        target_info = None
        action = None
        # 延遲淘汰旗標：3D 淘汰條件觸發時先暫存，等 3H 進場檢查後再決定
        pending_removal = False
        must_remove = False

        # 1. 掃描過去 20 根已收盤的 3D K 棒 (由舊到新，找到存活的最舊圖型即鎖定)
        scan_limit = min(20, len(df_3d) - 2)
        for i in range(scan_limit, 0, -1):
            row_c = df_3d.iloc[-i]
            row_b = df_3d.iloc[-i - 1]
            row_a = df_3d.iloc[-i - 2]

            if pd.isna(row_c['ma20']):
                continue

            a_is_bearish = row_a['close'] < row_a['open']
            b_is_bearish = row_b['close'] < row_b['open']
            b_engulf_a = row_b['close'] < row_a['close']
            c_is_bullish = row_c['close'] > row_c['open']
            c_engulf_b = row_c['close'] > row_b['open']
            c_above_ma20 = row_c['close'] > row_c['ma20']

            if a_is_bearish and b_is_bearish and b_engulf_a and c_is_bullish and c_engulf_b and c_above_ma20:
                # 找到圖型，立即檢驗是否被後續 3D K 棒淘汰
                is_invalid = False
                for j in range(1, i):
                    check_bar = df_3d.iloc[-j]
                    if check_bar['close'] > row_c['high'] or check_bar['low'] <= row_c['low'] or check_bar['close'] < check_bar['ma20']:
                        is_invalid = True
                        break
                
                if not is_invalid:
                    # 存活的最舊訊號，保留 target_info 供後續 3H 檢查，並提早鎖定
                    target_info = {
                        'dt_str': row_c['dt'].isoformat(),
                        'close': float(row_c['close']),
                        'high': float(row_c['high']),
                        'low': float(row_c['low']),
                        'ma20': float(row_c['ma20'])
                    }
                    action = 'update'
                    pending_removal = False
                    must_remove = False
                    break
                else:
                    if j == 1:
                        # 造成淘汰的是最新已收盤 3D 棒 → 可能為 00:00 邊界，延遲至 3H 檢查
                        target_info = {
                            'dt_str': row_c['dt'].isoformat(),
                            'close': float(row_c['close']),
                            'high': float(row_c['high']),
                            'low': float(row_c['low']),
                            'ma20': float(row_c['ma20'])
                        }
                        pending_removal = True
                        must_remove = False
                    else:
                        # 造成淘汰的是更早的 3D 棒，標記必刪，但繼續往近期尋找是否有存活的新訊號
                        must_remove = True
                        target_info = None
                        pending_removal = False
                    continue

        # 如果整個區間掃描完都沒有存活訊號，且中間曾發現過已確定死透的舊訊號，直接淘汰
        if target_info is None and must_remove:
            return {'symbol': symbol, 'action': 'remove'}

        # 2. 如果沒有新訊號，但仍在追蹤名單中，執行剔除判斷 (針對最新已收盤 3D 進行檢驗)
        if target_info is None and cached_info is not None:
            if latest_closed_3d['close'] > cached_info['high'] or latest_closed_3d['low'] <= cached_info['low'] or latest_closed_3d['close'] < latest_closed_3d['ma20']:
                # 3D 淘汰條件觸發，但需等 3H 檢查完才能最終決定
                target_info = cached_info
                pending_removal = True
            else:
                target_info = cached_info
                action = 'keep'

        # 如果既沒新訊號且也不在追蹤名單中，直接結束
        if target_info is None:
            return None

        # === 3H 條件進階判斷 (針對 target_info 進行校驗) ===
        # 擴大獲取 60 天 1H 資料 (約 1440 筆)，使用迴圈分批拉取
        fetch_since_1h = now_ms - (60 * 24 * 3600 * 1000)
        ohlcv_1h = []
        curr_1h = fetch_since_1h
        for _ in range(3):
            batch_1h = await exchange.fetch_ohlcv(symbol, '1h', since=curr_1h, limit=500)
            if not batch_1h: break
            ohlcv_1h.extend(batch_1h)
            curr_1h = batch_1h[-1][0] + (3600 * 1000)
            if curr_1h >= now_ms: break

        is_3h_met = False
        entry_price = 0.0
        stop_loss = 0.0
        trigger_ts = 0

        if ohlcv_1h:
            df_1h = pd.DataFrame(ohlcv_1h, columns=['ts', 'open', 'high', 'low', 'close', 'vol']).drop_duplicates(subset=['ts']).reset_index(drop=True)
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

            # 3H 觸發 + SL/10R 失效回退邏輯
            best_protect_sl = 0.0
            for _, h3_row in df_3h_after_c.iterrows():
                h3_open = h3_row['open']
                h3_close = h3_row['close']
                h3_low = h3_row['low']
                h3_high = h3_row['high']
                h3_ts = h3_row['ts']

                if is_3h_met:
                    # 原始風險 (用於 10R 計算，不受保護止損影響)
                    risk = entry_price - stop_loss

                    # 日線保護止損更新 (只能往上，不能往下)
                    closed_1d = df[df['ts'] + 86400000 <= h3_ts]
                    if len(closed_1d) >= 2:
                        last_closed = closed_1d.iloc[-1]
                        prev_closed = closed_1d.iloc[-2]
                        last_close_time = last_closed['ts'] + 86400000
                        if last_close_time > trigger_ts:
                            prev_body_high = max(prev_closed['open'], prev_closed['close'])
                            if last_closed['close'] > prev_body_high and last_closed['low'] > entry_price:
                                candidate_sl = last_closed['low']
                                if candidate_sl > best_protect_sl:
                                    best_protect_sl = candidate_sl

                    # 有效止損 = 原始 SL 與保護 SL 取較高者
                    effective_sl = max(stop_loss, best_protect_sl) if best_protect_sl > 0 else stop_loss

                    # 碰觸有效止損或價格已達 5R → 失效回退
                    if h3_low <= effective_sl or (risk > 0 and h3_high >= entry_price + 5 * risk):
                        is_3h_met = False
                        entry_price = 0.0
                        stop_loss = 0.0
                        trigger_ts = 0
                        best_protect_sl = 0.0
                
                if not is_3h_met:
                    # 尋找新觸發：開盤在 3D High 以下，收盤突破 3D High
                    if h3_open <= target_3d_high and h3_close > target_3d_high:
                        is_3h_met = True
                        entry_price = h3_close
                        stop_loss = h3_low
                        trigger_ts = int(h3_row['ts'])
                        best_protect_sl = 0.0

            # 額外檢查：當前未收盤的 3H K 棒是否已碰觸止損或止盈(5R)
            if is_3h_met:
                current_3h_candles = df_1h[df_1h['3h_period'] >= now_utc_3h_fl]
                if not current_3h_candles.empty:
                    current_3h_low = current_3h_candles['low'].min()
                    current_3h_high = current_3h_candles['high'].max()
                    current_ts = int(time.time() * 1000)
                    risk = entry_price - stop_loss

                    # 日線保護止損更新 (只能往上，不能往下)
                    closed_1d = df[df['ts'] + 86400000 <= current_ts]
                    if len(closed_1d) >= 2:
                        last_closed = closed_1d.iloc[-1]
                        prev_closed = closed_1d.iloc[-2]
                        last_close_time = last_closed['ts'] + 86400000
                        if last_close_time > trigger_ts:
                            prev_body_high = max(prev_closed['open'], prev_closed['close'])
                            if last_closed['close'] > prev_body_high and last_closed['low'] > entry_price:
                                candidate_sl = last_closed['low']
                                if candidate_sl > best_protect_sl:
                                    best_protect_sl = candidate_sl

                    effective_sl = max(stop_loss, best_protect_sl) if best_protect_sl > 0 else stop_loss
                    if current_3h_low <= effective_sl or (risk > 0 and current_3h_high >= entry_price + 5 * risk):
                        is_3h_met = False
                        entry_price = 0.0
                        stop_loss = 0.0
                        trigger_ts = 0
        # ========================

        # 延遲淘汰最終裁決：精確鎖定 00:00 邊界情境
        # 條件：3H 進場來自最新 3H 棒，且該 3H 棒落在最新已收盤 3D 棒的區間內
        if pending_removal:
            # 1H 資料缺失或 df_3h 為空時無法進行 3H 判斷，直接淘汰
            try:
                has_3h_data = ohlcv_1h and len(df_3h) > 0
            except NameError:
                has_3h_data = False
            if not has_3h_data:
                return {'symbol': symbol, 'action': 'remove'}
            last_3h_ts = int(df_3h.iloc[-1]['ts']) if (len(df_3h) > 0) else 0
            latest_3d_ts = int(latest_closed_3d['ts'])
            latest_3d_end_ts = latest_3d_ts + 3 * 86400 * 1000
            is_boundary = (
                is_3h_met and
                last_3h_ts > 0 and
                trigger_ts == last_3h_ts and
                last_3h_ts >= latest_3d_ts and
                last_3h_ts < latest_3d_end_ts
            )
            if is_boundary:
                action = 'update'
                logger.info(f"🔀 {symbol}: 3H 進場於最新 3D 收盤邊界成立，覆蓋淘汰")
            else:
                return {'symbol': symbol, 'action': 'remove'}

        d1_date_str = pd.to_datetime(target_info['dt_str']).strftime('%Y-%m-%d')

        return {
            'symbol': symbol,
            'action': action,
            'data': target_info,
            'is_3h_met': is_3h_met,
            'entry_price': entry_price,
            'stop_loss': stop_loss,
            'trigger_ts': trigger_ts,
            'precision': precision,
            'd1_date': d1_date_str
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

    # 按 d1_date 分組
    date_groups = {}
    for item in item_list:
        d = item.get('d1_date', '未知日期')
        if d not in date_groups:
            date_groups[d] = []
        date_groups[d].append(item)

    lines = [f"<b>{title}</b>\n"]
    for date_key in sorted(date_groups.keys()):
        coin_strs = []
        for item in date_groups[date_key]:
            base = get_base_coin(item['symbol'])
            if 'protect_sl' in item:
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
    """3H 已成立的幣種，獨立一則訊息，含倉位價值"""
    display_symbol = get_base_coin(item['symbol'])
    precision = item['precision']
    entry = item['entry_price']
    sl = item['stop_loss']

    # 倉位價值 = 預設虧損金額 / |(進場價 - 止損價) / 進場價|
    loss_pct = abs((entry - sl) / entry) if entry != 0 else 0
    position_value = default_loss / loss_pct if loss_pct > 0 else 0

    msg = (
        f"💎 <b>交易對:</b> {display_symbol}\n"
        f"📅 <b>吞噬轉換起始日期:</b> {item['d1_date']}\n\n"
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
        f"🚫 <b>黑名單前綴:</b> {bl_str}"
    )
    
    reply_markup = {
        "inline_keyboard": [
            [{"text": "📝 修改預設虧損", "switch_inline_query_current_chat": "/set_loss "}],
            [{"text": "➕ 新增黑名單", "switch_inline_query_current_chat": "/add_blacklist "},
             {"text": "➖ 移除黑名單", "switch_inline_query_current_chat": "/remove_blacklist "}]
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

            elif text.startswith("/add_blacklist"):
                parts = text.split()
                if len(parts) >= 2:
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
                    reply = "❌ 格式錯誤，未提供幣種名稱。"

                send_url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
                payload = {"chat_id": chat_id, "text": reply, "parse_mode": "HTML"}
                requests.post(send_url, json=payload, timeout=10)

            elif text.startswith("/remove_blacklist"):
                parts = text.split()
                if len(parts) >= 2:
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
    default_loss = config.get("default_loss_amount", 6)

    try:
        try:
            markets = await ex.load_markets()
            coins = [s for s, m in markets.items() if m.get('linear') and m.get('quote') == 'USDT' and is_crypto_symbol(s, config.get("blacklist", []))]
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
                if res['action'] == 'update' or res['action'] == 'keep':
                    watchlist[sym] = res['data']
                    all_results.append(res)
                elif res['action'] == 'remove':
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
                    holding_map[sym] = {'symbol': sym, 'd1_date': dt_str}
                    
        for p in existing_positions:
            if p['side'].upper() == 'LONG':
                sym = p['symbol']
                if sym not in holding_map:
                    holding_map[sym] = {'symbol': sym, 'd1_date': '外部建倉'}

        holding_items = []
        real_watching = []
        real_new_triggers = []

        for sym, data in holding_map.items():
            holding_items.append(data)
        
        holding_items_dict = {item['symbol']: item for item in holding_items}

        for item in all_results:
            sym = item['symbol']
            
            if sym in holding_items_dict:
                # 已經在持倉中，更新其 3D 日期標籤
                holding_items_dict[sym]['d1_date'] = item.get('d1_date', holding_items_dict[sym]['d1_date'])
            elif item.get('is_3h_met'):
                cached = watchlist.get(sym, {})
                trigger_ts = item.get('trigger_ts', 0)
                # 若同一觸發已處理過，不再進場，視為錯失後持續關注
                if cached.get('last_trigger_ts') == trigger_ts and trigger_ts > 0:
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
                    item['precision'], default_loss
                )
                if order:
                    if sym in watchlist:
                        watchlist[sym]['last_trigger_ts'] = item.get('trigger_ts', 0)
                    save_watchlist(watchlist)

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
                        batch = await ex.fetch_ohlcv(sym, '1d', limit=10)
                        if batch:
                            df_1d = pd.DataFrame(batch, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
                            now_utc_1d_fl = int(pd.Timestamp.now(tz='UTC').floor('1d').timestamp() * 1000)
                            closed_1d = df_1d[df_1d['ts'] < now_utc_1d_fl]
                            if len(closed_1d) >= 2:
                                last_closed = closed_1d.iloc[-1]
                                prev_closed = closed_1d.iloc[-2]
                                last_close_time = last_closed['ts'] + 24 * 3600 * 1000
                                if last_close_time > entry_time:
                                    prev_body_high = max(prev_closed['open'], prev_closed['close'])
                                    if last_closed['close'] > prev_body_high and last_closed['low'] > entry_price:
                                        candidate_sl = last_closed['low']
                                        # 保護止損只能往上，新值必須高於現有 sl_price 才替換
                                        if candidate_sl > current_sl:
                                            precision = matched_sig.get('precision', 4)
                                            old_sl = current_sl
                                            matched_sig['sl_price'] = candidate_sl
                                            save_active_signals(signals)
                                            logger.info(f"🛡️ 保護止損上移: {sym} | {old_sl:.{precision}f} → {candidate_sl:.{precision}f}")
                                            send_telegram_message(
                                                f"🛡️ <b>保護止損已上移</b>\n\n"
                                                f"💎 {get_base_coin(sym)}\n"
                                                f"📍 <code>{old_sl:.{precision}f}</code> → <code>{candidate_sl:.{precision}f}</code>"
                                            )
                                        # 無論是否更新，顯示當前保護止損
                                        item['protect_sl'] = float(matched_sig['sl_price'])
                except Exception as e:
                    logger.warning(f"獲取保護止損失敗 ({sym}): {e}")

        # === 排序與推播 ===
        # 1. 新進場訊號 (Triggered)
        for item in real_new_triggers:
            send_triggered_message(item, default_loss)
            
        # 2. 持倉中 (Holding)
        if holding_items:
            send_grouped_message(holding_items, "💼 <b>[持倉中]</b>")
            
        # 3. 關注中 (Watching)
        if real_watching:
            send_grouped_message(real_watching, "👀 <b>[關注中]</b>")

        # 4. 系統設定 (System Settings)
        if real_new_triggers or holding_items or real_watching:
            send_system_settings_message(config)

        active_count = len(watchlist)
        logger.info(f"✅ 掃描完成。新觸發: {len(real_new_triggers)} / 持倉: {len(holding_items)} / 關注: {len(real_watching)} / 追蹤總數: {active_count}")

        if not real_new_triggers and not holding_items and not real_watching:
            send_telegram_message(f"✅ <b>條件掃描完成</b>\n本次共掃描 {total_coins} 個幣種，無滿足條件標的。\n(當前清單追蹤中: {active_count} 個)")
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
            # 每日 UTC 整點 (且須能被 3 整除的小時) 的前 10 分鐘內觸發，防止迴圈耗時導致跳過
            if now.minute <= 10 and now.hour % 3 == 0 and (now.day != last_day or now.hour != last_exec_hour):
                try:
                    await run_scan()
                except Exception as e:
                    logger.error(f"定時掃描異常: {e}")
                last_day = now.day
                last_exec_hour = now.hour

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
bg_thread = threading.Thread(target=run_background_system, daemon=True)
bg_thread.start()

tg_thread = threading.Thread(target=tg_polling_background, daemon=True)
tg_thread.start()

if __name__ == '__main__':
    run_flask()
