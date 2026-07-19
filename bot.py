"""
Momentum Arb Bot — Standalone
Exploits 2-5 second lag between KuCoin price and Polymarket 15-min markets.

Env vars:
  TELEGRAM_BOT_TOKEN   — bot token
  ALLOWED_USER_ID      — your Telegram user ID
  MOMENTUM_STAKE       — $ per trade (default 3.0)
  PAPER_MODE           — true/false (default true)
  PORT                 — Railway port (default 8080)
"""

import asyncio
import logging
import os
from datetime import datetime, timezone

from dotenv import load_dotenv
load_dotenv()

from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import Application, CommandHandler, ContextTypes
from aiohttp import web

from core import pm_engine
from core.pm_engine import SpotFeed, fair_p_up, fetch_book, fetch_market, \
    depth_at, walk_asks, interval_slug, INTERVAL_S
from core.resolver import PMResolver
from core.live_executor import LiveExecutorV2 as LiveExecutor
from core.database import (
    init_db, get_open_trades, get_performance, get_calibration,
    is_daily_loss_limit_hit, get_conn, open_trade, close_trade
)
import aiohttp
import json
import math
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

TOKEN          = os.getenv("TELEGRAM_BOT_TOKEN", "")
ALLOWED_USER   = int(os.getenv("ALLOWED_USER_ID", "0"))
PAPER_MODE     = os.getenv("PAPER_MODE", "true").lower() == "true"
RESOLVE_INTERVAL = 12

# ── NEXUS v2 strategy config (kalshi-v3 doctrine, Polymarket economics) ───
CONV_MIN_P   = float(os.getenv("CONV_MIN_P", "0.90"))
CONV_MIN_EV  = float(os.getenv("CONV_MIN_EV", "0.02"))
CONV_MIN_TAU = float(os.getenv("CONV_MIN_TAU", "45"))
CONV_MAX_TAU = float(os.getenv("CONV_MAX_TAU", "480"))
PRICE_MIN    = float(os.getenv("PRICE_MIN", "0.55"))
PRICE_MAX    = float(os.getenv("PRICE_MAX", "0.97"))
MAX_DIVERGENCE = float(os.getenv("MAX_DIVERGENCE", "0.12"))
SPREAD_BUFFER  = float(os.getenv("SPREAD_BUFFER", "0.005"))
LAG_Z        = float(os.getenv("LAG_Z", "3.0"))
LAG_MIN_P    = float(os.getenv("LAG_MIN_P", "0.55"))
LAG_MIN_EV   = float(os.getenv("LAG_MIN_EV", "0.04"))
LAG_LOOKBACK = float(os.getenv("LAG_LOOKBACK_S", "8"))
LAG_MIN_MOVE = float(os.getenv("LAG_MIN_MOVE", "0.0008"))
LAG_MIN_TAU  = float(os.getenv("LAG_MIN_TAU", "60"))
LAG_MAX_TAU  = float(os.getenv("LAG_MAX_TAU", "780"))
LAG_COOLDOWN = float(os.getenv("LAG_COOLDOWN_S", "60"))
STAKE_USD    = float(os.getenv("STAKE_USD", os.getenv("MOMENTUM_STAKE", "10")))
MAX_SHARES   = float(os.getenv("MAX_SHARES", "40"))
MIN_FILL     = float(os.getenv("MIN_FILL_SHARES", "3"))
DEPTH_FRAC   = float(os.getenv("DEPTH_FRACTION", "0.30"))
SPIKE_MAX    = float(os.getenv("SPIKE_MAX", "2.5"))
CHOP_MAX     = int(os.getenv("CHOP_MAX_BURSTS", "3"))
CHOP_WINDOW  = float(os.getenv("CHOP_WINDOW_S", "600"))
TAIL_MULT    = float(os.getenv("TAIL_MULT", "1.25"))


def _parse_blackouts(spec):
    out = []
    for part in (spec or "").split(","):
        if "-" not in part:
            continue
        try:
            a, z = part.strip().split("-")
            h1, m1 = map(int, a.split(":")); h2, m2 = map(int, z.split(":"))
            out.append((h1 * 60 + m1, h2 * 60 + m2))
        except ValueError:
            continue
    return out


BLACKOUTS = _parse_blackouts(os.getenv(
    "NEWS_BLACKOUT_UTC", "12:25-12:45,13:55-14:15,18:00-18:20"))


def in_blackout():
    now = datetime.now(timezone.utc)
    cur = now.hour * 60 + now.minute
    return any(a <= cur <= b for a, b in BLACKOUTS)


# Global engine instances
http_session  = None
def _sess():
    return http_session
spot          = SpotFeed(_sess)
resolver      = None   # built in on_startup (needs spot)
live_executor = LiveExecutor()
app_ref       = None
KILL_SWITCH   = False
recent_bursts = {c: [] for c in pm_engine.SYMBOLS}
lag_cooldown  = {c: 0.0 for c in pm_engine.SYMBOLS}
_mkt_cache    = {}     # (crypto, iv_ts) -> market meta or None

# ── Alert helper ──────────────────────────────────────────────────────────────
async def alert(text: str):
    if not app_ref or not ALLOWED_USER:
        return
    try:
        await app_ref.bot.send_message(chat_id=ALLOWED_USER, text=text)
    except Exception as e:
        logger.error(f"Alert failed: {e}")

# ── Main loops ────────────────────────────────────────────────────────────────
async def spot_loop():
    while True:
        try:
            await spot.poll_once()
        except Exception as e:
            logger.debug(f"spot poll: {e}")
        await asyncio.sleep(2.0)


async def _get_market(crypto: str, iv_ts: int):
    key = (crypto, iv_ts)
    if key in _mkt_cache:
        return _mkt_cache[key]
    m = await fetch_market(http_session, crypto, iv_ts)
    _mkt_cache[key] = m
    if len(_mkt_cache) > 60:
        for k in sorted(_mkt_cache)[: len(_mkt_cache) - 40]:
            _mkt_cache.pop(k, None)
    return m


def _slug_exposed(slug: str) -> bool:
    for t in get_open_trades():
        if not str(t.get("strategy", "")).startswith("pm_"):
            continue
        try:
            if json.loads(t.get("metadata") or "{}").get("slug") == slug:
                return True
        except Exception:
            continue
    return False


async def execute_entry(strategy: str, m: dict, side_is_up: bool,
                        p_side: float, tau: float, book: dict):
    """Shared taker entry: paper walks the real book; live sends an exact
    worst-price FAK. DB records what actually filled."""
    token_id = m["up_tid"] if side_is_up else m["down_tid"]
    token_idx = m["up_idx"] if side_is_up else 1 - m["up_idx"]
    ask = book["ask"]
    want = min(MAX_SHARES, math.floor(STAKE_USD / ask),
               math.floor(DEPTH_FRAC * depth_at(book["asks"], ask)))
    if want < MIN_FILL:
        return None
    if PAPER_MODE:
        filled, vwap = walk_asks(book["asks"], ask, want)
        if filled < MIN_FILL:
            return None
    else:
        if KILL_SWITCH:
            return None
        fill = await live_executor.place_entry({
            "best_token_id": token_id, "best_price": ask,
            "worst_price": round(ask, 2), "trade_side":
            "YES" if side_is_up else "NO", "stake": round(want * ask, 2),
            "symbol": m["crypto"],
        })
        if not fill:
            return None
        vwap = float(fill["actual_price"])
        filled = round(want * ask / vwap, 2) if vwap > 0 else 0
        if filled < MIN_FILL:
            return None
    side = "YES" if side_is_up else "NO"
    tid = open_trade(
        strategy, m["crypto"], side, vwap, filled,
        metadata={
            "slug": m["slug"], "iv_ts": m["iv_ts"], "end_ts": m["end_ts"],
            "token_id": token_id, "token_idx": token_idx,
            "side_is_up": side_is_up, "crypto": m["crypto"],
            "model_p": round(p_side, 4), "tau": round(tau, 1),
            "mode": "paper" if PAPER_MODE else "live",
        },
        paper=PAPER_MODE)
    if tid:
        mode = "📄 PAPER" if PAPER_MODE else "💵 LIVE"
        win = filled * (1 - vwap)
        await alert(
            f"{mode} | {strategy.upper()} TAKER\n"
            f"🪙 {m['crypto']} {'UP' if side_is_up else 'DOWN'} @ "
            f"{vwap*100:.0f}¢ ×{filled:.0f}\n"
            f"🎯 model p={p_side:.3f} | τ={tau:.0f}s\n"
            f"✅ win +${win:.2f} | ❌ loss $-{filled*vwap:.2f}\n"
            f"📋 {m['slug']}")
    return tid


async def conv_loop():
    await asyncio.sleep(8)
    while True:
        await asyncio.sleep(2.0)
        try:
            if in_blackout() or is_daily_loss_limit_hit():
                continue
            now = time.time()
            for crypto in pm_engine.SYMBOLS:
                if not spot.ready(crypto):
                    continue
                v = spot.vol[crypto]
                if v.spike_ratio > SPIKE_MAX:
                    continue
                recent_bursts[crypto] = [
                    x for x in recent_bursts[crypto] if now - x <= CHOP_WINDOW]
                if len(recent_bursts[crypto]) >= CHOP_MAX:
                    continue
                iv = int(now // INTERVAL_S) * INTERVAL_S
                tau = iv + INTERVAL_S - now
                if not (CONV_MIN_TAU <= tau <= CONV_MAX_TAU):
                    continue
                open_px = spot.interval_open(crypto, iv)
                if not open_px:
                    continue          # never trade an unwitnessed strike
                m = await _get_market(crypto, iv)
                if not m or _slug_exposed(m["slug"]):
                    continue
                p_up = fair_p_up(spot.price[crypto], open_px, tau,
                                 v.sigma_for_tau(tau), TAIL_MULT)
                if p_up >= CONV_MIN_P:
                    side_up, p_side = True, p_up
                elif (1 - p_up) >= CONV_MIN_P:
                    side_up, p_side = False, 1 - p_up
                else:
                    continue
                token = m["up_tid"] if side_up else m["down_tid"]
                book = await fetch_book(http_session, token)
                if not book:
                    continue
                ask = book["ask"]
                if not (PRICE_MIN <= ask <= PRICE_MAX):
                    continue
                if p_side - ask > MAX_DIVERGENCE:
                    logger.info(f"divergence guard {crypto}: p={p_side:.3f} "
                                f"vs ask {ask:.2f} — assuming WE are wrong")
                    continue
                ev = p_side - ask - SPREAD_BUFFER
                if ev < CONV_MIN_EV:
                    continue
                await execute_entry("pm_conv", m, side_up, p_side, tau, book)
        except Exception as e:
            logger.error(f"conv loop: {e}", exc_info=True)


async def lag_loop():
    await asyncio.sleep(10)
    while True:
        await asyncio.sleep(0.5)
        try:
            if in_blackout() or is_daily_loss_limit_hit():
                continue
            now = time.time()
            for crypto in pm_engine.SYMBOLS:
                if now < lag_cooldown[crypto] or not spot.ready(crypto):
                    continue
                r, dt = spot.move_over(crypto, LAG_LOOKBACK)
                if dt <= 0:
                    continue
                sig = spot.vol[crypto].sigma_1s * math.sqrt(max(dt, 0.5))
                z = abs(r) / sig if sig > 0 else 0
                if z < LAG_Z or abs(r) < LAG_MIN_MOVE:
                    continue
                lag_cooldown[crypto] = now + LAG_COOLDOWN
                recent_bursts[crypto].append(now)
                iv = int(now // INTERVAL_S) * INTERVAL_S
                tau = iv + INTERVAL_S - now
                if not (LAG_MIN_TAU <= tau <= LAG_MAX_TAU):
                    continue
                open_px = spot.interval_open(crypto, iv)
                if not open_px:
                    continue
                m = await _get_market(crypto, iv)
                if not m or _slug_exposed(m["slug"]):
                    continue
                # burst-inflated fast vol excluded from pricing
                p_up = fair_p_up(spot.price[crypto], open_px, tau,
                                 spot.vol[crypto].sigma_for_tau(
                                     tau, include_fast=False), TAIL_MULT)
                for side_up, p_side in ((True, p_up), (False, 1 - p_up)):
                    if p_side < LAG_MIN_P:
                        continue
                    token = m["up_tid"] if side_up else m["down_tid"]
                    book = await fetch_book(http_session, token)
                    if not book:
                        continue
                    ask = book["ask"]
                    if not (0.05 <= ask <= 0.95):
                        continue
                    if p_side - ask > MAX_DIVERGENCE:
                        continue
                    if p_side - ask - SPREAD_BUFFER < LAG_MIN_EV:
                        continue
                    await execute_entry("pm_lag", m, side_up, p_side,
                                        tau, book)
                    break
        except Exception as e:
            logger.error(f"lag loop: {e}", exc_info=True)


async def resolver_loop():
    """Resolve open trades every 60 seconds."""
    await asyncio.sleep(15)  # startup delay
    while True:
        try:
            closed = await resolver.resolve()
            for msg in resolver.pending_alerts:
                await alert(msg)
            resolver.pending_alerts.clear()
            for trade in closed:
                pnl    = trade.get("pnl", 0)
                sym    = trade.get("symbol", "")
                reason = trade.get("exit_reason", "")
                emoji  = "✅" if pnl > 0 else ("⚪" if pnl == 0 else "❌")
                cal = get_calibration("pm_")
                cal_line = (f"\n🎓 calib: model {cal['model_pct']}% vs real "
                            f"{cal['real_pct']}% ({cal['n']} resolved)"
                            if cal.get("n") else "")
                await alert(
                    f"{emoji} {reason.upper()} | {sym}\n"
                    f"P&L: ${pnl:+.2f}{cal_line}"
                )
        except Exception as e:
            logger.error(f"Resolver loop error: {e}", exc_info=True)
        await asyncio.sleep(RESOLVE_INTERVAL)

# ── Telegram commands ─────────────────────────────────────────────────────────
KEYBOARD = ReplyKeyboardMarkup(
    [
        [KeyboardButton("📂 Positions"),    KeyboardButton("📋 Journal")],
        [KeyboardButton("💰 P&L"),          KeyboardButton("🔄 Refresh")],
        [KeyboardButton("💵 Live P&L"),     KeyboardButton("📒 Live Journal")],
        [KeyboardButton("💼 Balance"),       KeyboardButton("ℹ️ Status")],
        [KeyboardButton("🚨 Kill Switch"),  KeyboardButton("✅ Resume")],
        [KeyboardButton("🔄 Reset")],
    ],
    resize_keyboard=True,
)

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ALLOWED_USER: return
    await update.message.reply_text(
        f"Momentum Arb Bot\n"
        f"Paper: {PAPER_MODE}\n"
        f"Stake: ${os.getenv('MOMENTUM_STAKE', '3.0')}/trade\n"
        f"Scan: every {SCAN_INTERVAL}s",
        reply_markup=KEYBOARD
    )

async def cmd_positions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ALLOWED_USER: return
    trades = get_open_trades("momentum_arb")
    if not trades:
        await update.message.reply_text("No open positions.")
        return
    lines = [f"Open Positions ({len(trades)})"]
    for t in trades:
        entry = float(t.get("entry_price") or 0)
        lines.append(
            f"\n{t.get('symbol','')} {t.get('side','')}\n"
            f"Entry: ${entry:.4f} | SL: ${t.get('stop_loss',0):.4f} | TP: ${t.get('take_profit',0):.4f}"
        )
    await update.message.reply_text("\n".join(lines))

async def cmd_journal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ALLOWED_USER: return
    conn = get_conn()
    all_rows = conn.execute(
        "SELECT * FROM trades WHERE status='closed'"
    ).fetchall()
    rows = conn.execute(
        "SELECT * FROM trades WHERE status='closed' ORDER BY closed_at DESC LIMIT 30"
    ).fetchall()
    conn.close()
    all_trades = [dict(r) for r in all_rows]
    trades = [dict(r) for r in rows]
    if not trades:
        await update.message.reply_text("No closed trades yet.")
        return
    wins   = [t for t in all_trades if (t.get("pnl") or 0) > 0]
    losses = [t for t in all_trades if (t.get("pnl") or 0) < 0]
    total  = sum(t.get("pnl") or 0 for t in all_trades)
    wr     = len(wins) / len(all_trades) * 100 if all_trades else 0
    lines  = [
        f"Journal ({len(all_trades)} trades | showing last {len(trades)})",
        f"W:{len(wins)} L:{len(losses)} | P&L:${total:+.2f} | WR:{wr:.0f}%",
        ""
    ]
    for t in trades[-20:]:  # last 20
        pnl = t.get("pnl") or 0
        sym = t.get("symbol", "")
        side = t.get("side", "")
        reason = t.get("exit_reason", "")
        emoji = "WIN" if pnl > 0 else ("NEUTRAL" if pnl == 0 else "LOSS")
        lines.append(f"{emoji} | {sym} {side} | ${pnl:+.4f} | {reason}")
    await update.message.reply_text("\n".join(lines))

async def cmd_pnl(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ALLOWED_USER: return
    conn = get_conn()
    rows = conn.execute("SELECT * FROM trades WHERE status='closed'").fetchall()
    conn.close()
    trades = [dict(r) for r in rows]
    wins   = [t for t in trades if (t.get("pnl") or 0) > 0]
    losses = [t for t in trades if (t.get("pnl") or 0) < 0]
    total  = sum(t.get("pnl") or 0 for t in trades)
    wr     = len(wins) / len(trades) * 100 if trades else 0
    cal = get_calibration("pm_")
    cal_line = (f"\n🎓 Calibration (resolved): model {cal['model_pct']}% "
                f"vs real {cal['real_pct']}% ({cal['n']}T)"
                if cal.get("n") else "\n🎓 Calibration: no resolved trades yet")
    await update.message.reply_text(
        f"NEXUS v2 P&L\n"
        f"Trades: {len(trades)} | W:{len(wins)} L:{len(losses)}\n"
        f"Win Rate: {wr:.1f}%\n"
        f"Total P&L: ${total:+.2f}{cal_line}"
    )

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ALLOWED_USER: return
    open_trades = [t for t in get_open_trades()
                   if str(t.get("strategy", "")).startswith("pm_")]
    conn = get_conn()
    closed = conn.execute("SELECT COUNT(*) as c FROM trades WHERE status='closed'").fetchone()["c"]
    conn.close()
    await update.message.reply_text(
        f"NEXUS v2 (fair-value engine)\n"
        f"Paper: {PAPER_MODE}\n"
        f"Stake: ${STAKE_USD:.0f}/trade | shares≤{MAX_SHARES:.0f}\n"
        f"Open: {len(open_trades)} | Closed: {closed}\n"
        f"CONV p≥{CONV_MIN_P} ev≥{CONV_MIN_EV*100:.0f}¢ | "
        f"LAG z≥{LAG_Z} p≥{LAG_MIN_P}",
        reply_markup=KEYBOARD
    )

async def cmd_kill(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Emergency kill switch — cancels all live orders and blocks new ones."""
    global KILL_SWITCH
    if update.effective_user.id != ALLOWED_USER: return
    KILL_SWITCH = True
    # Cancel all open CLOB orders
    cancelled = 0
    if not PAPER_MODE and live_executor._initialized and live_executor._client:
        try:
            resp = await asyncio.get_event_loop().run_in_executor(
                None, live_executor._client.cancel_all
            )
            cancelled = len(resp) if isinstance(resp, list) else 0
            logger.info(f"Kill switch: cancelled {cancelled} open orders on CLOB")
        except Exception as e:
            logger.error(f"Kill switch: cancel_all error: {e}")
    await update.message.reply_text(
        f"🚨 KILL SWITCH ACTIVATED\n"
        f"All new live orders BLOCKED\n"
        f"CLOB orders cancelled: {cancelled}\n"
        f"Bot still running — use /resume to re-enable trading"
    )

async def cmd_resume(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Resume live trading after kill switch."""
    global KILL_SWITCH
    if update.effective_user.id != ALLOWED_USER: return
    KILL_SWITCH = False
    await update.message.reply_text(
        "✅ Kill switch DEACTIVATED\n"
        "Live trading resumed"
    )

async def cmd_livepnl(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """P&L for live trades only (paper=0)."""
    if update.effective_user.id != ALLOWED_USER: return
    conn = get_conn()
    rows = conn.execute("SELECT * FROM trades WHERE status='closed' AND paper=0").fetchall()
    conn.close()
    trades = [dict(r) for r in rows]
    if not trades:
        await update.message.reply_text("No live trades closed yet.")
        return
    wins   = [t for t in trades if (t.get("pnl") or 0) > 0]
    losses = [t for t in trades if (t.get("pnl") or 0) < 0]
    total  = sum(t.get("pnl") or 0 for t in trades)
    wr     = len(wins) / len(trades) * 100 if trades else 0
    status = "🚨 KILL SWITCH ON" if KILL_SWITCH else ("💵 LIVE" if not PAPER_MODE else "📄 PAPER")
    await update.message.reply_text(
        f"💵 Live P&L [{status}]\n"
        f"Trades: {len(trades)} | W:{len(wins)} L:{len(losses)}\n"
        f"Win Rate: {wr:.1f}%\n"
        f"Total P&L: ${total:+.2f}"
    )

async def cmd_livejournal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Journal for live trades only (paper=0)."""
    if update.effective_user.id != ALLOWED_USER: return
    conn = get_conn()
    all_rows = conn.execute("SELECT * FROM trades WHERE status='closed' AND paper=0").fetchall()
    rows = conn.execute(
        "SELECT * FROM trades WHERE status='closed' AND paper=0 ORDER BY closed_at DESC LIMIT 30"
    ).fetchall()
    conn.close()
    all_trades = [dict(r) for r in all_rows]
    trades = [dict(r) for r in rows]
    if not trades:
        await update.message.reply_text("No live trades closed yet.")
        return
    wins  = [t for t in all_trades if (t.get("pnl") or 0) > 0]
    losses= [t for t in all_trades if (t.get("pnl") or 0) < 0]
    total = sum(t.get("pnl") or 0 for t in all_trades)
    wr    = len(wins) / len(all_trades) * 100 if all_trades else 0
    lines = [
        f"💵 Live Journal ({len(all_trades)} trades | showing last {len(trades)})",
        f"W:{len(wins)} L:{len(losses)} | P&L:${total:+.2f} | WR:{wr:.0f}%",
        ""
    ]
    for t in trades:
        pnl    = t.get("pnl") or 0
        sym    = t.get("symbol", "")
        side   = t.get("side", "")
        reason = t.get("exit_reason", "")
        emoji  = "WIN" if pnl > 0 else ("NEUTRAL" if pnl == 0 else "LOSS")
        lines.append(f"{emoji} | {sym} {side} | ${pnl:+.4f} | {reason}")
    await update.message.reply_text("\n".join(lines))

async def cmd_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show USDC and MATIC balance on Polygon."""
    if update.effective_user.id != ALLOWED_USER: return
    wallet = os.getenv("WALLET_FUNDER_ADDRESS", "")
    if not wallet:
        await update.message.reply_text("WALLET_FUNDER_ADDRESS not set in Railway env vars.")
        return
    await update.message.reply_text("Fetching balance...")
    try:
        async with __import__("aiohttp").ClientSession() as session:
            # Try multiple Polygon RPC endpoints until one works
            RPCS = [
                "https://polygon-rpc.com",
                "https://rpc.ankr.com/polygon",
                "https://polygon-mainnet.public.blastapi.io",
                "https://1rpc.io/matic",
                "https://polygon.llamarpc.com",
            ]
            USDC_BRIDGED = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
            USDC_NATIVE  = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359"
            timeout = __import__("aiohttp").ClientTimeout(total=6)

            async def rpc_call(payload):
                for rpc in RPCS:
                    try:
                        async with session.post(rpc, json=payload, timeout=timeout) as rr:
                            d = await rr.json(content_type=None)
                            if d.get("result") is not None:
                                return d
                    except Exception:
                        continue
                return {"result": "0x0"}

            wallet_lower = wallet.lower()
            data_hex = "0x70a08231" + wallet_lower[2:].zfill(64)

            d1 = await rpc_call({"jsonrpc":"2.0","id":1,"method":"eth_call","params":[{"to":USDC_BRIDGED,"data":data_hex},"latest"]})
            d2 = await rpc_call({"jsonrpc":"2.0","id":2,"method":"eth_call","params":[{"to":USDC_NATIVE,"data":data_hex},"latest"]})
            d3 = await rpc_call({"jsonrpc":"2.0","id":3,"method":"eth_getBalance","params":[wallet,"latest"]})

            usdc_bridged = int(d1.get("result","0x0") or "0x0", 16) / 1_000_000
            usdc_native  = int(d2.get("result","0x0") or "0x0", 16) / 1_000_000
            usdc_bal     = usdc_bridged + usdc_native
            matic_bal    = int(d3.get("result","0x0") or "0x0", 16) / 10**18
            usdc_detail  = f"(USDC.e: ${usdc_bridged:.2f} | Native: ${usdc_native:.2f})"
            mode    = "🚨 KILL SWITCH ON" if KILL_SWITCH else ("💵 LIVE" if not PAPER_MODE else "📄 PAPER")
            short   = wallet[:6] + "..." + wallet[-4:]
            await update.message.reply_text(
                f"💼 Wallet Balance\n"
                f"Address: {short}\n"
                f"Network: Polygon\n"
                f"\n"
                f"💵 USDC: ${usdc_bal:.2f} {usdc_detail}\n"
                f"⛽ MATIC: {matic_bal:.6f}\n"
                f"\n"
                f"Mode: {mode}"
            )
    except Exception as e:
        logger.error(f"Balance fetch error: {e}")
        await update.message.reply_text(f"Balance fetch failed: {e}")

async def cmd_reset(update: Update, context: ContextTypes.DEFAULT_TYPE):



    if update.effective_user.id != ALLOWED_USER: return
    conn = get_conn()
    conn.execute("DELETE FROM trades")
    conn.execute("DELETE FROM daily_stats")
    conn.commit()
    await update.message.reply_text("Reset complete. All trades cleared.")

# ── App startup ───────────────────────────────────────────────────────────────
async def on_startup(app):
    global app_ref, http_session, resolver
    app_ref = app
    init_db()
    http_session = aiohttp.ClientSession()
    resolver = PMResolver(spot, _sess)
    # Purge legacy v1 momentum trades: neutral close at entry (uncounted)
    legacy = get_open_trades("momentum_arb")
    for t in legacy:
        close_trade(t["id"], t.get("entry_price") or 0, "legacy_purge")
    if legacy:
        await alert(f"🧹 purged {len(legacy)} legacy v1 momentum positions "
                    f"(neutral, uncounted)")
    if not PAPER_MODE:
        ok = await live_executor.initialise()
        if not ok:
            await app.bot.send_message(
                chat_id=int(os.getenv("ALLOWED_USER_ID","0")),
                text="⚠️ LiveExecutor failed to initialise — check WALLET_PRIVATE_KEY and WALLET_FUNDER_ADDRESS. Running in PAPER MODE as fallback."
            )
            logger.error("LiveExecutor init failed — will paper trade only")
    asyncio.create_task(spot_loop())
    asyncio.create_task(conv_loop())
    asyncio.create_task(lag_loop())
    asyncio.create_task(resolver_loop())
    await alert(
        f"🤖 NEXUS v2 STARTED [{'PAPER' if PAPER_MODE else 'LIVE'}]\n"
        f"CONV: p≥{CONV_MIN_P} ev≥{CONV_MIN_EV*100:.0f}¢ "
        f"τ∈[{CONV_MIN_TAU:.0f},{CONV_MAX_TAU:.0f}]s | "
        f"LAG: z≥{LAG_Z} ev≥{LAG_MIN_EV*100:.0f}¢\n"
        f"Fair value vs CLOB books; strikes only from witnessed interval "
        f"opens; settlements at true $1/$0.\n"
        f"⚠️ v2 trades far less than v1 — that is the design."
    )

def main():
    application = (
        Application.builder()
        .token(TOKEN)
        .post_init(on_startup)
        .build()
    )
    from telegram.ext import MessageHandler, filters
    async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id != ALLOWED_USER: return
        text = update.message.text
        if text == "📂 Positions":      await cmd_positions(update, context)
        elif text == "📋 Journal":      await cmd_journal(update, context)
        elif text == "💰 P&L":          await cmd_pnl(update, context)
        elif text == "ℹ️ Status":       await cmd_status(update, context)
        elif text == "💵 Live P&L":     await cmd_livepnl(update, context)
        elif text == "📒 Live Journal": await cmd_livejournal(update, context)
        elif text == "💼 Balance":         await cmd_balance(update, context)
        elif text == "🚨 Kill Switch":     await cmd_kill(update, context)
        elif text == "✅ Resume":       await cmd_resume(update, context)
        elif text == "🔄 Refresh":       await cmd_refresh(update, context)
        elif text == "🔄 Reset":         await cmd_reset(update, context)

    application.add_handler(CommandHandler("start",     cmd_start))

    application.add_handler(CommandHandler("positions", cmd_positions))
    application.add_handler(CommandHandler("journal",   cmd_journal))
    application.add_handler(CommandHandler("pnl",       cmd_pnl))
    application.add_handler(CommandHandler("reset",        cmd_reset))
    application.add_handler(CommandHandler("status",       cmd_status))
    application.add_handler(CommandHandler("balance",      cmd_balance))
    application.add_handler(CommandHandler("kill",         cmd_kill))
    application.add_handler(CommandHandler("resume",       cmd_resume))
    application.add_handler(CommandHandler("livepnl",      cmd_livepnl))
    application.add_handler(CommandHandler("livejournal",  cmd_livejournal))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, button_handler))


    PORT = int(os.getenv("PORT", 8080))
    application.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True
    )

if __name__ == "__main__":
    main()
