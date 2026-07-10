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

from core.momentum_arb import MomentumArbStrategy
from core.resolver import MomentumResolver
from core.database import (
    init_db, get_open_trades, get_performance,
    is_daily_loss_limit_hit, get_conn
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

TOKEN          = os.getenv("TELEGRAM_BOT_TOKEN", "")
ALLOWED_USER   = int(os.getenv("ALLOWED_USER_ID", "0"))
PAPER_MODE     = os.getenv("PAPER_MODE", "true").lower() == "true"
SCAN_INTERVAL  = 30   # seconds between price scans
RESOLVE_INTERVAL = 60 # seconds between resolver runs

# Global strategy instances
mom      = MomentumArbStrategy()
resolver = MomentumResolver()
app_ref  = None  # Telegram app reference

# ── Alert helper ──────────────────────────────────────────────────────────────
async def alert(text: str):
    if not app_ref or not ALLOWED_USER:
        return
    try:
        await app_ref.bot.send_message(chat_id=ALLOWED_USER, text=text)
    except Exception as e:
        logger.error(f"Alert failed: {e}")

# ── Main loops ────────────────────────────────────────────────────────────────
async def momentum_loop():
    """Scan for momentum signals every 30 seconds."""
    await asyncio.sleep(5)  # startup delay
    while True:
        try:
            if not is_daily_loss_limit_hit():
                signals = await mom.scan()
                for sig in signals:
                    sym  = sig["symbol"]
                    side = sig["trade_side"]
                    # Hedge/duplicate prevention
                    existing = get_open_trades("momentum_arb")
                    opp = "NO" if side == "YES" else "YES"
                    if any(t.get("symbol")==sym and t.get("side","").upper()==side for t in existing):
                        logger.info(f"Skip {sym} {side} — already open")
                        continue
                    if any(t.get("symbol")==sym and t.get("side","").upper()==opp for t in existing):
                        logger.info(f"Skip {sym} {side} — hedge prevention")
                        continue
                    # Open trade
                    from core.database import open_trade
                    tid = open_trade(
                        "momentum_arb", sym, side,
                        sig["best_price"], sig.get("stake", 3.0),
                        stop_loss=sig.get("stop_loss"),
                        take_profit=sig.get("take_profit"),
                        metadata={
                            "kucoin_price":  sig.get("kucoin_price", 0),
                            "momentum":      sig.get("momentum", ""),
                            "yes_prob":      sig.get("yes_prob", 0),
                            "slug":          sig.get("slug", ""),
                            "best_token_id": sig.get("best_token_id", ""),
                            "yes_token_id":  sig.get("yes_token_id", ""),
                            "no_token_id":   sig.get("no_token_id", ""),
                        },
                        paper=PAPER_MODE
                    )
                    if tid:
                        side_emoji = "📈" if side == "YES" else "📉"
                        await alert(
                            f"{side_emoji} Momentum Arb\n"
                            f"{sym} {sig.get('momentum','')}\n"
                            f"Market lagging: {side}={sig.get('yes_prob',0):.0f}%\n"
                            f"Stake: ${sig.get('stake',3):.1f} | "
                            f"{sig.get('secs_remaining',0):.0f}s remaining"
                        )
        except Exception as e:
            logger.error(f"Momentum loop error: {e}", exc_info=True)
        await asyncio.sleep(SCAN_INTERVAL)

async def resolver_loop():
    """Resolve open trades every 60 seconds."""
    await asyncio.sleep(15)  # startup delay
    while True:
        try:
            closed = await resolver.resolve()
            for trade in closed:
                pnl    = trade.get("pnl", 0)
                sym    = trade.get("symbol", "")
                reason = trade.get("exit_reason", "")
                emoji  = "✅" if pnl > 0 else ("⚪" if pnl == 0 else "❌")
                await alert(
                    f"{emoji} Trade Resolved\n"
                    f"{sym}\n"
                    f"P&L: ${pnl:+.4f} | {reason}"
                )
        except Exception as e:
            logger.error(f"Resolver loop error: {e}", exc_info=True)
        await asyncio.sleep(RESOLVE_INTERVAL)

# ── Telegram commands ─────────────────────────────────────────────────────────
KEYBOARD = ReplyKeyboardMarkup(
    [
        [KeyboardButton("📂 Positions"), KeyboardButton("📋 Journal")],
        [KeyboardButton("💰 P&L"),       KeyboardButton("ℹ️ Status")],
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
    rows = conn.execute(
        "SELECT * FROM trades WHERE status='closed' ORDER BY closed_at DESC LIMIT 30"
    ).fetchall()
    conn.close()
    trades = [dict(r) for r in rows]
    if not trades:
        await update.message.reply_text("No closed trades yet.")
        return
    wins   = [t for t in trades if (t.get("pnl") or 0) > 0]
    losses = [t for t in trades if (t.get("pnl") or 0) < 0]
    total  = sum(t.get("pnl") or 0 for t in trades)
    wr     = len(wins) / len(trades) * 100 if trades else 0
    lines  = [
        f"Journal ({len(trades)} trades)",
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
    await update.message.reply_text(
        f"Momentum Arb P&L\n"
        f"Trades: {len(trades)} | W:{len(wins)} L:{len(losses)}\n"
        f"Win Rate: {wr:.1f}%\n"
        f"Total P&L: ${total:+.2f}"
    )

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ALLOWED_USER: return
    open_trades = get_open_trades("momentum_arb")
    conn = get_conn()
    closed = conn.execute("SELECT COUNT(*) as c FROM trades WHERE status='closed'").fetchone()["c"]
    conn.close()
    await update.message.reply_text(
        f"Momentum Arb Bot\n"
        f"Paper: {PAPER_MODE}\n"
        f"Stake: ${os.getenv('MOMENTUM_STAKE', '3.0')}/trade\n"
        f"Open positions: {len(open_trades)}\n"
        f"Closed trades: {closed}\n"
        f"Scan: every {SCAN_INTERVAL}s",
        reply_markup=KEYBOARD
    )

async def cmd_reset(update: Update, context: ContextTypes.DEFAULT_TYPE):

    if update.effective_user.id != ALLOWED_USER: return
    conn = get_conn()
    conn.execute("DELETE FROM trades")
    conn.execute("DELETE FROM daily_stats")
    conn.commit()
    await update.message.reply_text("Reset complete. All trades cleared.")

# ── App startup ───────────────────────────────────────────────────────────────
async def on_startup(app):
    global app_ref
    app_ref = app
    init_db()
    asyncio.create_task(momentum_loop())
    asyncio.create_task(resolver_loop())
    await alert(
        f"Momentum Arb Bot started\n"
        f"Paper: {PAPER_MODE} | Stake: ${os.getenv('MOMENTUM_STAKE','3.0')}/trade"
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
        if text == "📂 Positions":  await cmd_positions(update, context)
        elif text == "📋 Journal":  await cmd_journal(update, context)
        elif text == "💰 P&L":      await cmd_pnl(update, context)
        elif text == "ℹ️ Status":   await cmd_status(update, context)
        elif text == "🔄 Reset":    await cmd_reset(update, context)

    application.add_handler(CommandHandler("start",     cmd_start))

    application.add_handler(CommandHandler("positions", cmd_positions))
    application.add_handler(CommandHandler("journal",   cmd_journal))
    application.add_handler(CommandHandler("pnl",       cmd_pnl))
    application.add_handler(CommandHandler("reset",     cmd_reset))
    application.add_handler(CommandHandler("status",    cmd_status))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, button_handler))


    PORT = int(os.getenv("PORT", 8080))
    application.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True
    )

if __name__ == "__main__":
    main()
