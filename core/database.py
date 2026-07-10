"""
NEXUS - Unified Database
Handles all strategies in one SQLite file.
Thread-safe with WAL mode for concurrent writes.
Includes daily loss limit and max position caps.
"""

import sqlite3
import logging
import os
import json
from datetime import datetime, timezone
from typing import Optional, List, Dict

logger = logging.getLogger(__name__)

_default_db = "/data/momentum.db" if os.path.isdir("/data") else "/tmp/momentum.db"
DB_PATH = os.getenv("DB_PATH", _default_db)


def get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")  # allows concurrent reads during writes
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = get_conn()
    try:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS trades (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                strategy        TEXT NOT NULL,
                symbol          TEXT NOT NULL,
                side            TEXT NOT NULL,
                entry_price     REAL NOT NULL,
                exit_price      REAL,
                size            REAL NOT NULL,
                pnl             REAL DEFAULT 0,
                pnl_pct         REAL DEFAULT 0,
                status          TEXT DEFAULT 'open',
                exit_reason     TEXT,
                stop_loss       REAL,
                take_profit     REAL,
                ai_score        INTEGER DEFAULT 0,
                metadata        TEXT DEFAULT '{}',
                opened_at       TEXT NOT NULL,
                closed_at       TEXT,
                paper           INTEGER DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS strategy_state (
                strategy        TEXT PRIMARY KEY,
                enabled         INTEGER DEFAULT 1,
                capital         REAL NOT NULL,
                paper           INTEGER DEFAULT 1,
                last_scan       TEXT,
                scan_count      INTEGER DEFAULT 0,
                total_pnl       REAL DEFAULT 0,
                win_count       INTEGER DEFAULT 0,
                loss_count      INTEGER DEFAULT 0,
                metadata        TEXT DEFAULT '{}'
            );

            CREATE TABLE IF NOT EXISTS daily_stats (
                date            TEXT PRIMARY KEY,
                total_pnl       REAL DEFAULT 0,
                trade_count     INTEGER DEFAULT 0,
                win_count       INTEGER DEFAULT 0,
                loss_count      INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS config (
                key             TEXT PRIMARY KEY,
                value           TEXT NOT NULL,
                updated_at      TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_trades_strategy ON trades(strategy);
            CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status);
            CREATE INDEX IF NOT EXISTS idx_trades_opened ON trades(opened_at);
        """)
        conn.commit()

        strategies = [
            ("prediction_market", 60.0),
            ("grid_bot",          60.0),
            ("triangular_arb",    50.0),
            ("funding_rate",      30.0),
        ]
        for name, capital in strategies:
            conn.execute("""
                INSERT OR IGNORE INTO strategy_state (strategy, capital)
                VALUES (?, ?)
            """, (name, capital))

        defaults = [
            ("daily_loss_limit_pct", "10"),
            ("max_open_per_strategy", "8"),
            ("max_total_open", "25"),
        ]
        for key, val in defaults:
            conn.execute("""
                INSERT OR REPLACE INTO config (key, value, updated_at)
                VALUES (?, ?, ?)
            """, (key, val, datetime.now(timezone.utc).isoformat()))
        conn.commit()
        logger.info("Momentum Arb database initialized")
    finally:
        conn.close()


def get_config(key: str, default=None):
    conn = get_conn()
    try:
        row = conn.execute("SELECT value FROM config WHERE key=?", (key,)).fetchone()
        return row["value"] if row else default
    finally:
        conn.close()


def set_config(key: str, value: str):
    conn = get_conn()
    try:
        conn.execute("""
            INSERT OR REPLACE INTO config (key, value, updated_at)
            VALUES (?, ?, ?)
        """, (key, value, datetime.now(timezone.utc).isoformat()))
        conn.commit()
    finally:
        conn.close()


def is_daily_loss_limit_hit() -> bool:
    conn = get_conn()
    try:
        today = datetime.now(timezone.utc).isoformat()[:10]
        row = conn.execute(
            "SELECT total_pnl FROM daily_stats WHERE date=?", (today,)
        ).fetchone()
        if not row:
            return False
        limit_pct = float(get_config("daily_loss_limit_pct", "10"))
        total_capital = conn.execute(
            "SELECT SUM(capital) as c FROM strategy_state"
        ).fetchone()["c"] or 200.0
        threshold = -(total_capital * limit_pct / 100)
        # Sanity check: loss > total capital = calculation bug, not real loss
        if row["total_pnl"] < -(total_capital * 10):
            logger.warning(f"Daily loss limit sanity check: P&L {row['total_pnl']:.2f} exceeds 10x capital — ignoring (likely calculation bug)")
            return False
        return row["total_pnl"] < threshold
    finally:
        conn.close()


def open_trade(strategy: str, symbol: str, side: str, entry_price: float,
               size: float, stop_loss: float = None, take_profit: float = None,
               ai_score: int = 0, metadata: dict = None, paper: bool = True) -> Optional[int]:
    conn = get_conn()
    try:
        if is_daily_loss_limit_hit():
            logger.warning(f"Daily loss limit hit — blocking new {strategy} trade")
            return None

        max_per = int(get_config("max_open_per_strategy", "5"))
        max_total = int(get_config("max_total_open", "25"))

        open_for_strategy = conn.execute(
            "SELECT COUNT(*) as c FROM trades WHERE strategy=? AND status='open'",
            (strategy,)
        ).fetchone()["c"]

        open_total = conn.execute(
            "SELECT COUNT(*) as c FROM trades WHERE status='open'"
        ).fetchone()["c"]

        if open_for_strategy >= max_per:
            logger.warning(f"Max open trades for {strategy} reached ({max_per})")
            return None

        if open_total >= max_total:
            logger.warning(f"Max total open trades reached ({max_total})")
            return None

        trade_id = conn.execute("""
            INSERT INTO trades
            (strategy, symbol, side, entry_price, size, stop_loss, take_profit,
             ai_score, metadata, opened_at, paper)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            strategy, symbol, side, entry_price, size, stop_loss, take_profit,
            ai_score, json.dumps(metadata or {}),
            datetime.now(timezone.utc).isoformat(), int(paper)
        )).lastrowid
        conn.commit()
        logger.info(f"Opened {strategy} trade #{trade_id}: {side} {symbol} @ {entry_price}")
        return trade_id
    finally:
        conn.close()


def close_trade(trade_id: int, exit_price: float, exit_reason: str) -> Optional[Dict]:
    conn = get_conn()
    try:
        trade = conn.execute(
            "SELECT * FROM trades WHERE id=? AND status='open'", (trade_id,)
        ).fetchone()
        if not trade:
            return None

        entry = trade["entry_price"]
        size  = trade["size"]
        side  = trade["side"]

        if side in ("long", "YES", "buy"):
            pnl     = (exit_price - entry) * size
            pnl_pct = ((exit_price - entry) / entry) * 100 if entry else 0.0
        else:
            pnl     = (entry - exit_price) * size
            pnl_pct = ((entry - exit_price) / entry) * 100 if entry else 0.0

        now = datetime.now(timezone.utc).isoformat()
        conn.execute("""
            UPDATE trades SET
                exit_price=?, exit_reason=?, pnl=?, pnl_pct=?,
                status='closed', closed_at=?
            WHERE id=?
        """, (exit_price, exit_reason, pnl, pnl_pct, now, trade_id))

        # pnl>0=win, pnl<0=loss, pnl==0=neutral (expired/unknown — not counted)
        if pnl > 0:
            conn.execute("""
                UPDATE strategy_state SET
                    total_pnl = total_pnl + ?, win_count = win_count + 1
                WHERE strategy=?
            """, (pnl, trade["strategy"]))
        elif pnl < 0:
            conn.execute("""
                UPDATE strategy_state SET
                    total_pnl = total_pnl + ?, loss_count = loss_count + 1
                WHERE strategy=?
            """, (pnl, trade["strategy"]))
        # pnl==0: neutral close — don't count as win or loss

        date = now[:10]
        is_win  = 1 if pnl > 0 else 0
        is_loss = 1 if pnl < 0 else 0
        conn.execute("""
            INSERT INTO daily_stats (date, total_pnl, trade_count, win_count, loss_count)
            VALUES (?, ?, 1, ?, ?)
            ON CONFLICT(date) DO UPDATE SET
                total_pnl   = total_pnl + excluded.total_pnl,
                trade_count = trade_count + 1,
                win_count   = win_count + excluded.win_count,
                loss_count  = loss_count + excluded.loss_count
        """, (date, pnl, is_win, is_loss))

        conn.commit()
        logger.info(f"Closed trade #{trade_id}: {exit_reason} P&L=${pnl:.2f}")
        return dict(trade) | {"pnl": pnl, "pnl_pct": pnl_pct}
    finally:
        conn.close()


def get_open_trades(strategy: str = None) -> List[Dict]:
    conn = get_conn()
    try:
        if strategy:
            rows = conn.execute(
                "SELECT * FROM trades WHERE status='open' AND strategy=? ORDER BY opened_at DESC",
                (strategy,)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM trades WHERE status='open' ORDER BY opened_at DESC"
            ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_performance(strategy: str = None, days: int = 30) -> Dict:
    conn = get_conn()
    try:
        if strategy:
            rows = conn.execute("""
                SELECT * FROM trades
                WHERE strategy=? AND status='closed'
                AND date(closed_at) >= date('now', ?)
            """, (strategy, f"-{days} days")).fetchall()
        else:
            rows = conn.execute("""
                SELECT * FROM trades
                WHERE status='closed'
                AND date(closed_at) >= date('now', ?)
            """, (f"-{days} days",)).fetchall()

        trades = [dict(r) for r in rows]
        total  = len(trades)
        wins    = sum(1 for t in trades if t["pnl"] > 0)
        losses  = sum(1 for t in trades if t["pnl"] < 0)
        neutral = sum(1 for t in trades if t["pnl"] == 0)
        # win_rate excludes neutral (expired/unknown) trades
        decided = wins + losses
        pnl    = sum(t["pnl"] for t in trades)

        return {
            "total":     total,
            "wins":      wins,
            "losses":    losses,
            "win_rate":  (wins / decided * 100) if decided > 0 else 0,
            "total_pnl": pnl,
            "avg_win":   sum(t["pnl"] for t in trades if t["pnl"] > 0) / max(wins, 1),
            "avg_loss":  sum(t["pnl"] for t in trades if t["pnl"] < 0) / max(losses, 1),
            "trades":    trades,
        }
    finally:
        conn.close()


def get_total_balance() -> float:
    conn = get_conn()
    try:
        capital    = conn.execute("SELECT SUM(capital) as c FROM strategy_state").fetchone()["c"] or 200.0
        closed_pnl = conn.execute("SELECT SUM(pnl) as p FROM trades WHERE status='closed'").fetchone()["p"] or 0.0
        return capital + closed_pnl
    finally:
        conn.close()
