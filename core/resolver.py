"""
Momentum Arb Trade Resolver
Closes momentum trades via:
1. Live Polymarket CLOB share price hitting TP/SL
2. 30-minute force-close (market expires, API removes it)

All fixes included:
- YES/NO direction correct
- session scope fix
- clobTokenIds JSON string parsing
- token_id saved to metadata
- funding_rate excluded (not used here)
"""

import asyncio
import aiohttp
import logging
import json
from datetime import datetime, timezone
from typing import List, Dict, Optional

from core.database import get_open_trades, close_trade

logger = logging.getLogger(__name__)

GAMMA_API = "https://gamma-api.polymarket.com"

class MomentumResolver:

    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self):
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=aiohttp.ClientTimeout(total=10)
            )
        return self.session

    async def resolve(self) -> List[Dict]:
        """Check all open momentum trades and close those that hit TP/SL or expired."""
        open_trades = get_open_trades("momentum_arb")
        logger.info(f"Resolver: checking {len(open_trades)} momentum trades")
        closed = []

        for trade in open_trades:
            trade_id  = trade["id"]
            symbol    = trade.get("symbol", "")
            side      = (trade.get("side") or "").upper()
            entry     = float(trade.get("entry_price") or 0)
            stop_loss = trade.get("stop_loss")
            take_profit = trade.get("take_profit")
            opened_at = trade.get("opened_at", "")

            if entry <= 0:
                continue

            # ── Check 1: Live Polymarket share price ──────────────────────────
            meta = {}
            try:
                meta = json.loads(trade.get("metadata") or "{}")
            except Exception:
                pass

            # Extract token_id — clobTokenIds stored as JSON string in Gamma API
            raw_tid = meta.get("best_token_id") or meta.get("yes_token_id") or ""
            if isinstance(raw_tid, list):
                token_id = raw_tid[0] if raw_tid else ""
            elif isinstance(raw_tid, str) and raw_tid.startswith("["):
                try:
                    token_id = json.loads(raw_tid)[0]
                except Exception:
                    token_id = ""
            else:
                token_id = raw_tid

            if token_id:
                share_price = await self._fetch_share_price(token_id)
                if share_price > 0:
                    logger.info(f"Share price: {symbol} {share_price:.3f} SL={stop_loss} TP={take_profit} side={side}")
                    # YES: profit when price rises → TP when high, SL when low
                    # NO:  profit when price falls → TP when low,  SL when high
                    is_yes = side in ("YES", "BUY", "long")
                    sl_hit = stop_loss and (
                        (is_yes and share_price <= float(stop_loss)) or
                        (not is_yes and share_price >= float(stop_loss))
                    )
                    tp_hit = take_profit and (
                        (is_yes and share_price >= float(take_profit)) or
                        (not is_yes and share_price <= float(take_profit))
                    )
                    if sl_hit:
                        result = close_trade(trade_id, float(stop_loss), "stop_loss")
                        if result:
                            closed.append(result)
                            logger.info(f"Closed SL: {symbol} {side} @ {share_price:.3f}")
                        continue
                    elif tp_hit:
                        result = close_trade(trade_id, float(take_profit), "take_profit")
                        if result:
                            closed.append(result)
                            logger.info(f"Closed TP: {symbol} {side} @ {share_price:.3f}")
                        continue

            # ── Check 2: 30-minute force close ───────────────────────────────
            if opened_at:
                try:
                    opened = datetime.fromisoformat(opened_at.replace("Z", "+00:00"))
                    age    = (datetime.now(timezone.utc) - opened).total_seconds()
                    if age > 1800:
                        result = close_trade(trade_id, entry, "momentum_expired")
                        if result:
                            closed.append(result)
                            logger.info(f"Closed expired ({age/60:.0f}min): {symbol}")
                except Exception as e:
                    logger.debug(f"Age parse error: {e}")

        return closed

    async def _fetch_share_price(self, token_id: str) -> float:
        """Fetch live Polymarket share price from CLOB API."""
        session = await self._get_session()
        try:
            async with session.get(
                f"https://clob.polymarket.com/price?token_id={token_id}&side=BUY",
                headers={
                    "User-Agent": "Mozilla/5.0",
                    "Accept": "application/json",
                    "Origin": "https://polymarket.com",
                    "Referer": "https://polymarket.com/",
                },
                timeout=aiohttp.ClientTimeout(total=5)
            ) as r:
                if r.status == 200:
                    d = await r.json(content_type=None)
                    return float(d.get("price") or 0)
                else:
                    logger.debug(f"CLOB HTTP {r.status} for token {token_id[:20]}")
        except Exception as e:
            logger.debug(f"CLOB fetch error: {e}")
        return 0.0

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
