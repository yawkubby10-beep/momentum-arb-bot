"""
NEXUS v2 Resolver — resolution truth, never fiction
====================================================
v1 sins, all removed:
- NO-side stop logic compared the NO token's own price against YES-style
  thresholds -> every DOWN trade insta-stopped at -30% on first pass.
- 30-minute force-close AT ENTRY erased every resolution outcome from the
  books (wins to $1 and losses to $0 both booked as breakeven).
- Stops filled at the stop PRICE, not the price that triggered them.

v2 rules:
- Before interval end: model-based stop only (p_side < STOP_P), filled by
  walking the REAL bids of the token we hold.
- After interval end: poll Gamma until outcomePrices show the winner, then
  settle at exactly $1.00 or $0.00. If resolution is overdue we ALERT and
  HOLD — we never invent an exit price.
"""

import json
import logging
import os
import time
from typing import Dict, List, Optional

import aiohttp

from core import pm_engine
from core.database import close_trade, get_open_trades

logger = logging.getLogger(__name__)

STOP_P = float(os.getenv("STOP_P", "0.30"))
STOP_MIN_TAU = float(os.getenv("STOP_MIN_TAU", "30"))
TAIL_MULT = float(os.getenv("TAIL_MULT", "1.25"))
RES_OVERDUE_S = float(os.getenv("RES_OVERDUE_S", "1800"))


class PMResolver:
    def __init__(self, spot: "pm_engine.SpotFeed", session_getter):
        self.spot = spot
        self._session = session_getter
        self._overdue_alerted: set = set()
        self.pending_alerts: List[str] = []

    async def resolve(self) -> List[Dict]:
        closed: List[Dict] = []
        rows = [t for t in get_open_trades()
                if str(t.get("strategy", "")).startswith("pm_")]
        now = time.time()
        sess = self._session()
        for t in rows:
            try:
                meta = json.loads(t.get("metadata") or "{}")
            except Exception:
                meta = {}
            end_ts = float(meta.get("end_ts") or 0)
            token_id = meta.get("token_id") or ""
            token_idx = int(meta.get("token_idx", 0))
            crypto = meta.get("crypto", "")
            iv_ts = int(meta.get("iv_ts") or 0)
            side_is_up = bool(meta.get("side_is_up", t.get("side") == "YES"))
            shares = float(t.get("size") or 0)
            if not token_id or end_ts <= 0:
                continue

            if now < end_ts - 5:
                res = await self._try_stop(sess, t, meta, crypto, iv_ts,
                                           side_is_up, token_id, shares,
                                           end_ts - now)
                if res:
                    closed.append(res)
                continue

            # ── resolution ────────────────────────────────────────────
            m = await pm_engine.fetch_market(sess, crypto, iv_ts)
            outcome_prices = None
            if m is None:
                # fetch_market returns None on parse issues; query raw
                outcome_prices = await self._raw_outcome_prices(
                    sess, meta.get("slug", ""))
            else:
                outcome_prices = await self._raw_outcome_prices(
                    sess, m["slug"])
            exit_px = (pm_engine.resolution_outcome(outcome_prices, token_idx)
                       if outcome_prices else None)
            if exit_px is not None:
                res = close_trade(t["id"], exit_px, "resolution")
                if res:
                    closed.append(res)
                continue
            if now > end_ts + RES_OVERDUE_S and t["id"] not in self._overdue_alerted:
                self._overdue_alerted.add(t["id"])
                self.pending_alerts.append(
                    f"⏳ resolution overdue: #{t['id']} "
                    f"{meta.get('slug','?')} — holding, will keep polling. "
                    f"NOT inventing an exit price.")
        return closed

    async def _raw_outcome_prices(self, sess, slug: str) -> Optional[list]:
        if not slug:
            return None
        try:
            async with sess.get(pm_engine.GAMMA_API, params={"slug": slug},
                                timeout=aiohttp.ClientTimeout(total=8)) as r:
                if r.status != 200:
                    return None
                data = await r.json()
            if not data:
                return None
            m = data[0] if isinstance(data, list) else data
            return pm_engine._parse_clob(m.get("outcomePrices"))
        except Exception as e:
            logger.debug(f"outcomePrices {slug}: {e}")
            return None

    async def _try_stop(self, sess, t, meta, crypto, iv_ts, side_is_up,
                        token_id, shares, tau) -> Optional[Dict]:
        if tau <= STOP_MIN_TAU or not self.spot.ready(crypto):
            return None
        open_px = self.spot.interval_open(crypto, iv_ts)
        if not open_px:
            return None          # cannot price without the witnessed open
        S = self.spot.price[crypto]
        sigma = self.spot.vol[crypto].sigma_for_tau(tau)
        p_up = pm_engine.fair_p_up(S, open_px, tau, sigma, TAIL_MULT)
        p_side = p_up if side_is_up else 1.0 - p_up
        if p_side >= STOP_P:
            return None
        book = await pm_engine.fetch_book(sess, token_id)
        if not book or book["bid"] < 0.03:
            return None          # nothing to salvage
        filled, vwap = pm_engine.bank_bids(book["bids"], book["bid"] - 0.02,
                                           shares)
        if filled < shares * 0.8:
            return None          # thin bids: hold rather than partial-mangle
        return close_trade(t["id"], vwap, "stop_loss")


# import-compat alias for anything still importing the old name
MomentumResolver = PMResolver
