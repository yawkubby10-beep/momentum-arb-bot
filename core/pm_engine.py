"""
NEXUS v2 engine — kalshi-v3 doctrine ported to Polymarket 15-minute markets
============================================================================
Replaces the momentum-chase (v1) with model-vs-price trading:

- Spot feed: KuCoin allTickers (1 call, all symbols) with OKX fallback,
  ~2s cadence; EWMA vol (fast/slow) + directly-measured 60s/300s horizon
  vol (1-second wiggle understates multi-minute movement — the exact bug
  that broke the Kalshi model's calibration).
- Interval opens: the strike of an up/down market is the price at interval
  start. Captured live at each 15-minute boundary; an interval whose open
  we did not witness is UNTRADEABLE (we refuse to guess strikes).
- Fair value: p_up = Phi(ln(S/open) / (sigma * sqrt(tau) * tail)).
- CLOB orderbooks (the real, executable ladders) — never Gamma indexer
  marks, which are cached and untradeable.
- Honest paper: taker walks the actual book; fees are zero on Polymarket
  but the spread you cross is real and fully charged.
"""

import asyncio
import json
import logging
import math
import time
from typing import Dict, List, Optional, Tuple

import aiohttp

logger = logging.getLogger(__name__)

GAMMA_API = "https://gamma-api.polymarket.com/markets"
CLOB_BOOK = "https://clob.polymarket.com/book"
KUCOIN_ALL = "https://api.kucoin.com/api/v1/market/allTickers"
OKX_TICKER = "https://www.okx.com/api/v5/market/ticker"

SYMBOLS = {"BTC": "BTC-USDT", "ETH": "ETH-USDT", "SOL": "SOL-USDT"}
OKX_MAP = {"BTC": "BTC-USDT", "ETH": "ETH-USDT", "SOL": "SOL-USDT"}
INTERVAL_S = 900
OPEN_CAPTURE_WINDOW_S = 90     # must see a tick this soon after boundary


def norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def fair_p_up(spot: float, open_px: float, tau_s: float,
              sigma_1s: float, tail: float) -> float:
    if spot <= 0 or open_px <= 0:
        return 0.5
    denom = sigma_1s * math.sqrt(max(tau_s, 1.0)) * tail
    if denom <= 0:
        return 1.0 if spot > open_px else 0.0
    d = max(-8.0, min(8.0, math.log(spot / open_px) / denom))
    return norm_cdf(d)


class VolEstimator:
    """fast ~60s / slow ~15min EWMA + 10s-grid realized vol at 60s/300s."""

    SIGMA_FLOOR = 2.0e-5
    SIGMA_CAP = 2.0e-3
    GRID_S = 10.0
    GRID_KEEP = 300

    def __init__(self):
        prior = (0.55 / math.sqrt(365 * 86400)) ** 2
        self.var_fast = prior
        self.var_slow = prior
        self.last_p: Optional[float] = None
        self.last_t: Optional[float] = None
        self.n_ticks = 0
        self.grid: List[Tuple[float, float]] = []
        self._hcache: Dict[int, Tuple[float, float]] = {}

    def tick(self, t: float, p: float):
        if p <= 0:
            return
        if self.last_p is not None and self.last_t is not None:
            dt = t - self.last_t
            if 0.05 <= dt <= 20.0:
                r = math.log(p / self.last_p)
                sample = (r * r) / dt
                a_f = 0.5 ** (dt / 60.0)
                a_s = 0.5 ** (dt / 900.0)
                self.var_fast = a_f * self.var_fast + (1 - a_f) * sample
                self.var_slow = a_s * self.var_slow + (1 - a_s) * sample
                self.n_ticks += 1
        self.last_p, self.last_t = p, t
        if not self.grid or t - self.grid[-1][0] >= self.GRID_S:
            self.grid.append((t, p))
            if len(self.grid) > self.GRID_KEEP:
                self.grid.pop(0)

    @property
    def sigma_1s(self) -> float:
        s = math.sqrt(max(self.var_fast, 1e-12))
        return min(max(s, self.SIGMA_FLOOR), self.SIGMA_CAP)

    @property
    def sigma_slow(self) -> float:
        s = math.sqrt(max(self.var_slow, 1e-12))
        return min(max(s, self.SIGMA_FLOOR), self.SIGMA_CAP)

    @property
    def spike_ratio(self) -> float:
        return self.sigma_1s / self.sigma_slow if self.sigma_slow > 0 else 1.0

    def _sigma_grid(self, h: float) -> float:
        if len(self.grid) < 4:
            return 0.0
        now = self.grid[-1][0]
        hit = self._hcache.get(int(h))
        if hit and now - hit[0] < 15.0:
            return hit[1]
        samples, j = [], len(self.grid) - 1
        while j >= 0 and len(samples) < 24:
            tj, pj = self.grid[j]
            k = j
            while k >= 0 and tj - self.grid[k][0] < h:
                k -= 1
            if k < 0:
                break
            tk, pk = self.grid[k]
            dt = tj - tk
            if dt > 0 and pk > 0 and pj > 0:
                r = math.log(pj / pk)
                samples.append(r * r / dt)
            j = k
        sig = math.sqrt(sum(samples) / len(samples)) if len(samples) >= 4 \
            else 0.0
        self._hcache[int(h)] = (now, sig)
        return sig

    def sigma_for_tau(self, tau: float, include_fast: bool = True) -> float:
        cands = [self.sigma_slow]
        if include_fast:
            cands.append(self.sigma_1s)
        for h in (60.0, 300.0):
            if h <= max(tau, 60.0) * 2.0:
                s = self._sigma_grid(h)
                if s > 0:
                    cands.append(s)
        return min(max(max(cands), self.SIGMA_FLOOR), self.SIGMA_CAP)


class SpotFeed:
    """REST-poll spot (2s), track vol, ring buffer, and interval OPENS."""

    def __init__(self, session_getter):
        self._session = session_getter
        self.price: Dict[str, float] = {}
        self.last_ts: Dict[str, float] = {c: 0.0 for c in SYMBOLS}
        self.vol: Dict[str, VolEstimator] = {c: VolEstimator() for c in SYMBOLS}
        self.ticks: Dict[str, List[Tuple[float, float]]] = {c: [] for c in SYMBOLS}
        self.opens: Dict[str, Dict[int, float]] = {c: {} for c in SYMBOLS}

    def _ingest(self, crypto: str, t: float, p: float):
        if p <= 0:
            return
        self.price[crypto] = p
        self.last_ts[crypto] = t
        self.vol[crypto].tick(t, p)
        buf = self.ticks[crypto]
        buf.append((t, p))
        while buf and t - buf[0][0] > 30.0:
            buf.pop(0)
        iv = int(t // INTERVAL_S) * INTERVAL_S
        if iv not in self.opens[crypto] and (t - iv) <= OPEN_CAPTURE_WINDOW_S:
            self.opens[crypto][iv] = p
            logger.info(f"interval open captured: {crypto} @{iv} = {p:.2f}")
        cutoff = t - 3 * INTERVAL_S
        for k in [k for k in self.opens[crypto] if k < cutoff]:
            del self.opens[crypto][k]

    def ready(self, crypto: str) -> bool:
        return (time.time() - self.last_ts.get(crypto, 0) < 8.0
                and self.vol[crypto].n_ticks >= 30)

    def interval_open(self, crypto: str, iv_ts: int) -> Optional[float]:
        return self.opens[crypto].get(iv_ts)

    def move_over(self, crypto: str, lookback_s: float) -> Tuple[float, float]:
        buf = self.ticks[crypto]
        if len(buf) < 2:
            return 0.0, 0.0
        now_t, now_p = buf[-1]
        base = None
        for t, p in buf:
            if now_t - t <= lookback_s:
                base = (t, p)
                break
        if base is None:
            base = buf[0]
        dt = now_t - base[0]
        if dt <= 0.2 or base[1] <= 0:
            return 0.0, 0.0
        return math.log(now_p / base[1]), dt

    async def poll_once(self):
        sess = self._session()
        got = set()
        try:
            async with sess.get(KUCOIN_ALL,
                                timeout=aiohttp.ClientTimeout(total=6)) as r:
                data = await r.json()
            now = time.time()
            wanted = {v: k for k, v in SYMBOLS.items()}
            for tkr in (data.get("data") or {}).get("ticker", []):
                c = wanted.get(tkr.get("symbol", ""))
                if c:
                    try:
                        self._ingest(c, now, float(tkr.get("last") or 0))
                        got.add(c)
                    except (TypeError, ValueError):
                        pass
        except Exception as e:
            logger.debug(f"kucoin poll: {e}")
        for c in SYMBOLS:
            if c in got:
                continue
            try:
                async with sess.get(OKX_TICKER,
                                    params={"instId": OKX_MAP[c]},
                                    timeout=aiohttp.ClientTimeout(total=5)) as r:
                    d = await r.json()
                items = d.get("data") or []
                if items:
                    self._ingest(c, time.time(),
                                 float(items[0].get("last") or 0))
            except Exception:
                pass


# ── Market discovery (Gamma for METADATA only, never for prices) ───────────

def _parse_clob(val):
    if isinstance(val, list):
        return val
    if isinstance(val, str):
        try:
            v = json.loads(val)
            return v if isinstance(v, list) else []
        except Exception:
            return []
    return []


def up_index(outcomes: list) -> int:
    """Which outcome index is 'Up'? Never assume ordering blindly."""
    for i, o in enumerate((outcomes or [])[:2]):
        s = str(o).lower()
        if "up" in s and "down" not in s:
            return i
        if "down" in s:
            return 1 - i
    return 0


def resolution_outcome(outcome_prices: list, token_idx: int):
    """Given resolved outcomePrices (e.g. ['1','0']), return 1.0/0.0 for the
    token we hold, or None if the market has not visibly resolved."""
    try:
        prices = [float(x) for x in outcome_prices[:2]]
    except (TypeError, ValueError):
        return None
    if len(prices) < 2 or max(prices) < 0.99:
        return None
    winner = 0 if prices[0] >= prices[1] else 1
    return 1.0 if winner == int(token_idx) else 0.0


def interval_slug(crypto: str, iv_ts: int) -> str:
    return f"{crypto.lower()}-updown-15m-{iv_ts}"


async def fetch_market(session: aiohttp.ClientSession, crypto: str,
                       iv_ts: int) -> Optional[dict]:
    slug = interval_slug(crypto, iv_ts)
    try:
        async with session.get(GAMMA_API, params={"slug": slug},
                               timeout=aiohttp.ClientTimeout(total=8)) as r:
            if r.status != 200:
                return None
            data = await r.json()
    except Exception as e:
        logger.debug(f"gamma {slug}: {e}")
        return None
    if not data:
        return None
    m = data[0] if isinstance(data, list) else data
    tids = _parse_clob(m.get("clobTokenIds"))
    if len(tids) < 2:
        return None
    up_idx = up_index(_parse_clob(m.get("outcomes")) or ["Up", "Down"])
    return {
        "slug": slug, "crypto": crypto, "iv_ts": iv_ts,
        "end_ts": iv_ts + INTERVAL_S,
        "up_idx": up_idx,
        "up_tid": tids[up_idx], "down_tid": tids[1 - up_idx],
        "question": m.get("question", ""),
        "closed": bool(m.get("closed")),
    }


# ── CLOB books: the real ladders ───────────────────────────────────────────

def parse_clob_book(raw: dict) -> dict:
    def norm(levels, reverse):
        out = []
        for lv in levels or []:
            try:
                out.append((float(lv.get("price")), float(lv.get("size"))))
            except (TypeError, ValueError, AttributeError):
                continue
        return sorted(out, key=lambda x: x[0], reverse=reverse)

    bids = norm(raw.get("bids"), reverse=True)     # best bid first
    asks = norm(raw.get("asks"), reverse=False)    # best ask first
    return {
        "bid": bids[0][0] if bids else 0.0,
        "ask": asks[0][0] if asks else 1.0,
        "bids": bids, "asks": asks,
        "bid_size": bids[0][1] if bids else 0.0,
        "ask_size": asks[0][1] if asks else 0.0,
    }


async def fetch_book(session: aiohttp.ClientSession,
                     token_id: str) -> Optional[dict]:
    try:
        async with session.get(CLOB_BOOK, params={"token_id": token_id},
                               timeout=aiohttp.ClientTimeout(total=6)) as r:
            if r.status != 200:
                return None
            return parse_clob_book(await r.json())
    except Exception as e:
        logger.debug(f"clob book: {e}")
        return None


def depth_at(asks: List[Tuple[float, float]], limit: float) -> float:
    return sum(sz for px, sz in asks if px <= limit + 1e-9)


def walk_asks(asks: List[Tuple[float, float]], limit: float,
              want_shares: float) -> Tuple[float, float]:
    """Honest paper IOC: fill up to want at prices <= limit.
    Returns (shares_filled, vwap)."""
    filled, cost = 0.0, 0.0
    for px, sz in asks:
        if px > limit + 1e-9 or filled >= want_shares:
            break
        take = min(sz, want_shares - filled)
        filled += take
        cost += take * px
    return filled, (cost / filled if filled else 0.0)


def bank_bids(bids: List[Tuple[float, float]], limit: float,
              want_shares: float) -> Tuple[float, float]:
    """Sell into bids >= limit (stop-loss exits). Returns (filled, vwap)."""
    filled, proceeds = 0.0, 0.0
    for px, sz in bids:
        if px < limit - 1e-9 or filled >= want_shares:
            break
        take = min(sz, want_shares - filled)
        filled += take
        proceeds += take * px
    return filled, (proceeds / filled if filled else 0.0)
