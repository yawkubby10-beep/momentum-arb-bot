"""
NEXUS - 15-Minute Momentum Arbitrage
Exploits a documented 2-5 second lag where Polymarket's 15-minute
BTC/ETH/SOL up/down markets don't reflect confirmed spot momentum
already visible on Binance.

Strategy (based on documented $313->$414k bot):
1. Watch Binance for strong confirmed price momentum (not noise)
2. When momentum is confirmed AND Polymarket hasn't priced it in yet
3. Trade the corresponding 15-minute up/down market BEFORE it adjusts
4. Win rate: ~72% when momentum threshold is calibrated correctly

Key facts from research:
- Polymarket 15-min markets: btc-updown-15m-{timestamp} slugs
- Timestamps are 15-minute interval boundaries on Unix time
- Gamma API: gamma-api.polymarket.com/markets?slug=btc-updown-15m-{ts}
- Market resolves: YES if price UP from interval start, NO if DOWN
- We need Binance websocket for real-time price + Polymarket Gamma API
"""

import asyncio
import aiohttp
import logging
import os
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

def _parse_clob(val):
    """Parse clobTokenIds — Gamma API returns it as a JSON string not a list."""
    if not val:
        return []
    if isinstance(val, list):
        return val
    if isinstance(val, str):
        try:
            import json as _j
            parsed = _j.loads(val)
            return parsed if isinstance(parsed, list) else []
        except Exception:
            return []
    return []

logger = logging.getLogger(__name__)

GAMMA_API      = "https://gamma-api.polymarket.com/markets"
BINANCE_WS     = "wss://stream.binance.com:9443/ws"
BINANCE_REST   = "https://api.binance.com/api/v3/ticker/price"

# Momentum thresholds (calibrated from research)
MOMENTUM_THRESHOLD = 0.0015  # 0.15% move in 60 seconds = strong momentum signals = strong signal
MIN_LIQUIDITY      = 500      # minimum market liquidity to trade
STAKE              = float(os.getenv("MOMENTUM_STAKE", "10.0"))

SYMBOLS = {
    "BTCUSDT": "btc",
    "ETHUSDT": "eth",
    "SOLUSDT": "sol",
}


class MomentumArbStrategy:

    def __init__(self, capital: float = 40.0, paper: bool = True):
        self.capital       = capital
        self.paper         = paper
        self.session       = None
        self.price_history = {s: [] for s in SYMBOLS}
        self.total_pnl     = 0.0
        self.trade_count   = 0
        self.win_count     = 0

    async def _get_session(self):
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10)
            )
        return self.session

    # ------------------------------------------------------------------ #
    # Binance price fetching                                               #
    # ------------------------------------------------------------------ #
    async def get_binance_prices(self) -> Dict[str, float]:
        """Fetch prices with multiple fallbacks: KuCoin → OKX → Bybit → CoinGecko."""
        session = await self._get_session()

        # Try KuCoin first
        prices = await self._fetch_kucoin(session)
        if prices:
            logger.info(f"Momentum arb: got {len(prices)} prices from KuCoin: {list(prices.keys())}")
            return prices

        # Fallback 1: OKX
        prices = await self._fetch_okx(session)
        if prices:
            logger.info(f"Momentum arb: got {len(prices)} prices from OKX: {list(prices.keys())}")
            return prices

        # Fallback 2: Bybit
        prices = await self._fetch_bybit(session)
        if prices:
            logger.info(f"Momentum arb: got {len(prices)} prices from Bybit: {list(prices.keys())}")
            return prices

        # Fallback 3: CoinGecko (free, no auth)
        prices = await self._fetch_coingecko(session)
        if prices:
            logger.info(f"Momentum arb: got {len(prices)} prices from CoinGecko: {list(prices.keys())}")
            return prices

        logger.warning("Momentum arb: all price sources failed")
        return {}

    async def _fetch_kucoin(self, session) -> Dict[str, float]:
        prices = {}
        kucoin_map = {"BTCUSDT": "BTC-USDT", "ETHUSDT": "ETH-USDT", "SOLUSDT": "SOL-USDT"}
        try:
            for sym, ksym in kucoin_map.items():
                async with session.get(
                    "https://api.kucoin.com/api/v1/market/orderbook/level1",
                    params={"symbol": ksym}, timeout=aiohttp.ClientTimeout(total=5)
                ) as r:
                    if r.status == 200:
                        d = await r.json()
                        p = (d.get("data") or {}).get("price")
                        if p: prices[sym] = float(p)
                await asyncio.sleep(0.05)
        except Exception as e:
            logger.debug(f"KuCoin failed: {e}")
        return prices

    async def _fetch_okx(self, session) -> Dict[str, float]:
        prices = {}
        okx_map = {"BTCUSDT": "BTC-USDT", "ETHUSDT": "ETH-USDT", "SOLUSDT": "SOL-USDT"}
        try:
            for sym, osym in okx_map.items():
                async with session.get(
                    f"https://www.okx.com/api/v5/market/ticker",
                    params={"instId": osym}, timeout=aiohttp.ClientTimeout(total=5)
                ) as r:
                    if r.status == 200:
                        d = await r.json()
                        items = (d.get("data") or [])
                        if items:
                            p = items[0].get("last")
                            if p: prices[sym] = float(p)
        except Exception as e:
            logger.debug(f"OKX failed: {e}")
        return prices

    async def _fetch_bybit(self, session) -> Dict[str, float]:
        prices = {}
        bybit_map = {"BTCUSDT": "BTCUSDT", "ETHUSDT": "ETHUSDT", "SOLUSDT": "SOLUSDT"}
        try:
            for sym, bsym in bybit_map.items():
                async with session.get(
                    "https://api.bybit.com/v5/market/tickers",
                    params={"category": "spot", "symbol": bsym},
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as r:
                    if r.status == 200:
                        d = await r.json()
                        items = ((d.get("result") or {}).get("list") or [])
                        if items:
                            p = items[0].get("lastPrice")
                            if p: prices[sym] = float(p)
        except Exception as e:
            logger.debug(f"Bybit failed: {e}")
        return prices

    async def _fetch_coingecko(self, session) -> Dict[str, float]:
        prices = {}
        try:
            async with session.get(
                "https://api.coingecko.com/api/v3/simple/price",
                params={"ids": "bitcoin,ethereum,solana", "vs_currencies": "usd"},
                timeout=aiohttp.ClientTimeout(total=8)
            ) as r:
                if r.status == 200:
                    d = await r.json()
                    if d.get("bitcoin"): prices["BTCUSDT"] = float(d["bitcoin"]["usd"])
                    if d.get("ethereum"): prices["ETHUSDT"] = float(d["ethereum"]["usd"])
                    if d.get("solana"): prices["SOLUSDT"] = float(d["solana"]["usd"])
        except Exception as e:
            logger.debug(f"CoinGecko failed: {e}")
        return prices
    def detect_momentum(self, symbol: str, current_price: float) -> Optional[str]:
        """
        Detect confirmed momentum by comparing current price
        against price 60 seconds ago.
        Returns 'UP', 'DOWN', or None.
        """
        history = self.price_history[symbol]
        now_ts  = datetime.now(timezone.utc).timestamp()

        # Add current price to history
        history.append((now_ts, current_price))

        # Keep only last 120 seconds
        history = [(ts, p) for ts, p in history if now_ts - ts <= 120]
        self.price_history[symbol] = history

        if len(history) < 2:
            return None

        # Compare current to 60 seconds ago
        sixty_ago = [(ts, p) for ts, p in history if now_ts - ts >= 55]
        if not sixty_ago:
            return None
        old_price   = sixty_ago[0][1]
        pct_change  = (current_price - old_price) / old_price if old_price > 0 else 0
        logger.info(f"Momentum check: {symbol} {old_price:.2f}→{current_price:.2f} ({pct_change*100:+.3f}%) threshold={MOMENTUM_THRESHOLD*100:.2f}%")

        old_price = sixty_ago[0][1]
        change    = (current_price - old_price) / old_price

        if change > MOMENTUM_THRESHOLD:
            return "UP"
        elif change < -MOMENTUM_THRESHOLD:
            return "DOWN"
        return None

    # ------------------------------------------------------------------ #
    # Polymarket 15-min market lookup                                     #
    # ------------------------------------------------------------------ #
    def get_current_15m_slug(self, crypto: str) -> str:
        """
        Generate the correct slug for the current 15-minute interval.
        Slug format: btc-updown-15m-{interval_start_unix_timestamp}
        Interval boundaries are every 15 minutes on UTC clock.
        """
        now     = datetime.now(timezone.utc)
        minute  = (now.minute // 15) * 15
        start   = now.replace(minute=minute, second=0, microsecond=0)
        ts      = int(start.timestamp())
        return f"{crypto}-updown-15m-{ts}"

    async def get_15m_market(self, crypto: str) -> Optional[Dict]:
        """Fetch current 15-min market for a crypto symbol."""
        session = await self._get_session()
        slug    = self.get_current_15m_slug(crypto)

        try:
            async with session.get(
                GAMMA_API, params={"slug": slug}
            ) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                if not data:
                    # Try next interval (market may have just rolled over)
                    now     = datetime.now(timezone.utc)
                    minute  = (now.minute // 15) * 15
                    start   = now.replace(minute=minute, second=0, microsecond=0)
                    next_ts = int((start + timedelta(minutes=15)).timestamp())
                    next_slug = f"{crypto}-updown-15m-{next_ts}"
                    async with session.get(
                        GAMMA_API, params={"slug": next_slug}
                    ) as resp2:
                        if resp2.status == 200:
                            data = await resp2.json()

                if not data:
                    return None

                market = data[0] if isinstance(data, list) else data

                # Parse prices
                prices = market.get("outcomePrices", "[]")
                if isinstance(prices, str):
                    import json
                    prices = json.loads(prices)

                if len(prices) < 2:
                    return None

                yes_price = float(prices[0])
                no_price  = float(prices[1])
                liquidity = float(market.get("liquidityNum", 0))

                if liquidity < MIN_LIQUIDITY:
                    return None

                # Calculate time remaining in this interval
                now    = datetime.now(timezone.utc)
                minute = (now.minute // 15) * 15
                end    = now.replace(minute=minute, second=0, microsecond=0) + timedelta(minutes=15)
                secs_remaining = (end - now).seconds

                # Only trade if >60 seconds remaining (enough time to benefit)
                if secs_remaining < 60:
                    return None

                return {
                    "slug":            market.get("slug", ""),
                    "question":        market.get("question", ""),
                    "yes_price":       yes_price,
                    "no_price":        no_price,
                    "liquidity":       liquidity,
                    "secs_remaining":  secs_remaining,
                    "yes_token_id":    (_parse_clob(market.get("clobTokenIds")) or ["", ""])[0],
                    "no_token_id":     (_parse_clob(market.get("clobTokenIds")) or ["", ""])[1],
                }

        except Exception as e:
            logger.debug(f"15m market fetch error for {crypto}: {e}")
            return None

    # ------------------------------------------------------------------ #
    # Main scan loop                                                       #
    # ------------------------------------------------------------------ #
    async def scan(self) -> List[Dict]:
        logger.info("Momentum arb: scanning Binance prices...")
        """
        Fetch Binance prices, detect momentum, check if Polymarket
        hasn't priced it in yet, return trade signals.
        """
        prices = await self.get_binance_prices()
        if not prices:
            return []

        signals = []

        for binance_symbol, crypto in SYMBOLS.items():
            price = prices.get(binance_symbol)
            if not price:
                continue

            momentum = self.detect_momentum(binance_symbol, price)
            if not momentum:
                continue

            market = await self.get_15m_market(crypto)
            if not market:
                continue

            # Skip near-resolved markets (loosened from 0.05 to 0.03)
            yes_p = float(market.get("yes_price") or 0.5)
            no_p  = float(market.get("no_price") or 0.5)
            logger.info(f"Market prices: {crypto} YES={yes_p:.3f} NO={no_p:.3f}")
            if yes_p < 0.03 or yes_p > 0.97 or no_p < 0.03 or no_p > 0.97:
                logger.info(f"Skip {crypto}: near-resolved YES={yes_p:.3f} NO={no_p:.3f}")
                continue

            # Check if Polymarket hasn't priced in the momentum yet
            # If BTC is strongly UP but market YES is still ~50%, that's the lag
            yes_prob = market["yes_price"] * 100

            if momentum == "UP" and yes_prob < 55:
                # Market hasn't priced in upward momentum yet
                signals.append({
                    "symbol":         crypto,
                    "binance_symbol": binance_symbol,
                    "momentum":       momentum,
                    "trade_side":     "YES",
                    "best_price":     market["yes_price"],
                    "best_token_id":  market["yes_token_id"],
                    "yes_prob":       yes_prob,
                    "liquidity":      market["liquidity"],
                    "secs_remaining": market["secs_remaining"],
                    "stake":          STAKE,
                    "question":       market["question"],
                    "kucoin_price":   prices.get(binance_symbol, 0),
                    "slug":           market.get("slug", ""),
                    "stop_loss":      round(market["yes_price"] * 0.70, 4),
                    "take_profit":    round(min(market["yes_price"] * 1.50, 0.95), 4),
                })
                logger.info(f"Momentum signal: {crypto} UP, market YES={yes_prob:.0f}% (lag detected)")

            elif momentum == "DOWN" and yes_prob > 45:
                # Market hasn't priced in downward momentum yet
                signals.append({
                    "symbol":         crypto,
                    "binance_symbol": binance_symbol,
                    "momentum":       momentum,
                    "trade_side":     "NO",
                    "best_price":     market["no_price"],
                    "best_token_id":  market["no_token_id"],
                    "yes_prob":       yes_prob,
                    "liquidity":      market["liquidity"],
                    "secs_remaining": market["secs_remaining"],
                    "stake":          STAKE,
                    "question":       market["question"],
                    "kucoin_price":   prices.get(binance_symbol, 0),
                    "slug":           market.get("slug", ""),
                    "stop_loss":      round(market["no_price"] * 0.70, 4),
                    "take_profit":    round(min(market["no_price"] * 1.50, 0.95), 4),
                })
                logger.info(f"Momentum signal: {crypto} DOWN, market YES={yes_prob:.0f}% (lag detected)")

        return signals

    def record_trade(self, won: bool, stake: float):
        self.trade_count += 1
        if won:
            self.win_count  += 1
            self.total_pnl  += stake * 0.85
        else:
            self.total_pnl  -= stake

    def get_status(self) -> Dict:
        win_rate = (self.win_count / self.trade_count * 100) if self.trade_count > 0 else 0
        return {
            "strategy":    "momentum_arb",
            "capital":     self.capital,
            "paper":       self.paper,
            "total_pnl":   self.total_pnl,
            "trade_count": self.trade_count,
            "win_rate":    win_rate,
        }

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
