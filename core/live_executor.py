"""
Live Execution Module — Polymarket CLOB V2
==========================================
Uses py-clob-client-v2 (EOA signature type 0).
Collateral: pUSD (wrapped from USDC via setup_polymarket_v2.py).

Strategy:
  ENTRY: FAK order at signal_price × 1.03 (3% worst-price ceiling)
         FAK fills immediately or cancels — no orphan orders.
  EXIT:  GTC limit order at TP/SL price (earns maker rebate).
  HEARTBEAT: Every 5 seconds (Polymarket cancels orders after 15s with no heartbeat).

Environment variables:
  WALLET_PRIVATE_KEY      — MetaMask private key
  WALLET_FUNDER_ADDRESS   — 0x9b21ed1D2D87dB33d3588c3c2CFF64E0b67180E5
  PAPER_MODE              — false to enable live trading
"""

import asyncio
import logging
import os
from typing import Optional, Dict

logger = logging.getLogger(__name__)

# Direct CLOB for auth (signature verification requires direct connection)
CLOB_HOST          = "https://clob.polymarket.com"
# São Paulo proxy for order placement only (bypasses Railway geoblock)
CLOB_PROXY         = "https://polymarket-proxy-black-echo-5111.fly.dev"
CHAIN_ID           = 137
TICK_SIZE          = "0.01"    # 15-min crypto markets use 0.01
NEG_RISK           = False     # 15-min BTC/ETH/SOL markets are not neg-risk
FAK_BUFFER         = 0.03      # 3% worst-price ceiling
HEARTBEAT_INTERVAL = 5         # seconds


class LiveExecutorV2:
    """
    Live order execution using Polymarket CLOB V2.
    Signature type 0 (EOA / MetaMask).
    Collateral: pUSD.
    """

    def __init__(self):
        self._client            = None
        self._direct_client     = None  # for heartbeat (direct CLOB)
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._heartbeat_id: str = ""
        self._initialized: bool = False
        self._private_key       = os.getenv("WALLET_PRIVATE_KEY", "")
        self._funder            = os.getenv("WALLET_FUNDER_ADDRESS", "")
        self._killed: bool      = False  # kill switch flag

    # ── Initialisation ─────────────────────────────────────────────────────────

    def _build_client(self):
        """Build ClobClient with V2 SDK. Runs in executor (blocking)."""
        from py_clob_client_v2 import ClobClient
        from py_clob_client_v2.clob_types import BalanceAllowanceParams, AssetType

        if not self._private_key:
            raise ValueError("WALLET_PRIVATE_KEY env var not set")
        if not self._funder:
            raise ValueError("WALLET_FUNDER_ADDRESS env var not set")

        # Step 1: derive API credentials using direct CLOB (auth needs direct connection)
        temp = ClobClient(
            host=CLOB_HOST,
            chain_id=CHAIN_ID,
            key=self._private_key,
            signature_type=0,
            funder=self._funder,
        )
        creds = temp.create_or_derive_api_key()

        # Step 2: trading client uses PROXY as host — all orders go via São Paulo
        client = ClobClient(
            host=CLOB_PROXY,
            chain_id=CHAIN_ID,
            key=self._private_key,
            creds=creds,
            signature_type=0,
            funder=self._funder,
        )

        # Step 3b: direct client for heartbeat (must use same host as auth)
        self._direct_client = ClobClient(
            host=CLOB_HOST,
            chain_id=CHAIN_ID,
            key=self._private_key,
            creds=creds,
            signature_type=0,
            funder=self._funder,
        )

        # Step 3: sync pUSD balance — use temp client (direct CLOB)
        try:
            temp.set_api_creds(creds)
            temp.update_balance_allowance(
                BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
            )
            logger.info("LiveExecutorV2: pUSD balance synced with CLOB")
        except Exception as e:
            logger.warning(f"LiveExecutorV2: balance sync warning (non-fatal): {e}")

        logger.info(f"LiveExecutorV2: CLOB V2 client ready for {self._funder[:10]}...")
        return client

    async def initialise(self) -> bool:
        """
        Initialise client and start heartbeat.
        Returns True on success, False on failure.
        On failure: bot stays in paper mode — no real money at risk.
        """
        try:
            loop = asyncio.get_event_loop()
            self._client = await loop.run_in_executor(None, self._build_client)
            self._initialized = True
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
            logger.info("LiveExecutorV2: initialised ✅")
            return True
        except Exception as e:
            logger.error(f"LiveExecutorV2: init failed: {e}")
            return False

    # ── Heartbeat ──────────────────────────────────────────────────────────────

    async def _heartbeat_loop(self):
        """
        Send heartbeat every 5 seconds.
        Polymarket cancels all open orders if no heartbeat within 10s+5s buffer.
        """
        while True:
            try:
                await asyncio.sleep(HEARTBEAT_INTERVAL)
                if self._killed:
                    continue  # keep loop alive but don't send if killed
                loop = asyncio.get_event_loop()
                hb_client = self._direct_client or self._client
                resp = await loop.run_in_executor(
                    None,
                    lambda: hb_client.post_heartbeat(self._heartbeat_id)
                )
                if isinstance(resp, dict):
                    self._heartbeat_id = resp.get("heartbeat_id", self._heartbeat_id)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"LiveExecutorV2: heartbeat error (non-fatal): {e}")

    # ── Kill Switch ────────────────────────────────────────────────────────────

    async def kill(self) -> int:
        """
        Cancel ALL open orders on Polymarket and block new orders.
        Returns number of orders cancelled.
        """
        self._killed = True
        cancelled = 0
        if not self._initialized or not self._client:
            return 0
        try:
            loop = asyncio.get_event_loop()
            resp = await loop.run_in_executor(None, self._client.cancel_all)
            # cancel_all returns a list of cancelled order IDs or a dict
            if isinstance(resp, list):
                cancelled = len(resp)
            elif isinstance(resp, dict):
                cancelled = len(resp.get("canceled", []))
            logger.info(f"LiveExecutorV2: kill switch — cancelled {cancelled} orders")
        except Exception as e:
            logger.error(f"LiveExecutorV2: cancel_all error: {e}")
        return cancelled

    async def resume(self):
        """Re-enable live trading after kill switch."""
        self._killed = False
        logger.info("LiveExecutorV2: kill switch deactivated — trading resumed")

    # ── Entry Order ────────────────────────────────────────────────────────────

    async def place_entry(self, sig: Dict) -> Optional[Dict]:
        """
        Place a FAK entry order.

        FAK (Fill-And-Kill): fills immediately against resting orders up to
        the worst-price ceiling, cancels remainder. No orphan orders.

        Args:
            sig: signal dict from momentum_arb.scan()
                 Must have: best_token_id, best_price, trade_side, stake

        Returns:
            {"order_id": str, "actual_price": float} on success
            None on failure (bot falls back to paper)
        """
        if not self._initialized or not self._client:
            logger.error("LiveExecutorV2: not initialised")
            return None
        if self._killed:
            logger.warning("LiveExecutorV2: kill switch active — blocking entry")
            return None

        token_id     = sig.get("best_token_id", "")
        signal_price = float(sig.get("best_price", 0))
        stake        = float(sig.get("stake", 10.0))
        side_str     = sig.get("trade_side", "YES")

        # Validate inputs
        if not token_id:
            logger.error("LiveExecutorV2: no token_id in signal")
            return None
        if signal_price <= 0 or signal_price >= 1:
            logger.error(f"LiveExecutorV2: invalid signal price {signal_price}")
            return None
        if stake <= 0:
            logger.error(f"LiveExecutorV2: invalid stake {stake}")
            return None

        # Side: YES = BUY the YES token, NO = BUY the NO token
        # In both cases we are BUYing (spending pUSD to acquire tokens)
        side = "BUY"

        # Worst-price ceiling for FAK: 3% above signal (slippage protection)
        # Round to tick size 0.01
        raw_worst = signal_price * (1 + FAK_BUFFER)
        worst_price = round(round(raw_worst / 0.01) * 0.01, 2)
        worst_price = min(worst_price, 0.97)  # never exceed 0.97

        logger.info(
            f"LiveExecutorV2: FAK entry {side_str} {sig.get('symbol','')} "
            f"signal={signal_price:.4f} worst={worst_price:.2f} stake=${stake}"
        )

        try:
            from py_clob_client_v2.clob_types import MarketOrderArgsV2, OrderType, PartialCreateOrderOptions

            order_args = MarketOrderArgsV2(
                token_id=token_id,
                amount=stake,            # pUSD amount to spend
                side=side,               # always BUY (we buy outcome tokens)
                price=worst_price,       # worst-price limit (FAK slippage cap)
                order_type=OrderType.FAK,
            )
            options = PartialCreateOrderOptions(
                tick_size=TICK_SIZE,
                neg_risk=NEG_RISK,
            )

            loop = asyncio.get_event_loop()
            resp = await loop.run_in_executor(
                None,
                lambda: self._client.create_and_post_market_order(
                    order_args, options, OrderType.FAK
                )
            )

            logger.info(f"LiveExecutorV2: FAK response: {resp}")

            if not resp:
                logger.error("LiveExecutorV2: empty response from CLOB")
                return None

            # Check order status
            status   = resp.get("status", "")
            order_id = resp.get("orderID", "")

            if status == "unmatched":
                logger.info(
                    f"LiveExecutorV2: FAK unmatched — no resting orders "
                    f"at worst_price={worst_price:.2f}, skipping trade"
                )
                return None

            if status in ("matched", "live", "delayed"):
                # Get actual fill price
                actual_price = await self._get_fill_price(order_id, signal_price)
                logger.info(
                    f"LiveExecutorV2: ✅ filled {side_str} {sig.get('symbol','')} "
                    f"@ {actual_price:.4f} (signal was {signal_price:.4f}, "
                    f"slippage={(actual_price-signal_price)/signal_price*100:+.2f}%)"
                )
                return {
                    "order_id":     order_id,
                    "actual_price": actual_price,
                    "status":       status,
                }

            # Error status
            error = resp.get("error", resp.get("errorMsg", "unknown"))
            logger.error(f"LiveExecutorV2: order rejected status={status} error={error}")
            return None

        except Exception as e:
            logger.error(f"LiveExecutorV2: entry exception: {e}", exc_info=True)
            return None

    async def _get_fill_price(self, order_id: str, fallback: float) -> float:
        """Get actual fill price. Falls back to signal price if unavailable."""
        if not order_id:
            return fallback
        try:
            loop = asyncio.get_event_loop()
            order = await loop.run_in_executor(
                None, lambda: self._client.get_order(order_id)
            )
            if order and order.get("price"):
                return float(order["price"])
        except Exception as e:
            logger.debug(f"LiveExecutorV2: fill price fetch error: {e}")
        return fallback

    # ── Exit Order ─────────────────────────────────────────────────────────────

    async def place_exit(
        self,
        token_id: str,
        side: str,
        exit_price: float,
        size: float,
        reason: str,
    ) -> bool:
        """
        Place a GTC limit order to close a position.
        GTC earns maker rebate (0% fee + rebate share).

        Args:
            token_id:   YES or NO token ID from metadata
            side:       "YES" or "NO" (what we HOLD, so we SELL)
            exit_price: TP or SL price level
            size:       number of shares (stake / entry_price)
            reason:     "take_profit" or "stop_loss"

        Returns:
            True if order placed, False on failure
        """
        if not self._initialized or not self._client:
            logger.error("LiveExecutorV2: not initialised")
            return False
        if self._killed:
            logger.warning("LiveExecutorV2: kill switch active — blocking exit")
            return False
        if not token_id or exit_price <= 0 or size <= 0:
            logger.error(f"LiveExecutorV2: invalid exit params")
            return False

        # Round to tick size
        exit_rounded = round(round(exit_price / 0.01) * 0.01, 2)

        # Sell at exit price - 1 tick to ensure fill (give up 1 cent to guarantee close)
        sell_price = round(max(exit_rounded - 0.01, 0.01), 2)
        sell_size  = round(size, 4)

        logger.info(
            f"LiveExecutorV2: GTC exit SELL {side} "
            f"price={sell_price:.2f} size={sell_size:.4f} reason={reason}"
        )

        try:
            from py_clob_client_v2.clob_types import OrderArgsV2, OrderType, PartialCreateOrderOptions

            order_args = OrderArgsV2(
                token_id=token_id,
                price=sell_price,
                size=sell_size,
                side="SELL",
            )
            options = PartialCreateOrderOptions(
                tick_size=TICK_SIZE,
                neg_risk=NEG_RISK,
            )

            loop = asyncio.get_event_loop()
            resp = await loop.run_in_executor(
                None,
                lambda: self._client.create_and_post_order(
                    order_args, options, OrderType.GTC
                )
            )

            logger.info(f"LiveExecutorV2: exit GTC response: {resp}")

            if resp and resp.get("status") in ("live", "matched"):
                logger.info(f"LiveExecutorV2: ✅ exit order placed ({reason})")
                return True
            else:
                error = resp.get("error", "unknown") if resp else "no response"
                logger.error(f"LiveExecutorV2: exit order failed: {error}")
                return False

        except Exception as e:
            logger.error(f"LiveExecutorV2: exit exception: {e}", exc_info=True)
            return False

    # ── Cleanup ────────────────────────────────────────────────────────────────

    async def shutdown(self):
        """Cancel heartbeat cleanly."""
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
        logger.info("LiveExecutorV2: shutdown complete")
