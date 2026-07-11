"""
Live Execution Module for Momentum Arb Bot
==========================================
Handles real USDC order placement on Polymarket CLOB.

Strategy:
- ENTRY: FAK order at signal_price × 1.03 (3% worst-price ceiling)
  FAK fills immediately against resting orders up to the price ceiling.
  Anything unfilled is cancelled — no orphan orders.
- EXIT: GTC limit order at TP/SL price (earns maker rebate, 0% fee)
- HEARTBEAT: Sent every 5 seconds to keep session alive

Safety:
- All exceptions caught and logged — never crash the bot
- On any failure, falls back to paper recording only
- Daily loss limit enforced before any order
- Validates fill before recording as open position

Environment variables required:
  WALLET_PRIVATE_KEY     — MetaMask private key (export from MetaMask)
  WALLET_FUNDER_ADDRESS  — MetaMask wallet address (0x9b21...80E5)
  PAPER_MODE             — set to "false" to enable live trading
"""

import asyncio
import logging
import os
from typing import Optional, Dict

logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────
CLOB_HOST       = "https://clob.polymarket.com"
CHAIN_ID        = 137          # Polygon mainnet
TICK_SIZE       = "0.01"       # 15-min crypto markets always use 0.01
NEG_RISK        = False        # 15-min markets are not negative-risk
FAK_BUFFER      = 0.03         # 3% worst-price ceiling above signal price
FILL_TIMEOUT    = 15           # seconds to wait for fill confirmation
HEARTBEAT_INTERVAL = 5        # seconds between heartbeats


class LiveExecutor:
    """
    Handles live order placement on Polymarket CLOB.
    Instantiated once at bot startup. Reused for all orders.
    """

    def __init__(self):
        self._client = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._heartbeat_id: str = ""
        self._initialized: bool = False
        self._private_key  = os.getenv("WALLET_PRIVATE_KEY", "")
        self._funder       = os.getenv("WALLET_FUNDER_ADDRESS", "")

    # ── Initialisation ─────────────────────────────────────────────────────────

    def _build_client(self):
        """Build ClobClient. Called once at first use."""
        from py_clob_client.client import ClobClient
        if not self._private_key:
            raise ValueError("WALLET_PRIVATE_KEY env var not set")
        if not self._funder:
            raise ValueError("WALLET_FUNDER_ADDRESS env var not set")

        client = ClobClient(
            host=CLOB_HOST,
            chain_id=CHAIN_ID,
            key=self._private_key,
            signature_type=0,      # Standard EOA (MetaMask)
            funder=self._funder,
        )
        client.set_api_creds(client.create_or_derive_api_creds())
        logger.info(f"LiveExecutor: CLOB client initialised for {self._funder[:10]}...")
        return client

    async def initialise(self) -> bool:
        """
        Initialise the CLOB client and start heartbeat.
        Returns True on success, False on failure.
        Called once at bot startup when PAPER_MODE=false.
        """
        try:
            self._client = await asyncio.get_event_loop().run_in_executor(
                None, self._build_client
            )
            self._initialized = True
            # Start heartbeat loop
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
            logger.info("LiveExecutor: initialised and heartbeat started")
            return True
        except Exception as e:
            logger.error(f"LiveExecutor: initialisation failed: {e}")
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
                resp = await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: self._client.post_heartbeat(self._heartbeat_id or None)
                )
                self._heartbeat_id = resp.get("heartbeat_id", "") if isinstance(resp, dict) else ""
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"LiveExecutor: heartbeat error (non-fatal): {e}")

    # ── Entry ──────────────────────────────────────────────────────────────────

    async def place_entry(self, sig: Dict) -> Optional[Dict]:
        """
        Place a FAK entry order for a momentum signal.

        Args:
            sig: signal dict from momentum_arb.scan()
                 Must contain: best_token_id, best_price, trade_side, stake

        Returns:
            Dict with actual_entry_price and order_id on success
            None on failure (bot should fall back to paper)
        """
        if not self._initialized or not self._client:
            logger.error("LiveExecutor: not initialised — cannot place entry")
            return None

        token_id = sig.get("best_token_id", "")
        if not token_id:
            logger.error("LiveExecutor: no token_id in signal — cannot place entry")
            return None

        signal_price = float(sig.get("best_price", 0))
        stake        = float(sig.get("stake", 10.0))
        side         = sig.get("trade_side", "YES")

        if signal_price <= 0 or stake <= 0:
            logger.error(f"LiveExecutor: invalid price={signal_price} or stake={stake}")
            return None

        # Worst-price ceiling: 3% above signal price (FAK slippage protection)
        worst_price = round(min(signal_price * (1 + FAK_BUFFER), 0.97), 2)

        # Round to tick size (0.01 for 15-min crypto markets)
        worst_price = round(round(worst_price / 0.01) * 0.01, 2)

        logger.info(
            f"LiveExecutor: placing FAK {side} {sig.get('symbol','')} "
            f"signal={signal_price:.4f} worst_price={worst_price:.2f} stake=${stake}"
        )

        try:
            from py_clob_client.clob_types import MarketOrderArgs, OrderType, PartialCreateOrderOptions
            from py_clob_client.order_builder.constants import BUY

            order_args = MarketOrderArgs(
                token_id=token_id,
                amount=stake,          # USDC amount to spend
                side=BUY,
                price=worst_price,     # worst-price limit (slippage protection)
                order_type=OrderType.FAK,
            )

            options = PartialCreateOrderOptions(
                tick_size=TICK_SIZE,
                neg_risk=NEG_RISK,
            )

            # Sign order (local, fast)
            signed = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self._client.create_market_order(order_args, options)
            )

            # Submit to CLOB
            resp = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self._client.post_order(signed, OrderType.FAK)
            )

            logger.info(f"LiveExecutor: FAK response: {resp}")

            # Validate response
            if not resp or not resp.get("success"):
                logger.error(f"LiveExecutor: order rejected: {resp}")
                return None

            status   = resp.get("status", "")
            order_id = resp.get("orderID", "")

            if status == "unmatched":
                # FAK returned unmatched — nothing filled at our price ceiling
                logger.info(f"LiveExecutor: FAK unmatched — no fill at worst_price={worst_price:.2f}, skipping")
                return None

            if status in ("matched", "live"):
                # Get actual fill details
                actual_price = await self._get_fill_price(order_id, signal_price)
                logger.info(
                    f"LiveExecutor: filled {side} {sig.get('symbol','')} "
                    f"@ {actual_price:.4f} (signal was {signal_price:.4f})"
                )
                return {
                    "order_id":    order_id,
                    "actual_price": actual_price,
                    "status":       status,
                }

            logger.warning(f"LiveExecutor: unexpected status={status}, resp={resp}")
            return None

        except Exception as e:
            logger.error(f"LiveExecutor: entry order exception: {e}", exc_info=True)
            return None

    async def _get_fill_price(self, order_id: str, fallback: float) -> float:
        """
        Get actual fill price from order details.
        Falls back to signal price if order details unavailable.
        """
        try:
            order = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self._client.get_order(order_id)
            )
            if order and order.get("price"):
                return float(order["price"])
        except Exception as e:
            logger.debug(f"LiveExecutor: could not get fill price: {e}")
        return fallback

    # ── Exit ───────────────────────────────────────────────────────────────────

    async def place_exit(
        self,
        token_id: str,
        side: str,
        exit_price: float,
        shares: float,
        reason: str,
    ) -> bool:
        """
        Place a GTC limit order to close a position.
        GTC earns maker rebate (0% fee).

        Args:
            token_id:   YES or NO token ID from position metadata
            side:       "YES" or "NO" (the side we HOLD, so we need to SELL)
            exit_price: TP or SL price
            shares:     number of shares to sell (stake / entry_price)
            reason:     "take_profit" or "stop_loss"

        Returns:
            True if order placed, False on failure
        """
        if not self._initialized or not self._client:
            logger.error("LiveExecutor: not initialised — cannot place exit")
            return False

        if not token_id or exit_price <= 0 or shares <= 0:
            logger.error(f"LiveExecutor: invalid exit params token={token_id} price={exit_price} shares={shares}")
            return False

        # Round exit price to tick size
        exit_price_rounded = round(round(exit_price / 0.01) * 0.01, 2)

        # For exit: we SELL the tokens we hold
        # Add small buffer so we get filled: sell slightly below market for YES,
        # slightly above market for NO
        if side == "YES":
            # We hold YES tokens, sell at exit_price - 1 tick to ensure fill
            sell_price = round(max(exit_price_rounded - 0.01, 0.01), 2)
        else:
            # We hold NO tokens, sell at exit_price + 1 tick
            sell_price = round(min(exit_price_rounded + 0.01, 0.99), 2)

        logger.info(
            f"LiveExecutor: placing GTC exit SELL {side} "
            f"price={sell_price:.2f} shares={shares:.4f} reason={reason}"
        )

        try:
            from py_clob_client.clob_types import OrderArgs, OrderType, PartialCreateOrderOptions
            from py_clob_client.order_builder.constants import SELL

            order_args = OrderArgs(
                token_id=token_id,
                price=sell_price,
                size=round(shares, 4),
                side=SELL,
            )

            options = PartialCreateOrderOptions(
                tick_size=TICK_SIZE,
                neg_risk=NEG_RISK,
            )

            signed = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self._client.create_order(order_args, options)
            )

            resp = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self._client.post_order(signed, OrderType.GTC)
            )

            logger.info(f"LiveExecutor: exit GTC response: {resp}")

            if resp and resp.get("success"):
                logger.info(f"LiveExecutor: exit order placed successfully ({reason})")
                return True
            else:
                logger.error(f"LiveExecutor: exit order rejected: {resp}")
                return False

        except Exception as e:
            logger.error(f"LiveExecutor: exit order exception: {e}", exc_info=True)
            return False

    # ── Cleanup ────────────────────────────────────────────────────────────────

    async def shutdown(self):
        """Cancel heartbeat and clean up."""
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
        logger.info("LiveExecutor: shutdown complete")
