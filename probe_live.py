"""
probe_live.py — prove the pipe, risk nothing
=============================================
Answers ONE question definitively: will Polymarket's CLOB accept a signed
order from THIS machine and THIS wallet?

How it stays risk-free: it sends a FAK BUY with a worst-price ceiling of
$0.01 on a market trading far above that. A FAK either fills within the
ceiling or dies instantly — at 1 cent against a ~50 cent book it CANNOT
fill. So the CLOB must fully validate auth, signature, geo/compliance and
balance, then kill the order. Whatever it replies is the truth about the
pipe.

Run it from the machine you intend to trade from (Hetzner Finland or your
Mac in Accra — NOT Railway US, which Polymarket geo-blocks for orders):

    export WALLET_PRIVATE_KEY=...      # Magic wallet key
    export WALLET_FUNDER_ADDRESS=...
    PROBE_CONFIRM=YES python probe_live.py

Verdicts:
  PIPE PROVEN   — order accepted+killed (or accepted with 0 fill)
  GEO/COMPLIANCE BLOCK — the exact error is printed; move host
  AUTH FAIL     — credential/signature problem; the error says which
"""

import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import aiohttp  # noqa: E402

from core import pm_engine  # noqa: E402
from core.live_executor import LiveExecutorV2  # noqa: E402

import time  # noqa: E402


async def main() -> int:
    if os.getenv("PROBE_CONFIRM") != "YES":
        print("Refusing to run: set PROBE_CONFIRM=YES to send the "
              "(unfillable) probe order.")
        return 2
    if not os.getenv("WALLET_PRIVATE_KEY") or not os.getenv(
            "WALLET_FUNDER_ADDRESS"):
        print("Missing WALLET_PRIVATE_KEY / WALLET_FUNDER_ADDRESS env vars.")
        return 2

    print("── step 1: find a live 15-min market (Gamma metadata) ──")
    async with aiohttp.ClientSession() as sess:
        iv = int(time.time() // pm_engine.INTERVAL_S) * pm_engine.INTERVAL_S
        m = await pm_engine.fetch_market(sess, "BTC", iv)
        if not m:
            m = await pm_engine.fetch_market(sess, "BTC",
                                             iv + pm_engine.INTERVAL_S)
        if not m:
            print("FAIL: could not find a current btc-updown-15m market. "
                  "Check network / Gamma availability from this host.")
            return 1
        print(f"   market: {m['slug']}  up_token={m['up_tid'][:18]}...")
        book = await pm_engine.fetch_book(sess, m["up_tid"])
        if book:
            print(f"   book: bid {book['bid']:.2f} ask {book['ask']:.2f} "
                  f"(sizes {book['bid_size']:.0f}/{book['ask_size']:.0f})")
            if book["ask"] <= 0.05:
                print("   ask too close to probe ceiling — using DOWN token")
                m["up_tid"] = m["down_tid"]

    print("── step 2: initialise live executor (auth + creds + heartbeat) ──")
    ex = LiveExecutorV2()
    ok = await ex.initialise()
    if not ok:
        print("VERDICT: AUTH FAIL — client initialisation failed. The error "
              "above names the layer (key derivation / creds / balance).")
        return 1
    print("   executor initialised ✅")

    print("── step 3: send unfillable FAK (BUY @ $0.01 ceiling, $1 stake) ──")
    resp = await ex.place_entry({
        "best_token_id": m["up_tid"],
        "best_price": 0.01,
        "worst_price": 0.01,
        "trade_side": "YES",
        "stake": 1.0,
        "symbol": "PROBE",
        "probe": True,
    })

    print("── step 4: safety cancel-all ──")
    try:
        cancelled = await ex.kill()
        print(f"   cancel_all: {cancelled} orders cancelled")
        await ex.resume()
    except Exception as e:
        print(f"   cancel_all error (check manually): {e!r}")

    print()
    if resp is not None:
        print("VERDICT: PIPE PROVEN ✅ — the CLOB validated and accepted a "
              "signed order from this host (it killed at the 1¢ ceiling, as "
              "designed). Live trading from this machine is possible.")
        print(f"   response: {resp}")
        return 0
    print("VERDICT: ORDER REJECTED — read the LiveExecutorV2 log lines "
          "above. A geo/compliance message means move the host (Hetzner "
          "Finland / Accra); a signature/auth message means the wallet "
          "setup, not the location.")
    return 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
