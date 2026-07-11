"""
Polymarket V2 Setup Script
===========================
Run this ONCE before going live. It:
  1. Connects to Polygon
  2. Checks your USDC and MATIC balances
  3. Approves USDC for the CollateralOnramp
  4. Wraps your USDC → pUSD
  5. Approves pUSD for CTF Exchange V2
  6. Syncs the CLOB balance cache

Run:
  python3 setup_polymarket_v2.py

Env vars (set in terminal before running):
  export WALLET_PRIVATE_KEY=your_private_key_here
"""

import os
import sys
import time

try:
    from web3 import Web3
    from web3.middleware import ExtraDataToPOAMiddleware
except ImportError:
    print("Installing web3...")
    os.system("pip install web3 --quiet")
    from web3 import Web3
    from web3.middleware import ExtraDataToPOAMiddleware

# ── Addresses (official Polymarket V2 docs, July 2026) ────────────────────────
WALLET_ADDRESS   = "0x9b21ed1D2D87dB33d3588c3c2CFF64E0b67180E5"
USDC_NATIVE      = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"  # USDC.e (bridged) on Polygon — accepted by CollateralOnramp
PUSD_CONTRACT    = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"  # Polymarket USD
ONRAMP           = "0x93070a847efEf7F70739046A929D47a521F5B8ee"  # CollateralOnramp
CTF_EXCHANGE_V2  = "0xE111180000d2663C0091e4f400237545B87B996B"  # CTF Exchange V2
MAX_UINT256      = 2**256 - 1

RPCS = [
    "https://1rpc.io/matic",
    "https://polygon-rpc.com",
    "https://rpc.ankr.com/polygon",
    "https://polygon.llamarpc.com",
    "https://polygon-bor-rpc.publicnode.com",
]

# ── ABIs ──────────────────────────────────────────────────────────────────────
ERC20_ABI = [
    {"inputs": [{"name":"owner","type":"address"},{"name":"spender","type":"address"}],
     "name": "allowance","outputs": [{"name":"","type":"uint256"}],
     "stateMutability": "view","type": "function"},
    {"inputs": [{"name":"spender","type":"address"},{"name":"amount","type":"uint256"}],
     "name": "approve","outputs": [{"name":"","type":"bool"}],
     "stateMutability": "nonpayable","type": "function"},
    {"inputs": [{"name":"account","type":"address"}],
     "name": "balanceOf","outputs": [{"name":"","type":"uint256"}],
     "stateMutability": "view","type": "function"},
]

ONRAMP_ABI = [
    {"inputs": [
        {"name":"_asset","type":"address"},
        {"name":"_to","type":"address"},
        {"name":"_amount","type":"uint256"}
     ],
     "name": "wrap","outputs": [],
     "stateMutability": "nonpayable","type": "function"},
]

def connect_polygon():
    for rpc in RPCS:
        try:
            print(f"  Trying {rpc}...")
            w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 10}))
            w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
            if w3.is_connected() and w3.eth.chain_id == 137:
                print(f"  ✅ Connected to Polygon")
                return w3
        except Exception as e:
            print(f"  ❌ {e}")
    return None

def send_tx(w3, account, fn_call, gas=100000):
    """Build, sign, send a transaction and wait for receipt."""
    nonce     = w3.eth.get_transaction_count(account.address)
    gas_price = w3.eth.gas_price
    txn       = fn_call.build_transaction({
        "chainId":  137,
        "gas":      gas,
        "gasPrice": gas_price,
        "nonce":    nonce,
        "from":     account.address,
    })
    signed  = w3.eth.account.sign_transaction(txn, account.key)
    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
    print(f"  TX: {tx_hash.hex()}")
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
    return receipt

def sync_clob_balance(private_key):
    """Tell the CLOB API to sync our pUSD balance."""
    try:
        from py_clob_client_v2 import ClobClient
        from py_clob_client_v2.clob_types import BalanceAllowanceParams, AssetType
        client = ClobClient(
            host="https://clob.polymarket.com",
            chain_id=137,
            key=private_key,
        )
        creds = client.create_or_derive_api_key()
        client.set_api_creds(creds)
        resp = client.update_balance_allowance(
            BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
        )
        print(f"  CLOB balance sync: {resp}")
    except Exception as e:
        print(f"  CLOB sync error (non-fatal): {e}")

def main():
    print("=" * 65)
    print("Polymarket V2 Setup — USDC → pUSD → Approve → Trade Ready")
    print("=" * 65)
    print()

    # Get private key
    private_key = os.getenv("WALLET_PRIVATE_KEY", "").strip()
    if not private_key:
        private_key = input("Paste your Polygon private key: ").strip()
    # Clean key — strip whitespace, newlines, any non-hex chars
    private_key = private_key.strip().replace(" ", "").replace("\n", "").replace("\r", "")
    # Remove 0x prefix for cleaning, re-add after
    if private_key.startswith("0x") or private_key.startswith("0X"):
        private_key = private_key[2:]
    # Keep only valid hex characters
    private_key = "".join(c for c in private_key if c in "0123456789abcdefABCDEF")
    if len(private_key) != 64:
        print(f"ERROR: Private key must be 64 hex characters, got {len(private_key)}")
        print("Make sure you copied the full key from MetaMask without spaces")
        sys.exit(1)
    private_key = "0x" + private_key

    # Connect
    print("\nConnecting to Polygon...")
    w3 = connect_polygon()
    if not w3:
        print("ERROR: Cannot connect to Polygon. Check your internet connection.")
        sys.exit(1)

    # Verify key matches wallet
    account = w3.eth.account.from_key(private_key)
    if account.address.lower() != WALLET_ADDRESS.lower():
        print(f"\nERROR: Private key does not match expected wallet")
        print(f"  Key produces: {account.address}")
        print(f"  Expected:     {WALLET_ADDRESS}")
        sys.exit(1)

    wallet = Web3.to_checksum_address(WALLET_ADDRESS)

    # Load contracts
    usdc  = w3.eth.contract(address=Web3.to_checksum_address(USDC_NATIVE), abi=ERC20_ABI)
    pusd  = w3.eth.contract(address=Web3.to_checksum_address(PUSD_CONTRACT), abi=ERC20_ABI)
    ramp  = w3.eth.contract(address=Web3.to_checksum_address(ONRAMP), abi=ONRAMP_ABI)
    exchg = Web3.to_checksum_address(CTF_EXCHANGE_V2)
    onrmp = Web3.to_checksum_address(ONRAMP)
    usdca = Web3.to_checksum_address(USDC_NATIVE)

    # Check balances
    usdc_bal  = usdc.functions.balanceOf(wallet).call() / 1_000_000
    pusd_bal  = pusd.functions.balanceOf(wallet).call() / 1_000_000
    matic_bal = w3.eth.get_balance(wallet) / 10**18

    print(f"\nWallet: {WALLET_ADDRESS}")
    print(f"USDC.e:         ${usdc_bal:.4f}")
    print(f"pUSD:           ${pusd_bal:.4f}")
    print(f"MATIC:          {matic_bal:.6f}")

    if usdc_bal == 0 and pusd_bal == 0:
        print("\nERROR: No USDC or pUSD found. Fund your wallet first.")
        sys.exit(1)

    if matic_bal < 0.001:
        print("\nERROR: Not enough MATIC for gas. Need at least 0.001 MATIC.")
        sys.exit(1)

    print()

    # ── Step 1: Approve USDC for CollateralOnramp ─────────────────────────────
    if usdc_bal > 0:
        usdc_allowance = usdc.functions.allowance(wallet, onrmp).call()
        if usdc_allowance < int(usdc_bal * 1_000_000):
            print(f"Step 1: Approving USDC for CollateralOnramp...")
            receipt = send_tx(w3, account, usdc.functions.approve(onrmp, MAX_UINT256), gas=80000)
            if receipt.status != 1:
                print("  ❌ USDC approval failed")
                sys.exit(1)
            print("  ✅ USDC approved for CollateralOnramp")
        else:
            print("Step 1: USDC already approved for CollateralOnramp ✅")

        # ── Step 2: Wrap USDC → pUSD ──────────────────────────────────────────
        usdc_amount = int(usdc_bal * 1_000_000)
        print(f"\nStep 2: Wrapping ${usdc_bal:.4f} USDC → pUSD...")
        receipt = send_tx(w3, account,
            ramp.functions.wrap(usdca, wallet, usdc_amount),
            gas=200000
        )
        if receipt.status != 1:
            print("  ❌ Wrap failed. Check Polygonscan for details:")
            print(f"  https://polygonscan.com/tx/{receipt.transactionHash.hex()}")
            sys.exit(1)
        print("  ✅ Wrap successful")

        # Verify pUSD received
        time.sleep(3)
        pusd_bal = pusd.functions.balanceOf(wallet).call() / 1_000_000
        print(f"  pUSD balance now: ${pusd_bal:.4f}")
    else:
        print(f"Step 1-2: Already have ${pusd_bal:.4f} pUSD — skipping wrap")

    # ── Step 3: Approve pUSD for CTF Exchange V2 ──────────────────────────────
    pusd_allowance = pusd.functions.allowance(wallet, exchg).call()
    if pusd_allowance < int(pusd_bal * 1_000_000 * 0.9):
        print(f"\nStep 3: Approving pUSD for CTF Exchange V2...")
        receipt = send_tx(w3, account, pusd.functions.approve(exchg, MAX_UINT256), gas=80000)
        if receipt.status != 1:
            print("  ❌ pUSD approval failed")
            sys.exit(1)
        print("  ✅ pUSD approved for CTF Exchange V2")
    else:
        print("\nStep 3: pUSD already approved for CTF Exchange V2 ✅")

    # ── Step 4: Sync CLOB balance cache ───────────────────────────────────────
    print("\nStep 4: Syncing CLOB balance cache...")
    sync_clob_balance(private_key)

    # Final summary
    usdc_final = usdc.functions.balanceOf(wallet).call() / 1_000_000
    pusd_final = pusd.functions.balanceOf(wallet).call() / 1_000_000

    print()
    print("=" * 65)
    print("✅ SETUP COMPLETE — Ready to trade on Polymarket V2!")
    print("=" * 65)
    print(f"  USDC remaining: ${usdc_final:.4f}")
    print(f"  pUSD balance:   ${pusd_final:.4f} (your trading collateral)")
    print()
    print("Next step:")
    print("  Add WALLET_PRIVATE_KEY to Railway env vars")
    print("  Set PAPER_MODE=false in Railway")
    print("  Bot will trade using your pUSD balance")

if __name__ == "__main__":
    main()
