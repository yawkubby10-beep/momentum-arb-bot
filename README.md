# NEXUS v2 — Polymarket 15-minute fair-value engine

Rewritten from the v1 momentum-chaser after a full audit. v1's paper record
was unusable: every DOWN trade insta-stopped at −30% (NO-token price checked
against YES-style thresholds), resolutions were erased (30-min force-close
**at entry** booked $1 wins and $0 losses as breakeven), stops filled at the
stop *price* rather than the trigger price, entries priced off Gamma's cached
indexer marks, and the DB sign-flipped NO-side P&L. All fixed.

## v2 design (kalshi-v3 doctrine, Polymarket economics)

- **Fair value vs CLOB books.** p_up = Φ(ln(S/open)/(σ√τ·tail)) from a live
  spot feed with horizon-matched vol (60s/300s realized, not 1-second wiggle
  scaled up). Prices and depth come from the real CLOB orderbook — never the
  Gamma indexer.
- **Witnessed strikes only.** An up/down market's strike is the interval-open
  price. The bot captures it live at each 15-minute boundary; an interval
  whose open it did not witness is untradeable. No guessed strikes.
- **Two taker strategies.** CONV: buy p≥0.90 favorites when the ask offers
  ≥2¢ edge (Polymarket charges zero trading fees — the spread is the only
  cost, and it's charged honestly). LAG: on a vol-normalized burst, take only
  if the book still lags fair value by ≥4¢ — priced with burst-free vol.
  Divergence guard: if the model beats the ask by >12¢, assume WE are wrong.
- **Resolution truth.** Positions settle at the market's actual $1/$0
  outcome. Overdue resolutions alert and hold — never an invented exit.
- **Calibration line.** Every entry stores model_p; `/pnl` shows claimed vs
  resolved reality. This number decides everything.

## Live path

`probe_live.py` proves the pipe with zero fill risk (an unfillable 1¢ FAK —
the CLOB must validate auth, signature, geo and balance to kill it):

```
export WALLET_PRIVATE_KEY=... WALLET_FUNDER_ADDRESS=...
PROBE_CONFIRM=YES python probe_live.py
```

Run it from a host Polymarket permits: **Hetzner Finland or Ghana — not
Railway US** (US IPs are geo-blocked for order posting). Deployment for live
therefore targets the Hetzner box. Paper mode runs anywhere.

Live entries use exact EV-gated worst-price FAKs (no +3% chasing). Live
positions ride to resolution (stops are paper-only in v2.0).

Attach a volume at `/data` (DB defaults to `/data/momentum.db`) or history
resets on redeploy.
