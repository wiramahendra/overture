# Overture Billing — Flat Model

Canonical: **flat per-tier runtime limit**, not per-device metering.

| Tier | Runtimes | Price |
|---|---|---|
| seed | 3 | $29/mo (2900c) |
| horizon | 50 | $149/mo (14900c) |
| infinite | 500 | $699/mo (69900c) |

Source: `billing/tier.go:14 TierRuntimeLimit` + `billing/tier.go:21 TierMonthlyPriceCents`

- Enforcement: `billing/enforcer.go` via Redis counter `overture:runtimes:<tenant>:count` (also accepts legacy `igris:` key).
- Registration gate: `api/routes_runtime.go:190 CheckRuntimeLimit` 402 when exceeded.
- `LICENSE_SYSTEM_DESIGN.md` describes a $49/device metered design — **archived**. Not used (flat is active). Keep for reference only.
- Polar: price IDs are flat tier prices (not per-seat). `billing/polar_client.go` TODO placeholder must be replaced with actual Polar product IDs before billing goes live.
