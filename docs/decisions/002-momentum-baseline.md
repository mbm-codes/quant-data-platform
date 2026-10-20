# Baseline Momentum Strategy

## Purpose

Provide a transparent, deterministic recommendation baseline that proves the
recommendation, ledger, and evaluation flow.

## Scope

- Informational only; it does not execute trades.
- Evaluates only explicitly supplied NSE equity symbols.
- Uses canonical official daily close prices.
- Requires the existing price-freshness guard to pass.

## Signal

For each symbol, calculate 20-trading-day momentum:

```text
(current close / close 20 trading sessions earlier) - 1