# Daily-Price Source Policy

## Decision

The V0 canonical `daily_prices` dataset accepts only sources whose price
semantics are documented and verified.

## Current sources

| Source | Role | Price basis | Canonical promotion |
| --- | --- | --- | --- |
| Kaggle NSE historical files | Staging and exploration | `unknown` | Not allowed |
| Official NSE daily bhavcopy | Canonical-source candidate | Expected `raw`; verify per source documentation | Allowed after validation |

## Rationale

The Kaggle historical dataset does not document whether its OHLC prices are
raw, adjusted for corporate actions, or otherwise transformed. It remains
useful for exploratory analysis, source profiling, and quarantine workflows,
but must not populate V0 canonical `daily_prices`.

## Consequences

- Keep Kaggle outputs under `data/staging/`.
- Keep `price_basis="unknown"` for Kaggle records.
- Do not merge Kaggle and official-source records.
- Build the V0 canonical adapter from a verified official NSE daily source.
- Continue using Python, Parquet, and DuckDB; Spark and Trino are deferred.