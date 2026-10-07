# Daily Prices Contract

## Purpose
Canonical daily NSE equity-price dataset used by V0 portfolio,
recommendation, and evaluation workflows.

## Dataset
Name: `daily_prices`

## Grain and business key
One row per `exchange`, `symbol`, and `trade_date`.

Business key:
`(exchange, symbol, trade_date)`

A duplicate key is invalid unless a source correction is explicitly processed.
A correction replaces the prior record and creates a new dataset version.

## Price semantics
- V0 stores raw, unadjusted daily prices.
- All prices are denominated in INR.
- `close_price` is the official daily close when available.
- Corporate-action-adjusted returns are out of scope until the adjustment policy
  is implemented.
- Downstream strategy outputs must record `price_basis=raw`.

## Required columns

| Column | Type | Required | Meaning |
|---|---|---:|---|
| exchange | string | yes | Exchange code, initially `NSE` |
| symbol | string | yes | Canonical exchange ticker |
| trade_date | date | yes | Trading date in IST |
| open_price | decimal(20,8) | yes | Unadjusted opening price |
| high_price | decimal(20,8) | yes | Unadjusted session high |
| low_price | decimal(20,8) | yes | Unadjusted session low |
| close_price | decimal(20,8) | yes | Unadjusted official close |
| volume_qty | int64 | yes | Traded quantity; zero is valid |
| currency | string | yes | Initially `INR` |
| price_basis | string | yes | Initially `raw` |
| source_name | string | yes | Source identifier |
| source_file | string | yes | Original source filename or URL |
| source_hash | string | no | Content hash when available |
| ingested_at | timestamp | yes | UTC ingestion timestamp |
| run_id | string | yes | Producing ingestion run |
| dataset_version | string | yes | Immutable curated dataset version |

## Validation rules
- `high_price >= max(open_price, close_price, low_price)`
- `low_price <= min(open_price, close_price, high_price)`
- All prices are positive.
- `volume_qty >= 0`.
- Business key is unique.
- `trade_date` cannot be in the future.