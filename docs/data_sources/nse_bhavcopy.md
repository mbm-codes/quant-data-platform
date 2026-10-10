# NSE Daily Bhavcopy Source

## Scope

This source contains one NSE bhavcopy file per trading day, covering
2023-11-01 through 2026-01-20.

## V0 universe

V0 accepts only rows where `SERIES=EQ`. Other series are excluded because
the initial canonical model represents regular listed equities only.

## Canonical mapping

| Source column | Canonical field |
| --- | --- |
| `SYMBOL` | `symbol` |
| `DATE1` | `trade_date` |
| `OPEN_PRICE` | `open_price` |
| `HIGH_PRICE` | `high_price` |
| `LOW_PRICE` | `low_price` |
| `CLOSE_PRICE` | `close_price` |
| `TTL_TRD_QNTY` | `volume_qty` |

`DATE1` uses the format `%d-%b-%Y`.

## Data semantics

Bhavcopy OHLC and volume fields are treated as raw official daily market data.
The canonical V0 record therefore uses `price_basis="raw"`.

## Excluded fields

`PREV_CLOSE`, `LAST_PRICE`, `AVG_PRICE`, turnover, trade-count, and delivery
fields are retained only in the source file and are not part of the V0
canonical daily-price contract.