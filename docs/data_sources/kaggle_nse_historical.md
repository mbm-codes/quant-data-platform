# Kaggle NSE Historical Price Dataset

## Overview

This dataset contains historical daily trading data for equities listed on the National Stock Exchange of India (NSE). It was obtained from Kaggle and is sourced from the remote repository [bhaktijpatil/nse-stock-market-historical-data](https://github.com/bhaktijpatil/nse-stock-market-historical-data).

The data is stored as one gzip-compressed CSV file per symbol, for example:

```text
data_sources/nse/hist_files/RELIANCE.NS.csv.gz
```

The current local drop contains 1,940 symbol files.

## Coverage

The published dataset description states that it contains NSE historical data through October 31, 2023. Local inspection of `RELIANCE.NS.csv.gz` shows dates from January 1, 1996 through November 1, 2023.

Coverage varies by symbol because securities list, delist, and begin trading at different times. The dataset should therefore not be assumed to provide a complete, point-in-time NSE universe for every historical date.

## File Format

Each file is a compressed CSV with the following columns:

| Column | Description |
|---|---|
| `Date` | Trading timestamp, observed with a `+05:30` India timezone offset |
| `Open` | Opening price for the trading day |
| `High` | Highest traded price for the trading day |
| `Low` | Lowest traded price for the trading day |
| `Close` | Closing price for the trading day |
| `Volume` | Number of shares traded |
| `Dividends` | Dividend value recorded for the trading day |
| `Stock Splits` | Stock-split or reverse-split coefficient recorded for the trading day |

The symbol is derived from the filename. For example, `RELIANCE.NS.csv.gz` maps to source symbol `RELIANCE.NS`.

## Intended Use in This Project

This dataset is the initial historical source for M1 ingestion. The ingestion process will:

1. Preserve the original source files as immutable inputs.
2. Parse timestamps into an IST `trade_date`.
3. Normalize source symbols, such as `RELIANCE.NS`, to canonical NSE symbols, such as `RELIANCE`.
4. Validate the data contract, including OHLC relationships, non-negative volume, and duplicate business keys.
5. Store validated records as partitioned Parquet for local DuckDB analysis.
6. Record source file, run ID, dataset version, and ingestion timestamp for reproducibility.

## Price-Adjustment Policy

The dataset includes `Dividends` and `Stock Splits`, but its published description does not explicitly confirm whether the OHLC prices are raw, split-adjusted, dividend-adjusted, or otherwise adjusted.

Therefore, the project must treat price adjustment status as **unknown** until it is independently verified. Historical records from this source must not be labeled `price_basis=raw` or used for return calculations that assume raw prices without an explicit adjustment-policy decision.

## Limitations

- This is a third-party Kaggle dataset, not a direct NSE exchange feed.
- Historical constituent membership is not included; survivorship bias may be present.
- Completeness, corrections, and delisted-symbol coverage have not yet been independently validated.
- The stated end date and observed file coverage differ slightly.
- Licensing, attribution, and redistribution terms must be reviewed from the original source before publishing or redistributing the data.

## Aggregate Summary
Inventory run: 1,940 files processed
Valid staging rows: 6,444,054
Quarantined rows: 4,130
Files containing rejected rows: 343

## Source Attribution

- Dataset origin: Kaggle
- Remote repository: [bhaktijpatil/nse-stock-market-historical-data](https://github.com/bhaktijpatil/nse-stock-market-historical-data)
- Local source directory: `data_sources/nse/hist_files/`
- Initial ingestion status: pending M1 historical-source validation