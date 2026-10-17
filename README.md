# 📊 NSE Quant Data Platform — Lakehouse Architecture

A **quant-grade, research-first data platform** for Indian equity markets built on a **modern lakehouse architecture**, with a strong emphasis on **data correctness, reproducibility, and explainability**.

This project is designed as **data and research infrastructure**, not a trading system.

---

## ✨ Key Highlights

- 🇮🇳 **Indian markets–focused** (NSE-only MVP)
- ❄️ Modern **lakehouse architecture** (Spark + Iceberg + Trino)
- 📚 **Bias-safe** historical and daily market data pipelines
- 🧪 Reproducible **feature engineering & backtesting**
- 🤖 **AI / LLM-assisted research workflows** (assistive, not predictive)
- 💰 **Cost-efficient** and cloud-portable (DigitalOcean)
- 🧱 Built with **open-source–first principles**

---

## 📍 Current Status

✅ NSE historical OHLCV data ingestion — complete  
✅ Daily incremental data pipeline — complete  
🔄 Bronze/Silver lakehouse layers (Iceberg) — in progress  
🔄 Transformation pipeline — in progress  
⬜ Trino analytics layer — planned  
⬜ Feature engineering — planned  
⬜ Backtesting framework — planned  

---

## 🎯 Project Goals

This platform aims to:

1. Provide **reliable, bias-safe NSE market data**
2. Enable **large-scale time-series analytics**
3. Support **reproducible quant research**
4. Demonstrate **Staff-level data platform engineering**
5. Serve as a **reference architecture** for Indian market data systems

---

## 🚫 Explicit Non-Goals

This project intentionally does **NOT**:

- Execute trades or place orders  
- Claim alpha or profitability  
- Perform real-time / tick-level trading  
- Integrate with brokers or exchanges  
- Act as financial advice  

> This is a **research and infrastructure platform**, not a trading product.

---

## 🧠 Target Users

- **Quant Researchers** – bias-safe data, reproducible experiments  
- **Data / Platform Engineers** – ingestion, lakehouse, orchestration design  
- **ML Engineers** – feature consumption, experiment tracking  
- **Hiring Managers / Interviewers** – system design depth & tradeoff clarity  

---
## 🏗️ High-Level Architecture

```text
                +----------------------+
                |     NSE Bhavcopy     |
                +----------+-----------+
                           |
                    (Historical + Daily)
                           |
                    +------v------+
                    |   Spark     |
                    | Ingestion   |
                    +------+------+
                           |
                   Iceberg (Bronze)
                           |
                +----------v-----------+
                |   Spark Processing   |
                | (Adjustments, Bias)  |
                +----------+-----------+
                           |
                   Iceberg (Silver)
                           |
               +-----------v------------+
               |        Trino           |
               |   Analytics & SQL      |
               +-----------+------------+
                           |
         +-----------------v------------------+
         | Features | Backtests | ML | AI     |
         +------------------------------------+
-->


## V0 Local Quickstart

V0 is laptop-first. It uses Python, Parquet, and DuckDB; it does not require Spark, Iceberg, or Trino.

### Setup

Install the project dependencies:

```bash
uv sync
```

### Load the sample dataset

Validate the included daily-price CSV and write canonical, year-partitioned Parquet data:

```bash
uv run python -m core.cli load-csv tests/fixtures/v0/daily_prices.csv
```

### Query canonical prices

Query the local Parquet dataset through DuckDB:

```bash
uv run python -m core.cli query --symbol RELIANCE
```

### Run V0 tests

```bash
uv run pytest tests/v0 -q
```

### V0 Data Flow

```text
CSV → schema validation → partitioned Parquet → DuckDB → CLI query
```

### Scope Note

The existing Spark-based ingestion, Iceberg setup, and Spark test suite are preserved as legacy and future scale-out work. They are not required for the V0 local workflow.

## Portfolio valuation (V0)

Create a transaction CSV using this schema:

```csv
transaction_id,account_id,symbol,trade_date,side,quantity,unit_price,fees
example-001,main,RELIANCE,2024-01-02,BUY,10,2510.00,20.00
example-002,main,RELIANCE,2024-01-03,SELL,4,2520.00,20.00

### Price freshness

By default, `portfolio-value` rejects a close price that is more than seven
calendar days older than the requested `--as-of` date. Override the threshold
only when appropriate:

```bash
uv run python -m core.cli portfolio-value \
  examples/portfolio_transactions.example.csv \
  --as-of 2024-01-05 \
  --max-price-age-days 7 \
  --run-id m3-freshness-check

