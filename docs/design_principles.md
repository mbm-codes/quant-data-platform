# Design Principles

This project is intentionally conservative.  
Market data systems fail in quiet ways, and the cost of being wrong is usually higher than the cost of being late.

These principles guide how the system is built and how it evolves.

---

## 1. Correctness comes before freshness

If the data is wrong, nothing built on top of it matters.

We prefer delayed data over incorrect data, especially for historical analysis, backtesting, and research workflows.

We try to enforce this by:
- Treating the NSE bhavcopy as a deterministic source
- Separating raw data from cleaned data (Bronze → Silver)
- Running explicit checks on OHLCV fields instead of assuming validity
- Logging failures instead of silently fixing them

This does make pipelines slower and occasionally incomplete. That’s a tradeoff we’re comfortable with.

---

## 2. Results should be reproducible

If a result can’t be reproduced later, it can’t be trusted.

This applies to analytics, backtests, and any downstream ML work. Re-running the same logic on the same inputs should produce the same output, even months later.

In practice:
- Raw data is kept immutable
- Derived datasets are versioned instead of overwritten
- Iceberg snapshots are used to make historical states queryable
- Transformation rules live in config, not buried in code

The cost is extra storage and some operational complexity, but the clarity is worth it.

---

## 3. Pipelines must be safe to re-run

Reprocessing is not an edge case. It’s normal.

Bugs get fixed, assumptions change, and historical data occasionally needs to be corrected. When that happens, re-running a pipeline should not risk corrupting existing data.

We design for this by:
- Using natural keys (`trade_date`, `symbol`, `series`)
- Avoiding full table overwrites
- Relying on atomic commits provided by Iceberg
- Keeping the raw layer immutable

This adds complexity to job design, but it prevents much bigger problems later.

---

## 4. Raw data is immutable; derived data is replaceable

Once raw data is ingested, it doesn’t change.

Any correction or improvement happens by producing a new derived dataset, not by mutating history. This makes it possible to understand how a dataset was produced and what logic was used at the time.

The downside is higher storage usage. The upside is auditability and peace of mind.

---

## 5. Behavior should be explicit, not implicit

Hidden logic is hard to debug and easy to break.

Where possible, we prefer configuration over hard-coded behavior:
- Cleaning and validation rules live outside the code
- Pipelines can be run with or without cleaning enabled
- Failure behavior is documented instead of assumed

This makes the system more verbose, but also more predictable.

---

## 6. Open source first, portable by default

The platform is built using open-source components and standard interfaces.

This keeps costs predictable and avoids locking the system to a specific cloud or vendor. It also makes it easier to reason about failures and performance.

We accept that this means fewer managed conveniences and more responsibility at the platform layer.

---

## Closing note

These principles are not theoretical.  
They exist to make the system easier to operate, easier to change, and harder to break as it grows.
