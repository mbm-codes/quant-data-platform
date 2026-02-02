# Non-Goals

This document lists things the project is **explicitly not trying to do** right now.

Being clear about non-goals is intentional. Market data platforms fail more often from uncontrolled scope than from missing features.

These exclusions may change over time, but only deliberately.

---

## 1. Real-time or low-latency trading systems

This project does not aim to support real-time trading, order execution, or sub-second decision making.

The focus is on **end-of-day and historical market data**, where correctness, auditability, and reproducibility matter more than latency.

If real-time use cases are added later, they will be built as separate systems with different guarantees.

---

## 2. Intraday tick-level data (for now)

The current scope is daily OHLCV data from NSE bhavcopies.

Tick data, order book depth, and intraday bars are intentionally excluded because:
- They significantly increase data volume and complexity
- They require different storage, partitioning, and validation strategies
- They distract from building a solid, reproducible foundation

This may change in the future, but not until the core platform is stable.

---

## 3. Predictive or automated trading models

The platform does not attempt to produce buy/sell signals or automated trading strategies.

Any ML or AI work in this project is intentionally **assistive**, not predictive. It is used for analysis, summarization, comparison, and exploration — not for making trading decisions or optimizing performance metrics.

The goal is to support **disciplined market research**, not to claim alpha.

In practice, this means the platform focuses on:
- Making market data trustworthy and auditable
- Enabling reproducible historical analysis and backtesting
- Reducing bias in research workflows
- Helping humans understand market behavior more clearly

The system deliberately avoids claiming predictive power or trading performance.  
Insights are meant to **inform judgment, not replace it**.


---

## 4. Automatic correction of bad data

The system does not silently “fix” incorrect or suspicious data.

When data violates validation rules:
- It is logged
- It is surfaced
- It is left traceable to the raw source

Manual intervention or explicit reprocessing is preferred over hidden corrections.

---

## 5. Exhaustive feature libraries

The project does not aim to provide hundreds of technical indicators or factor libraries.

Only a small, well-defined set of features will be built, with:
- Clear definitions
- Versioning
- Deterministic behavior

This avoids feature sprawl and makes downstream analysis easier to reason about.

---

## 6. Cloud-vendor–specific optimizations

The platform intentionally avoids dependencies on proprietary cloud services.

While it may be deployed on different providers, the architecture is designed to remain:
- Portable
- Inspectable
- Open-source–first

Vendor-specific optimizations are considered out of scope unless there is a clear, defensible need.

---

## 7. Perfect data coverage guarantees

The system does not promise that all symbols, all days, or all historical periods are always present and complete.

Instead, it aims to:
- Make gaps explicit
- Distinguish missing data from bad data
- Allow safe reprocessing when gaps are addressed

Consumers of the data are expected to handle these realities explicitly.

---

## Closing note

These non-goals exist to keep the project focused and honest.

Saying “no” to certain capabilities today makes it possible to build the right ones well tomorrow.
