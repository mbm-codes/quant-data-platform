# 🎯 Target Users & Use-Cases

## Overview

The **NSE Quant Research & AI-Native Data Platform** is a **research-first data infrastructure system** built for working with Indian equity market data from the **:contentReference[oaicite:0]{index=0} (NSE)**.

The platform prioritizes:
- Data correctness
- Reproducibility
- Bias-safe analytics
- Clear system boundaries

It is **not** a trading system, execution platform, or retail investing product.

---

## 1. Primary User: Quant Researcher

### Profile
- Works on systematic strategies, factor research, or risk modeling
- Requires long historical data with strict correctness guarantees
- Values reproducibility over speed
- Iterates frequently on hypotheses

---

### Use-Cases

#### UC-Q1: Historical Market Analysis
**Goal:** Study long-term price behavior and market regimes.

**Examples**
- Volatility behavior across bull/bear cycles
- Sector performance during macro events
- Cross-market comparisons over time

**Platform Support**
- 5–10 years of NSE historical OHLCV data
- Bias-safe adjusted price tables
- SQL analytics via Trino
- Large-scale Spark processing

---

#### UC-Q2: Feature Engineering & Factor Research
**Goal:** Design, validate, and iterate on financial features.

**Examples**
- Momentum (1M / 3M / 12M)
- Rolling volatility
- Cross-sectional ranks
- Sector-normalized signals

**Platform Support**
- Spark-based feature pipelines
- Iceberg-backed feature tables
- Versioned feature definitions
- Optional Feast integration for feature reuse

---

#### UC-Q3: Reproducible Backtesting
**Goal:** Evaluate strategies with confidence and auditability.

**Key Requirements**
- Deterministic re-runs
- Point-in-time correctness
- No forward-looking bias

**Platform Support**
- As-of-date–aware datasets
- Backtesting engine with persisted results
- Standard metrics (Sharpe, drawdown, volatility)

---

## 2. Primary User: Data / Platform Engineer

### Profile
- Designs ingestion, processing, and storage systems
- Evaluates cost, scalability, and reliability tradeoffs
- Thinks in terms of ownership boundaries and abstractions

---

### Use-Cases

#### UC-D1: Historical & Incremental Data Ingestion
**Goal:** Build reliable pipelines for exchange data.

**Challenges**
- Large historical backfills
- Trading calendar alignment
- Late or corrected data

**Platform Support**
- One-time historical ingestion pipeline
- Daily incremental ingestion
- Idempotent, re-runnable Spark jobs
- Airflow-based orchestration

---

#### UC-D2: Lakehouse Design & Query Performance
**Goal:** Enable fast analytics without data duplication.

**Platform Support**
- Iceberg table format
- Bronze / Silver data separation
- Partitioning and bucketing strategy
- Trino for low-latency SQL queries

---

#### UC-D3: Data Quality & Reliability
**Goal:** Detect and prevent silent data corruption.

**Platform Support**
- Automated quality checks (missing bars, OHLC sanity)
- Pipeline failure on critical violations
- Freshness and completeness metrics

---

## 3. Secondary User: ML Engineer / Applied Scientist

### Profile
- Trains models using engineered features
- Needs consistent, point-in-time-correct training data
- Tracks experiments and model performance

---

### Use-Cases

#### UC-M1: ML Dataset Generation
**Goal:** Create training datasets without feature drift.

**Platform Support**
- Point-in-time-correct feature joins
- Iceberg-based feature storage
- Optional Feast for feature access
- Versioned datasets

---

#### UC-M2: Experiment Tracking & Comparison
**Goal:** Compare models and configurations reliably.

**Platform Support**
- MLflow experiment tracking
- Stored metrics and artifacts
- Clear separation of experiments

---

## 4. Secondary User: AI-Assisted Researcher

### Profile
- Uses LLMs to reduce cognitive load
- Does not rely on AI for decision-making

---

### Use-Cases

#### UC-A1: Data Anomaly Explanation
**Goal:** Understand data or pipeline failures quickly.

**Platform Support**
- LLM-generated explanations for:
  - Missing data
  - Validation failures
  - Backtest anomalies

---

#### UC-A2: Research Summarization
**Goal:** Accelerate insight extraction and documentation.

**Examples**
- Summarize backtest performance
- Compare strategy iterations
- Auto-document feature definitions

**Constraint**
- AI outputs are assistive, not authoritative

---

## 5. Tertiary User: Hiring Manager / Interviewer

### Profile
- Evaluates system design maturity
- Assesses real-world tradeoff awareness

---

### What This Project Demonstrates

- Bias-safe financial data modeling
- Modern lakehouse architecture
- Clear scoping and non-goals
- Awareness of failure modes and tradeoffs

---

## 6. Explicitly Unsupported Use-Cases (Non-Goals)

The platform does **not** support:

- Live trading or order execution
- Intraday or tick-level strategies
- Alpha or profitability claims
- Retail portfolio management
- Regulatory or compliance reporting

These exclusions are intentional design decisions.

---

## Summary

> This platform is designed for **researchers and engineers who value correctness, reproducibility, and clarity**, rather than speed, hype, or execution.

This positioning makes the project:
- Strong for FAANG / Staff Data roles
- Credible for quant research teams
- Safe and honest for open-source publication
