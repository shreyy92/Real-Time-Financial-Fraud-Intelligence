# Member 2 — PySpark Distributed Processing & Feature Engineering

## Real-Time Financial Fraud Intelligence Platform (B.Tech Big Data Project)

---

## 1. Responsibilities of Member 2

Member 2 is responsible for distributed big data processing, feature engineering, and data validation between the cleaning layer (Member 1) and downstream consumers (Member 4 Machine Learning, Member 3 Kafka/Streaming, Member 5 Storage & APIs):

```text
Member 1 (Cleaning) ──► Clean Parquet ──► Member 2 (PySpark Processing) ──► ML-Ready Parquet ──► Member 4 (ML)
                                                   ▲
                                                   └── Member 3 (Streaming Parity)
```

Key objectives:
- Consume cleaned Parquet datasets (`paysim_clean.parquet` and `ieee_clean.parquet`) without re-cleaning.
- Implement distributed PySpark feature pipelines using native Catalyst expressions (zero Python UDFs).
- Enforce strict row-grain preservation: **1 input transaction == 1 output feature row**.
- Enforce strict non-leakage: historical and velocity aggregations use strictly past information (`step < current_step` and `TransactionDT < current_DT`).
- Enforce architectural isolation between **Tier A** (authorization-time) and **Tier B** (post-transaction outcome state).
- Validate input/output schemas, feature quality, duplicate prevention, and target leakage.
- Generate ML-ready Parquet datasets accompanied by comprehensive documentation and output contracts.

---

## 2. Directory Structure

```text
member2_spark/
├── README.md                          # Master module documentation (this file)
├── config/
│   └── feature_config.yaml            # Configurable paths, split boundaries, and window sizes
├── docs/
│   ├── FEATURE_DICTIONARY.md          # Complete catalog of all features, types, formulas, and tiers
│   ├── FEATURE_ENGINEERING_DESIGN.md  # System architecture, DAG, and PySpark optimizations
│   ├── DATASET_FEATURES.md            # In-depth breakdown of PaySim and IEEE feature families
│   ├── LEAKAGE_CONTROL.md             # Anti-leakage policy, simulator artifact controls, and rules
│   └── OUTPUT_CONTRACT.md             # Downstream contracts for Member 3 and Member 4
├── src/
│   ├── __init__.py                    # Module export
│   ├── spark_session.py               # Reusable, optimized SparkSession builder
│   ├── paysim/
│   │   ├── __init__.py                # PaySim exports
│   │   ├── load.py                    # Parquet reader with schema validation
│   │   ├── temporal_features.py       # txn_id, step_hour, cyclical sin/cos, type one-hots
│   │   ├── amount_features.py         # Tier A amount transforms & Tier B balance deltas
│   │   ├── destination_features.py    # Strictly historical destination behavioral metrics
│   │   ├── velocity_features.py       # 24h and 168h destination rolling velocity windows
│   │   └── build_features.py          # End-to-end PaySim pipeline orchestrator
│   ├── ieee/
│   │   ├── __init__.py                # IEEE exports
│   │   ├── load.py                    # Parquet reader with schema validation
│   │   ├── missingness_features.py    # Structured missingness & identity completeness
│   │   ├── card_features.py           # Deterministic card proxy generation (card1-6 + addr1)
│   │   ├── temporal_features.py       # Relative diurnal hour, day index, cyclical encodings
│   │   ├── aggregate_features.py      # Amount fingerprints, masked aggregates, card windows
│   │   └── build_features.py          # End-to-end IEEE pipeline orchestrator
│   └── validation/
│       ├── __init__.py                # Validation exports
│       ├── schema_check.py            # Input/output contract schema validator
│       ├── feature_quality.py         # Null audits, duplicate checks, row-grain assertions
│       └── leakage_check.py           # Target leakage and Tier B classification audit
├── tests/
│   ├── __init__.py                    # Test package
│   ├── test_spark_session.py          # SparkSession creation and config tests
│   ├── test_paysim_features.py        # PaySim synthetic tests, windows, non-leakage proofs
│   ├── test_ieee_features.py          # IEEE synthetic tests, missingness, card proxy windows
│   └── test_validation.py             # Schema, quality, and leakage detection tests
└── output/
    └── .gitkeep                       # Destination directory for generated artifacts
```

---

## 3. Prerequisites & Environment Setup

- **Java:** JDK 17 or JDK 21 (Confirmed on current system: `Java 21.0.8`).
- **Python:** Python ≥ 3.10 (Confirmed: `Python 3.12.8`).
- **Required Libraries:** `pyspark`, `pyyaml`, `pytest`, `pyarrow`.

Install dependencies:
```powershell
pip install pyspark pyyaml pytest pyarrow
```

---

## 4. Configuration (`config/feature_config.yaml`)

Paths and hyperparameters are decoupled from source code:
```yaml
feature_version: "v1.0"

spark:
  app_name: "FinancialFraud-Member2"
  master: "local[*]"
  driver_memory: "4g"
  shuffle_partitions: 16

paysim:
  input_path: "data/processed/paysim_clean.parquet"
  output_path: "data/features/paysim_features.parquet"
  include_tier_b: true
  split_cuts:
    train_max_step: 500
    valid_max_step: 600
  windows:
    short_hours: 24
    long_hours: 168

ieee:
  input_path: "data/processed/ieee_clean.parquet"
  output_path: "data/features/ieee_features.parquet"
  split_cuts:
    train_max_day: 130.0
  windows:
    short_secs: 3600
    mid_secs: 86400
    long_secs: 604800
```

---

## 5. PaySim Feature Pipeline

Grounded in the Member 1 audit findings:
1. **Origin Accounts:** 99.85% single-use in PaySim. Sender history is intentionally omitted.
2. **Destination History:** Customer destinations receive up to 113 transactions. Strictly historical windowing (`rangeBetween(unboundedPreceding, -1)` on `step`) derives `dest_prior_txn_count`, `dest_first_seen_flag`, cumulative amounts, and dormancy.
3. **Velocity Windows:** 24h and 168h rolling counts and sums compute recipient burst patterns.
4. **Tier Separation:**
   - **Tier A (Safe):** Amount log, ratios, pre-balances, cyclical hours, destination history.
   - **Tier B (Artifact):** Post-transaction balances and error deltas (`newbalanceOrig + amount - oldbalanceOrg`). Documented as simulator artifacts.
5. **Deterministic Key:** Mints `txn_id` via SHA-256 over `(step, type, amount, nameOrig, nameDest)`.

---

## 6. IEEE-CIS Feature Pipeline

1. **Structured Missingness:** Retains all rows without `dropna`. Computes `has_identity_flag`, `identity_nonnull_count`, and mask group presence indicators (`ind_addr`, `ind_dist1`, `ind_D*`, `ind_V*`).
2. **Card Proxy Entity:** Lacks customer IDs; derives `card_proxy_id` deterministically from `card1`–`card6` and `card_addr_proxy_id` with `addr1`.
3. **Relative Temporal:** Computes `hour_of_day`, `hour_sin/cos`, `day_of_week_rel` from `TransactionDT`.
4. **Masked Summaries:** Computes row-wise aggregates across M, C, and D families.
5. **Card History & Velocity:** Strictly prior windows over `(TransactionDT, TransactionID)` derive `card_prior_txn_count`, `card_first_seen_flag`, mean spend, z-scores, and 1h, 24h, 7d velocity bursts.
6. **Pruning:** Removes the 14 low-information near-constants identified in Audit Part 4.2 (`V1`, `V14`, etc.).

---

## 7. Validation Suite

The validation module ensures data integrity:
- **`schema_check.py`:** Validates input fields from Member 1 and output contract columns for Member 4.
- **`feature_quality.py`:** Verifies row-grain integrity (`count == distinct_ids`), detects NaN/Inf, checks nulls in critical columns.
- **`leakage_check.py`:** Prohibits target label as input, blocks target-derived features (`fraud_rate`), and classifies features into `SAFE`, `REVIEW`, and `LEAKAGE_RISK`.

---

## 8. Unit Testing

All unit tests use self-contained, in-memory synthetic DataFrames (5–10 rows) and do not require downloading external datasets.

Run all tests:
```powershell
pytest member2_spark/tests/ -v
```

Verified test coverage:
- SparkSession creation and configuration.
- PaySim temporal features, cyclical encodings, and deterministic surrogate key generation.
- PaySim amount features, simulator drain detection, and Tier B separation.
- PaySim destination historical windows and strict non-leakage (zero-count first appearance, no self-inclusion, no future bleeding, determinism under shuffled row order).
- IEEE structured missingness retention (zero dropped rows), identity coverage metrics, and block indicators.
- IEEE card proxy generation and missing component handling.
- IEEE relative diurnal hours and amount currency decimal fingerprints.
- IEEE card historical aggregates and strictly past window ordering.
- End-to-end pipeline execution and row-grain preservation.
- Schema validation, feature quality checks, and leakage auditing.

---

## 9. Downstream Handoff to Member 4 and Member 3

- **Member 4 (ML):** Consume features directly from the Parquet paths using the `split` column (`train`, `valid`, `test`). Model inputs should default to **Tier A features only**. Tier B features must be ablated separately to document simulator separability.
- **Member 3 (Kafka/Streaming):** Implement identical stateful window specifications keyed on `nameDest` (PaySim) and `card_proxy_id` (IEEE) with bounded state windows (24h, 168h).
- Full handoff specifications: see [`docs/OUTPUT_CONTRACT.md`](file:///c:/Users/hp/OneDrive/Desktop/big_data/member2_spark/docs/OUTPUT_CONTRACT.md).
