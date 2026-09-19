# Member 1 → Member 2 Audit & Handoff Report
**Project:** Real-Time Financial Fraud Intelligence Platform (B.Tech Big Data)
**Audited artifact:** `fraud_detecton_memeber_1_task_1.zip` (292.6 MB)  |  **Audit date:** Saturday 19 September 2026
**Prepared for:** Member 2 (Big Data Engineering / PySpark Feature Engineering)

> **How this audit was done.** Every number below was computed directly from the Parquet files (full data, not samples, unless explicitly labelled "sample") using PyArrow 25.0.1 and DuckDB 1.5.5. PySpark was *not* installed in the audit environment, so **no Spark code was executed**; Parts 8–9 are a design, not tested code. Nothing in this report is taken from `data_documentation.md` without being re-checked against the data, except where marked **NOT VERIFIABLE**. Facts about the *meaning* of masked IEEE-CIS columns come from the public competition description (Vesta), **not** from the ZIP, and are labelled as such.
> The re-runnable audit scripts and machine-readable schemas that back this report ship alongside it (see "Companion files" at the end of Part 10).

---

## 0. Verdict at a glance

| Question | Answer |
|---|---|
| Do the data files match Member 1's numeric claims? | **Yes.** Every claim that can be checked from the files passes (row counts, nulls, duplicates, negatives, `ProductCD` values, identity coverage, column count). |
| Is the handoff complete as an *engineering* deliverable? | **No.** The ZIP has 2 Parquet datasets + one 27-line markdown file. There is **no code, no schema/DDL, no Hive artifacts, no raw data, no validation script, no environment file**. The documentation mentions `raw_data/`, `hive_warehouse/`, `docs/` folders and two Hive tables that are **not in the ZIP**. Member 1's cleaning cannot be reproduced or re-verified from what was delivered. |
| Can Member 2 start now? | **Yes** — the two Parquet datasets are clean, consistent across parts, and sufficient as inputs. But Member 2 must first absorb the ten findings below, several of which change what "feature engineering" should even mean for PaySim. |

### Ten findings that shape Member 2's design (evidence in the sections cited)

1. **PaySim origin accounts are essentially single-use.** 6,353,307 distinct `nameOrig` values across 6,362,620 rows: **99.85 % of origin accounts appear exactly once** (max 3 appearances). Per-customer history, velocity and "deviation from this customer's normal" features on the **sender** are **not computable in any meaningful way**. Behaviour exists only on the **destination** side (2,722,362 accounts, up to 113 transactions each). *(Part 2, 6, 7)*
2. **PaySim has no transaction ID and no time inside a step.** Only `step` (1 hour resolution). 290,449 TRANSFER/CASH_OUT rows share `(nameDest, step)` with another row. Windows must be defined on `step`, strictly-prior, and Member 2 must mint a deterministic `txn_id`. *(Part 6, 8, 9)*
3. **PaySim fraud is almost perfectly separable by balance arithmetic — an artifact of the simulator.** 8,018 of 8,213 fraud rows (97.6 %) have `amount == oldbalanceOrg > 0`; **0 of 6,354,407 legitimate rows** do. Expect ~perfect metrics. This must be documented as a simulation property and handled with leakage tiers and ablations, or the project will not survive an interview. *(Part 4, 7)*
4. **PaySim fraud count per step is ~constant regardless of traffic volume.** 341 low-volume steps (<100 txns each) contain **79.2 %** fraud; the last 48 steps are **4.72 %** fraud vs **0.056 %** in the first 48. Any *volume* feature (transactions per step/hour, global velocity) is **label leakage**, and a naive "train on early steps, test on late steps" split yields a validation set with **8–15× the training prevalence** (cut points at step 400–650; still 2.5× at step 300). *(Part 4, 7)*
5. **`isFlaggedFraud` fires on 16 rows out of 6.36 M** (all TRANSFER, all fraud). It is the simulator's own rule output, near-constant, and must not be a model feature. *(Part 4)*
6. **IEEE-CIS has no customer/account ID.** Behavioural features require a *proxy* entity (`card1`–`card6`, optionally `addr1`). A card-level key gives 14,893 groups covering 99.3 % of rows in groups of ≥2; adding `addr1` gives 43,018 groups (97.1 %). Finer "UID" tricks based on `D1` are unverified. *(Part 6)*
7. **IEEE-CIS sparsity is structured and informative, not random.** Columns fall into **70 groups with byte-identical null patterns**. Identity coverage is exactly **24.42 %** (144,233 rows), and fraud is **7.85 %** with identity vs **2.09 %** without. `V167–V278` and `V322–V339` are populated almost only where identity exists. **`dropna()` on all columns leaves 0 rows.** *(Part 4)*
8. **Near-constant ≠ useless.** `V108–V125` are 96–99.9 % one value, yet their minority rows show fraud rates of roughly **6.5–43 %** (up to 12× the 3.5 % base rate). A blanket "drop constants" step would discard real signal. *(Part 4)*
9. **Only the labelled IEEE training table is present** (590,540 rows, days 1–183, no test set). Member 2 must define a **time-based split** (75/25 at ≈ day 130 keeps prevalence stable at 3.51 % / 3.45 %). *(Part 7)*
10. **Physical row order is not a contract.** PaySim happens to be globally sorted by `step`; IEEE is four interleaved sorted runs (one per part file). Spark will not preserve either. All window features need an explicit, deterministic ordering with tie-breakers. *(Part 8)*

---

## 1. PART 1 — ZIP content inventory

The ZIP holds **46 entries**: 2 Parquet dataset folders, 1 markdown file, and macOS junk. The two datasets were written by **Apache Spark 4.0.4** (Parquet writer `parquet-mr 1.15.2`), 4 part files each (Snappy), timestamps 14 Sep 2026 23:36–23:40.


| Path | Type | Size | Purpose | Status |
|---|---|---:|---|---|
| `ieee_clean.parquet/` | Directory | - | Spark-written Parquet dataset folder ("table") | Complete (has _SUCCESS) |
| `ieee_clean.parquet/part-00000-63f3d4ec-878e-4c28-acf6-d4c6c484dbbb-c000.snappy.parquet` | Parquet (Snappy) part file | 24.7 MB | IEEE-CIS data partition; 168,151 rows; 1 row-group; schema identical across parts | Complete (footer readable, row count verified) |
| `ieee_clean.parquet/part-00001-63f3d4ec-878e-4c28-acf6-d4c6c484dbbb-c000.snappy.parquet` | Parquet (Snappy) part file | 24.7 MB | IEEE-CIS data partition; 168,659 rows; 1 row-group; schema identical across parts | Complete (footer readable, row count verified) |
| `ieee_clean.parquet/part-00002-63f3d4ec-878e-4c28-acf6-d4c6c484dbbb-c000.snappy.parquet` | Parquet (Snappy) part file | 24.5 MB | IEEE-CIS data partition; 168,171 rows; 1 row-group; schema identical across parts | Complete (footer readable, row count verified) |
| `ieee_clean.parquet/part-00003-63f3d4ec-878e-4c28-acf6-d4c6c484dbbb-c000.snappy.parquet` | Parquet (Snappy) part file | 12.7 MB | IEEE-CIS data partition; 85,559 rows; 1 row-group; schema identical across parts | Complete (footer readable, row count verified) |
| `ieee_clean.parquet/_SUCCESS` | Spark commit marker | 0 B | Empty marker: Spark job committed successfully | Complete |
| `ieee_clean.parquet/._SUCCESS.crc` | Hadoop checksum (.crc) | 8 B | Hadoop LocalFileSystem checksum sidecar for the part file / _SUCCESS | Complete - NOT project content; exclude from Git |
| `ieee_clean.parquet/.part-00000-63f3d4ec-878e-4c28-acf6-d4c6c484dbbb-c000.snappy.parquet.crc` | Hadoop checksum (.crc) | 192.7 KB | Hadoop LocalFileSystem checksum sidecar for the part file / _SUCCESS | Complete - NOT project content; exclude from Git |
| `ieee_clean.parquet/.part-00001-63f3d4ec-878e-4c28-acf6-d4c6c484dbbb-c000.snappy.parquet.crc` | Hadoop checksum (.crc) | 192.7 KB | Hadoop LocalFileSystem checksum sidecar for the part file / _SUCCESS | Complete - NOT project content; exclude from Git |
| `ieee_clean.parquet/.part-00002-63f3d4ec-878e-4c28-acf6-d4c6c484dbbb-c000.snappy.parquet.crc` | Hadoop checksum (.crc) | 191.2 KB | Hadoop LocalFileSystem checksum sidecar for the part file / _SUCCESS | Complete - NOT project content; exclude from Git |
| `ieee_clean.parquet/.part-00003-63f3d4ec-878e-4c28-acf6-d4c6c484dbbb-c000.snappy.parquet.crc` | Hadoop checksum (.crc) | 99.3 KB | Hadoop LocalFileSystem checksum sidecar for the part file / _SUCCESS | Complete - NOT project content; exclude from Git |
| `paysim_clean.parquet/` | Directory | - | Spark-written Parquet dataset folder ("table") | Complete (has _SUCCESS) |
| `paysim_clean.parquet/part-00000-f0748dcd-b62a-47e7-a3c6-3a7fc12ab2ac-c000.snappy.parquet` | Parquet (Snappy) part file | 72.6 MB | PaySim data partition; 1,738,330 rows; 1 row-group; schema identical across parts | Complete (footer readable, row count verified) |
| `paysim_clean.parquet/part-00001-f0748dcd-b62a-47e7-a3c6-3a7fc12ab2ac-c000.snappy.parquet` | Parquet (Snappy) part file | 72.9 MB | PaySim data partition; 1,727,556 rows; 1 row-group; schema identical across parts | Complete (footer readable, row count verified) |
| `paysim_clean.parquet/part-00002-f0748dcd-b62a-47e7-a3c6-3a7fc12ab2ac-c000.snappy.parquet` | Parquet (Snappy) part file | 72.8 MB | PaySim data partition; 1,727,358 rows; 1 row-group; schema identical across parts | Complete (footer readable, row count verified) |
| `paysim_clean.parquet/part-00003-f0748dcd-b62a-47e7-a3c6-3a7fc12ab2ac-c000.snappy.parquet` | Parquet (Snappy) part file | 49.7 MB | PaySim data partition; 1,169,376 rows; 1 row-group; schema identical across parts | Complete (footer readable, row count verified) |
| `paysim_clean.parquet/_SUCCESS` | Spark commit marker | 0 B | Empty marker: Spark job committed successfully | Complete |
| `paysim_clean.parquet/._SUCCESS.crc` | Hadoop checksum (.crc) | 8 B | Hadoop LocalFileSystem checksum sidecar for the part file / _SUCCESS | Complete - NOT project content; exclude from Git |
| `paysim_clean.parquet/.part-00000-f0748dcd-b62a-47e7-a3c6-3a7fc12ab2ac-c000.snappy.parquet.crc` | Hadoop checksum (.crc) | 567.2 KB | Hadoop LocalFileSystem checksum sidecar for the part file / _SUCCESS | Complete - NOT project content; exclude from Git |
| `paysim_clean.parquet/.part-00001-f0748dcd-b62a-47e7-a3c6-3a7fc12ab2ac-c000.snappy.parquet.crc` | Hadoop checksum (.crc) | 569.5 KB | Hadoop LocalFileSystem checksum sidecar for the part file / _SUCCESS | Complete - NOT project content; exclude from Git |
| `paysim_clean.parquet/.part-00002-f0748dcd-b62a-47e7-a3c6-3a7fc12ab2ac-c000.snappy.parquet.crc` | Hadoop checksum (.crc) | 569.1 KB | Hadoop LocalFileSystem checksum sidecar for the part file / _SUCCESS | Complete - NOT project content; exclude from Git |
| `paysim_clean.parquet/.part-00003-f0748dcd-b62a-47e7-a3c6-3a7fc12ab2ac-c000.snappy.parquet.crc` | Hadoop checksum (.crc) | 388.0 KB | Hadoop LocalFileSystem checksum sidecar for the part file / _SUCCESS | Complete - NOT project content; exclude from Git |
| `__MACOSX/** (23 entries)` | macOS AppleDouble resource forks | 5.4 KB | Junk created by macOS "Compress" - ._* files, no project content | JUNK - delete; never commit |
| `data_documentation.md` | Markdown documentation | 1.2 KB | Member 1's data-lake / cleaning documentation (27 lines) | Present but THIN (see Part 2.3) |

**Checked for and NOT found anywhere in the ZIP:**

| Category | Found? |
|---|---|
| Python scripts (`.py`) | **None** |
| Notebooks (`.ipynb`) | **None** |
| SQL / Hive DDL (`.sql`, `.hql`) | **None** |
| Documentation | **1** (`data_documentation.md`) |
| Configuration files (`.yml`, `.yaml`, `.json`, `.ini`, `.conf`, `.properties`) | **None** |
| Parquet files | **8 part files** in 2 dataset folders (PaySim ×4, IEEE ×4) |
| CSV files (incl. raw `paysim.csv`, `train_transaction.csv`, `train_identity.csv`) | **None** (the doc says raw data lives in `raw_data/`, which is not delivered) |
| Shell scripts (`.sh`) | **None** |
| Docker files | **None** |
| `requirements.txt` / `environment.yml` / `pyproject.toml` | **None** |
| Data schema files / data dictionary | **None** |
| Tests / validation scripts / logs | **None** |

**Completeness assessment.**
- **Data: complete.** Both Parquet folders have a `_SUCCESS` marker, readable footers, identical schemas across all 4 parts, and part-level row counts that sum exactly to the claimed totals (PaySim 1,738,330 + 1,727,556 + 1,727,358 + 1,169,376 = 6,362,620; IEEE 168,151 + 168,659 + 168,171 + 85,559 = 590,540). Totals: PaySim ≈ 268.0 MB, IEEE ≈ 86.5 MB of Parquet.
- **Code / reproducibility: absent.** Nothing tells Member 2 *how* the data was cleaned, which duplicate rule was used, or how the IEEE join was done (only the doc's one-line "left-joined ... on TransactionID").
- **Documentation: present but thin** (see Part 2.3 for a gap list).
- **Junk:** `__MACOSX/` (23 AppleDouble entries), 10 Hadoop `.crc` sidecars and 2 `_SUCCESS` markers are artifacts of how the ZIP was made, not project content.

---

## 2. PART 2 — Dataset verification (actual files vs. `data_documentation.md`)

Legend: **PASS** = verified in the data · **WARNING** = true but incomplete/misleading, or a caveat Member 2 must know · **FAIL** = claim not met or cannot be met from the delivered artifacts.

### 2.1 Claim-by-claim comparison

**A. PaySim (`paysim_clean.parquet`)**

| # | Claim in documentation | Actual result | Verdict |
|---|---|---|---|
| A1 | Rows = 6,362,620 | 6,362,620 (footers and `COUNT(*)` agree) | **PASS** |
| A2 | 0 nulls | 0 nulls in all 12 columns | **PASS** |
| A3 | 0 duplicates | 0 full-row duplicates; 0 duplicates on `(nameOrig, step, type, amount, nameDest)` | **PASS** |
| A4 | `amount` cast to double | `amount` is `double` | **PASS** |
| A5 | No negative amounts | Min `amount` = 0; no negative value in any of the 5 numeric money columns | **PASS** |
| A5b | (unstated) zero amounts | **16 rows with `amount = 0`**; all 16 are `CASH_OUT` **and** `isFraud = 1` | **WARNING** |
| A6 | `step_hour` = `step % 24` | 0 mismatches over 6.36 M rows; range 0–23; 24 distinct values | **PASS** |
| A6b | (unstated) meaning of hour 0 | `step % 24` maps step 24, 48, … to hour 0 and step 1 to hour 1. If `step 1` is the simulation's hour 0, the label is shifted by one. Cyclical encodings are unaffected but "hour 0" is not guaranteed to mean midnight. | **WARNING** |
| A7 | Hive table `fraud_detection.paysim_clean` exists | No Hive warehouse, DDL or metastore export in ZIP | **FAIL** *(not verifiable)* |
| A8 | Folder structure `raw_data/ processed_data/ hive_warehouse/ docs/` | ZIP contains only the two Parquet folders and the `.md` at the root; the four folders are not present | **FAIL** *(not delivered)* |
| A9 | (unstated) schema hygiene | `isFlaggedFraud` is `NOT NULL` while every other column is nullable → nullability is inconsistent (harmless, but shows the schema was not explicitly defined) | **WARNING** |

**B. IEEE-CIS (`ieee_clean.parquet`)**

| # | Claim in documentation | Actual result | Verdict |
|---|---|---|---|
| B1 | Transaction rows = 590,540 | 590,540 | **PASS** |
| B2 | Joined row count = 590,540 | 590,540; `TransactionID` has 590,540 distinct values (min 2,987,000, max 3,577,539) | **PASS** |
| B3 | Columns = 434 | 434 (394 transaction-side incl. `TransactionID`/`isFraud` + 40 identity-side: `id_01–id_38`, `DeviceType`, `DeviceInfo`) | **PASS** |
| B4 | Identity rows = 144,233 | Exactly **144,233** rows have ≥1 non-null identity field (24.42 %). *Caveat:* the raw identity table is not delivered, so an all-null identity row could not be detected. | **PASS** |
| B5 | Left join keeps all transactions | All 590,540 transaction rows present; consistent with a left join | **PASS** |
| B6 | 0 nulls in `TransactionID`, `isFraud`, `TransactionAmt`, `ProductCD`, `card1` | 0 nulls in each of the five | **PASS** |
| B7 | 0 duplicates | `TransactionID` unique. **But:** 3 rows are identical to another row in **all 433 other columns** (only `TransactionID` differs); 479 rows are identical apart from `TransactionID` **and** `TransactionDT`. These are plausible repeat purchases, not join duplicates. | **PASS** + **WARNING** |
| B8 | No negative amounts | Min `TransactionAmt` = 0.251, max 31,937.391; 0 values ≤ 0 | **PASS** |
| B9 | `ProductCD` ∈ {C, W, S, R, H} | Exactly those 5 values: W 439,670 · C 68,519 · R 37,699 · H 33,024 · S 11,628 | **PASS** |
| B10 | D/M/V columns are sparse "by design", not a data-quality issue | Sparsity is real (see Part 4) — but it is **structured** (70 identical-null-mask groups), **identity-coupled**, and **predictive of fraud**. "Not a quality issue" is right; "ignore it" would be wrong. | **PASS** + **WARNING** |
| B11 | Hive table `fraud_detection.ieee_clean` exists | Not deliverable/verifiable | **FAIL** *(not verifiable)* |
| B12 | (unstated) test set | Only the labelled train table is present (183 days). No unlabeled test data. | **WARNING** |

### 2.2 PaySim — measured profile

**Shape:** 6,362,620 rows × 12 columns · 4 Parquet parts · Spark 4.0.4.

**Target:** `isFraud` — **8,213 fraud (0.1291 %)**, 6,354,407 legitimate. Imbalance ≈ 1 : 774. `isFlaggedFraud` — 16 ones (all fraud).

**Identifiers:** `nameOrig` (6,353,307 distinct; 'C' prefix + digits, length 5–11), `nameDest` (2,722,362 distinct; 'C' or 'M' prefix + digits, length 2–11; IDs are unpadded, e.g. `C2`, `C970`, so never sort them as numbers). **No transaction ID exists.** 1,769 accounts appear both as an origin and as a destination.

**Time columns:** `step` (int, 1–743, all 743 values present, ≈ hours), `step_hour` (derived, 0–23). No calendar timestamp.

**Numeric summary (all rows):**

| Column | Min | Max | Mean | Std | Median | p99 |
|---|---:|---:|---:|---:|---:|---:|
| `step` | 1 | 743 | 243.40 | 142.33 | 239 | 681 |
| `amount` | 0 | 92,445,516.64 | 179,861.90 | 603,858.23 | 74,871.94 | 1,615,979.47 |
| `oldbalanceOrg` | 0 | 59,585,040.37 | 833,883.10 | 2,888,242.67 | 14,208.00 | 16,027,256.13 |
| `newbalanceOrig` | 0 | 49,585,040.37 | 855,113.67 | 2,924,048.50 | 0 | 16,176,160.56 |
| `oldbalanceDest` | 0 | 356,015,889.35 | 1,100,701.67 | 3,399,180.11 | 132,705.67 | 12,371,819.15 |
| `newbalanceDest` | 0 | 356,179,278.92 | 1,224,996.40 | 3,674,128.94 | 214,661.44 | 13,137,866.94 |
| `step_hour` | 0 | 23 | 15.32 | 4.32 | 16 | 23 |

`amount` reaches exactly 10,000,000.00 in 3,207 rows (2,920 legitimate, 287 fraud) — a simulator cap.

**Categorical — `type` × fraud:**

| `type` | Rows | Fraud | Fraud % | `isFlaggedFraud`=1 |
|---|---:|---:|---:|---:|
| CASH_OUT | 2,237,500 | 4,116 | 0.184 | 0 |
| PAYMENT | 2,151,495 | 0 | 0 | 0 |
| CASH_IN | 1,399,284 | 0 | 0 | 0 |
| TRANSFER | 532,909 | 4,097 | 0.769 | 16 |
| DEBIT | 41,432 | 0 | 0 | 0 |

Fraud occurs **only** in TRANSFER and CASH_OUT (2,770,409 rows). Every PAYMENT goes C→M and every other type goes C→C, so `nameDest` starting with 'M' is **exactly equivalent** to `type = PAYMENT` (verified row-for-row: 2,151,495 rows are both, 4,211,125 are neither) — redundant information.

**Account-repeat structure (the key structural finding):**

| Times the account appears | `nameOrig` accounts | `nameOrig` accounts with ≥1 fraud |
|---:|---:|---:|
| 1 | 6,344,009 | 8,185 |
| 2 | 9,283 | 28 |
| 3 | 15 | 0 |

| `nameDest` (customer 'C' only) | Accounts | Transactions received |
|---|---:|---:|
| 1 txn | 113,396 | 113,396 |
| 2–3 | 133,250 | 323,788 |
| 4–10 | 195,029 | 1,233,983 |
| 11–100 | 130,280 | 2,539,327 |
| >100 (max 113) | 6 | 631 |

Merchants ('M'): 2,149,308 receive exactly 1 transaction; 1,093 receive 2–3. Merchant `oldbalanceDest`/`newbalanceDest` are **always 0**.

**Time structure:** transaction volume per simulated day is wildly uneven — day index 0: 574,255 txns · 1: 455,238 · **2: 1,070** · 3: 28,240 · … · 16: 425,766 · 17–30: 272 to 57,853 — while fraud stays at **~216–320 per day** regardless. The busiest step has 51,352 rows; the quietest has 2.

**Row order:** reading the four parts in file order, `step` is non-decreasing for all 6,362,620 rows (0 decreases): part 0 = steps 1–161, part 1 = 161–257, part 2 = 257–369, part 3 = 369–743. This is a property of the source ordering, **not** something Spark guarantees.

### 2.3 IEEE-CIS — measured profile

**Shape:** 590,540 rows × 434 columns · 4 Parquet parts · dtypes: 4 `int32` (`TransactionID`, `isFraud`, `TransactionDT`, `card1`), 399 `double`, 31 `string`.

**Target:** `isFraud` — **20,663 fraud (3.4990 %)**, 569,877 legitimate (≈ 1 : 27.6).

**Identifier:** `TransactionID` — unique (590,540), integer range 2,987,000–3,577,539 (not contiguous).

**Time:** `TransactionDT` — seconds from an **undisclosed** reference; 86,400 → 15,811,131 (day 1.0 → 183.0); 573,349 distinct → **33,932 rows share a timestamp** with another row. `TransactionID` and `TransactionDT` are perfectly monotone together (0 inversions when sorted by ID). Each of the 4 part files spans the *entire* ID/time range and is internally sorted (three inversions total at part boundaries) → four interleaved sorted runs.

**Core transaction columns:**

| Column | Null % | Distinct | Min | Max | Mean / Median | Note |
|---|---:|---:|---:|---:|---|---|
| `TransactionAmt` | 0 | 20,902 | 0.251 | 31,937.391 | 135.03 / 68.77 | AUC vs. fraud 0.498 (no monotone signal); heavy right tail |
| `card1` | 0 | 13,553 | 1,000 | 18,396 | – | integer code; 7,041 values appear <5 times |
| `card2` | 1.51 | 500 | 100 | 600 | – | stored as double |
| `card3` | 0.27 | 114 | 100 | 231 | – | stored as double |
| `card5` | 0.72 | 119 | 100 | 237 | – | stored as double |
| `addr1` / `addr2` | 11.13 / 11.13 | 332 / 74 | 100 / 10 | 540 / 102 | – | `addr2 = 87` for 99.2 % of non-null |
| `dist1` / `dist2` | 59.65 / 93.63 | 2,651 / 1,751 | 0 / 0 | 10,286 / 11,623 | – | |

**`ProductCD` × fraud:** W 439,670 rows (2.04 % fraud, mean amt 153.16) · **C 68,519 (11.69 %, mean 42.87)** · R 37,699 (3.78 %, 168.31) · H 33,024 (4.77 %, 73.17) · S 11,628 (5.90 %, 60.27).

**Other categorical cardinalities (non-null distinct):** `card4` 4 · `card6` 4 · `P_emaildomain` 59 · `R_emaildomain` 60 · `DeviceType` 2 · `DeviceInfo` 1,786 · `id_30` 75 · `id_31` 130 · `id_33` 260 · `M1–M9` 2–3 each · other `id_*` strings 2–4.

**Identity coverage:** 144,233 rows (24.42 %) have identity; fraud rate **7.847 %** with identity vs **2.094 %** without.

**Temporal behaviour (from `TransactionDT`, reference hour unknown):** relative hour-of-day fraud rate ranges from **2.37 %** (hour 14) to **10.11 %** (hour 9, only 2,414 rows); monthly fraud rate drifts between 2.71 % and 4.29 %.

**Amount fingerprint:** for `ProductCD = C`, 90.4 % of amounts have more than 2 decimal places; for all other products it is 0 % — a currency-conversion fingerprint usable as a feature.

### 2.4 Documentation gap list (what `data_documentation.md` does not say)

The 27-line document omits: column list and dtypes · fraud rate and class balance · the fact that PaySim has **no transaction ID/timestamp** and that `nameOrig` is nearly single-use · that `isFlaggedFraud` is a 16-row rule output · the 16 zero-amount rows · the fact that the IEEE test set is absent · what `TransactionDT` means · the duplicate-removal rule actually used · the join method details (left join stated, but not identity de-duplication) · partitioning/sort order of the Parquet output · Spark/Java/Python versions · row-order guarantees · where the raw data and Hive tables physically live · and any reference to the code that produced the outputs.



---

## 3. PART 3 — Complete schema

Both tables below list **every column** (12 for PaySim, 434 for IEEE-CIS). The same content is shipped as CSV (`paysim_schema.csv`, `ieee_schema.csv`) with extra columns (mean, median, std, top level, fraud rate when null / present, single-feature AUC, treatment reason, V-redundancy mapping).

Conventions: **Unique** = exact distinct count of non-null values (not an estimate). **Min/Max** are shown for numeric columns only; for string columns the dominant level is appended to the description. **Description** for PaySim comes from the standard PaySim definition; for IEEE-CIS it comes from the **public Vesta competition description, not from the ZIP** — most IEEE columns are masked and their true meaning is not documented, so treat descriptions as "as publicly described", not verified. Masked-column roles are inferred from data behaviour (dtype, cardinality, range) and marked as such.

### 3.1 PaySim (12 columns)


| Dataset | Column | Data Type | Null % | Unique (exact) | Min | Max | Description | Potential Feature Role |
|---|---|---|---:|---:|---:|---:|---|---|
| paysim | `step` | int32 | 0.00 | 743 | 1 | 743 | Simulation time unit: 1 step = 1 hour (range 1-743, ~31 days). No calendar timestamp. | TIME: ordering key + window axis; not an ML feature directly (drift proxy) |
| paysim | `type` | string | 0.00 | 5 | CASH_IN | TRANSFER | Transaction type: CASH_IN, CASH_OUT, DEBIT, PAYMENT, TRANSFER | CATEGORICAL (5 levels); fraud only in TRANSFER/CASH_OUT |
| paysim | `amount` | double | 0.00 | 5,316,900 | 0 | 92,445,516.64 | Transaction amount in local currency | NUMERIC: heavy right-skew -> log1p; ratios to balances |
| paysim | `nameOrig` | string | 0.00 | 6,353,307 | C1000000639 | C999999784 | Originating (sender) customer ID (always 'C' prefix) | IDENTIFIER (entity key). NOT a model feature. 99.85% of IDs appear once |
| paysim | `oldbalanceOrg` | double | 0.00 | 1,845,844 | 0 | 59,585,040.37 | Sender balance BEFORE the transaction | NUMERIC: pre-txn state, available at scoring time |
| paysim | `newbalanceOrig` | double | 0.00 | 2,682,586 | 0 | 49,585,040.37 | Sender balance AFTER the transaction | POST-TXN STATE: leakage-prone (see Part 7); prefer derived deltas with explicit caveat |
| paysim | `nameDest` | string | 0.00 | 2,722,362 | C1000004082 | M999999784 | Recipient ID ('C' = customer, 'M' = merchant) | IDENTIFIER (entity key); source of fan-in/first-seen features. NOT a raw feature |
| paysim | `oldbalanceDest` | double | 0.00 | 3,614,697 | 0 | 356,015,889.35 | Recipient balance BEFORE the transaction (0 for all merchants) | NUMERIC: pre-txn state; zeros are structural for merchants |
| paysim | `newbalanceDest` | double | 0.00 | 3,555,499 | 0 | 356,179,278.92 | Recipient balance AFTER the transaction (0 for all merchants) | POST-TXN STATE: leakage-prone |
| paysim | `isFraud` | int32 | 0.00 | 2 | 0 | 1 | TARGET: 1 = fraudulent transaction, 0 = legitimate | TARGET LABEL |
| paysim | `isFlaggedFraud` | int32 (NOT NULL) | 0.00 | 2 | 0 | 1 | Simulator's own rule-based flag; fires on only 16 rows (all TRANSFER, all fraud) | EXCLUDE from ML features (near-constant; rule-engine output). Keep only as rules-baseline reference |
| paysim | `step_hour` | int32 | 0.00 | 24 | 0 | 23 | Member 1 derived: step % 24 (hour-of-day proxy; 0-23) | TEMPORAL (cyclical). Semantics slightly ambiguous (see Part 2) |

### 3.2 IEEE-CIS — column-family map

| Family | Columns | # | Stored as | Null % range |
|---|---|---:|---|---|
| Identifier | `TransactionID` | 1 | int32 | 0 |
| Target | `isFraud` | 1 | int32 | 0 |
| Time | `TransactionDT` | 1 | int32 | 0 |
| Amount | `TransactionAmt` | 1 | double | 0 |
| Product | `ProductCD` | 1 | string | 0 |
| Card | `card1` (int32), `card2`, `card3`, `card5` (double), `card4`, `card6` (string) | 6 | mixed | 0 – 1.51 |
| Address / distance | `addr1`, `addr2`, `dist1`, `dist2` | 4 | double | 11.13 – 93.63 |
| Email | `P_emaildomain`, `R_emaildomain` | 2 | string | 15.99 / 76.75 |
| Counting | `C1`–`C14` | 14 | double | **0 (all)** |
| Timedelta | `D1`–`D15` | 15 | double | 0.21 – 93.41 |
| Match | `M1`–`M9` | 9 | string | 28.68 – 59.35 |
| Vesta engineered | `V1`–`V339` | 339 | double | 0.002 – 86.12 |
| Identity | `id_01`–`id_38` (12 of them are strings: `id_12`, `id_15`, `id_16`, `id_23`, `id_27`, `id_28`, `id_29`, `id_30`, `id_31`, `id_33`–`id_38`) | 38 | double / string | 75.58 – 99.20 |
| Device | `DeviceType`, `DeviceInfo` | 2 | string | 76.16 / 79.91 |
| **Total** | | **434** | | |

### 3.3 IEEE-CIS — all 434 columns

The last column is the default treatment from Part 4 (`1_KEEP_IMPUTE`, `2_KEEP_MISSING_INDICATOR`, `3_KEEP_CATEGORICAL`, `4_DROP_LOW_INFO`, `5_NEEDS_INVESTIGATION`, or a special role). `MGxx` in the role column is the identical-null-mask group (table in Part 4.4).


| Dataset | Column | Data Type | Null % | Unique (exact, non-null) | Min | Max | Description | Potential Feature Role | Treatment (Part 4) |
|---|---|---|---:|---:|---:|---:|---|---|---|
| ieee | `TransactionID` | int32 | 0.00 | 590,540 | 2,987,000 | 3,577,539 | Unique transaction key (join key to identity table) | identifier (MG00) | EXCLUDE_IDENTIFIER |
| ieee | `isFraud` | int32 | 0.00 | 2 | 0 | 1 | Target label (1 = fraud) | target (MG00) | TARGET |
| ieee | `TransactionDT` | int32 | 0.00 | 573,349 | 86,400 | 15,811,131 | Timedelta in seconds from an undisclosed reference datetime (not a real timestamp) | time (MG00) | DERIVE_TIME_FEATURES |
| ieee | `TransactionAmt` | double | 0.00 | 20,902 | 0.251 | 31,937.39 | Transaction payment amount (USD per competition description) | numeric-continuous; log1p; amount decimals (MG00) | 1_KEEP_IMPUTE |
| ieee | `ProductCD` | string | 0.00 | 5 |  |  | Product code of the transaction (C/H/R/S/W) [top level: W = 74.5% of non-null] | categorical-low-card (MG00) | 3_KEEP_CATEGORICAL |
| ieee | `card1` | int32 | 0.00 | 13,553 | 1,000 | 18,396 | Payment-card attribute (masked; competition lists as categorical) | categorical-high-card / entity proxy (MG00) | 3_KEEP_CATEGORICAL |
| ieee | `card2` | double | 1.51 | 500 | 100 | 600 | Payment-card attribute (masked; categorical) | categorical-high-card (MG08) | 3_KEEP_CATEGORICAL |
| ieee | `card3` | double | 0.27 | 114 | 100 | 231 | Payment-card attribute (masked; categorical) | categorical-mid-card (MG04) | 3_KEEP_CATEGORICAL |
| ieee | `card4` | string | 0.27 | 4 |  |  | Card network (visa / mastercard / american express / discover) [top level: visa = 65.3% of non-null] | categorical-low-card (MG06) | 3_KEEP_CATEGORICAL |
| ieee | `card5` | double | 0.72 | 119 | 100 | 237 | Payment-card attribute (masked; categorical) | categorical-mid-card (MG07) | 3_KEEP_CATEGORICAL |
| ieee | `card6` | string | 0.27 | 4 |  |  | Card type (debit / credit / charge card / debit or credit) [top level: debit = 74.7% of non-null] | categorical-low-card (MG05) | 3_KEEP_CATEGORICAL |
| ieee | `addr1` | double | 11.13 | 332 | 100 | 540 | Address attribute (masked, region-like; categorical) | categorical-mid-card (MG09) | 3_KEEP_CATEGORICAL |
| ieee | `addr2` | double | 11.13 | 74 | 10 | 102 | Address attribute (masked, country-like; categorical); 87 = 99.2% of non-null | categorical (dominant level) (MG09) | 3_KEEP_CATEGORICAL |
| ieee | `dist1` | double | 59.65 | 2,651 | 0 | 10,286 | Distance-type feature (masked) | numeric-continuous (MG28) | 2_KEEP_MISSING_INDICATOR |
| ieee | `dist2` | double | 93.63 | 1,751 | 0 | 11,623 | Distance-type feature (masked) | numeric-continuous (MG63) | 2_KEEP_MISSING_INDICATOR |
| ieee | `P_emaildomain` | string | 15.99 | 59 |  |  | Purchaser email domain [top level: gmail.com = 46.0% of non-null] | categorical-high-card (domain / provider-group / TLD splits) (MG15) | 3_KEEP_CATEGORICAL |
| ieee | `R_emaildomain` | string | 76.75 | 60 |  |  | Recipient email domain [top level: gmail.com = 41.6% of non-null] | categorical-high-card (MG41) | 3_KEEP_CATEGORICAL |
| ieee | `C1` | double | 0.00 | 1,657 | 0 | 4,685 | Counting feature (masked; Vesta: e.g. number of addresses linked to the card) | numeric-count; heavy tail -> log1p (MG00) | 1_KEEP_IMPUTE |
| ieee | `C2` | double | 0.00 | 1,216 | 0 | 5,691 | Counting feature (masked; Vesta: e.g. number of addresses linked to the card) | numeric-count; heavy tail -> log1p (MG00) | 1_KEEP_IMPUTE |
| ieee | `C3` | double | 0.00 | 27 | 0 | 26 | Counting feature (masked; Vesta: e.g. number of addresses linked to the card) | numeric-count; heavy tail -> log1p (MG00) | 1_KEEP_IMPUTE |
| ieee | `C4` | double | 0.00 | 1,260 | 0 | 2,253 | Counting feature (masked; Vesta: e.g. number of addresses linked to the card) | numeric-count; heavy tail -> log1p (MG00) | 1_KEEP_IMPUTE |
| ieee | `C5` | double | 0.00 | 319 | 0 | 349 | Counting feature (masked; Vesta: e.g. number of addresses linked to the card) | numeric-count; heavy tail -> log1p (MG00) | 1_KEEP_IMPUTE |
| ieee | `C6` | double | 0.00 | 1,328 | 0 | 2,253 | Counting feature (masked; Vesta: e.g. number of addresses linked to the card) | numeric-count; heavy tail -> log1p (MG00) | 1_KEEP_IMPUTE |
| ieee | `C7` | double | 0.00 | 1,103 | 0 | 2,255 | Counting feature (masked; Vesta: e.g. number of addresses linked to the card) | numeric-count; heavy tail -> log1p (MG00) | 1_KEEP_IMPUTE |
| ieee | `C8` | double | 0.00 | 1,253 | 0 | 3,331 | Counting feature (masked; Vesta: e.g. number of addresses linked to the card) | numeric-count; heavy tail -> log1p (MG00) | 1_KEEP_IMPUTE |
| ieee | `C9` | double | 0.00 | 205 | 0 | 210 | Counting feature (masked; Vesta: e.g. number of addresses linked to the card) | numeric-count; heavy tail -> log1p (MG00) | 1_KEEP_IMPUTE |
| ieee | `C10` | double | 0.00 | 1,231 | 0 | 3,257 | Counting feature (masked; Vesta: e.g. number of addresses linked to the card) | numeric-count; heavy tail -> log1p (MG00) | 1_KEEP_IMPUTE |
| ieee | `C11` | double | 0.00 | 1,476 | 0 | 3,188 | Counting feature (masked; Vesta: e.g. number of addresses linked to the card) | numeric-count; heavy tail -> log1p (MG00) | 1_KEEP_IMPUTE |
| ieee | `C12` | double | 0.00 | 1,199 | 0 | 3,188 | Counting feature (masked; Vesta: e.g. number of addresses linked to the card) | numeric-count; heavy tail -> log1p (MG00) | 1_KEEP_IMPUTE |
| ieee | `C13` | double | 0.00 | 1,597 | 0 | 2,918 | Counting feature (masked; Vesta: e.g. number of addresses linked to the card) | numeric-count; heavy tail -> log1p (MG00) | 1_KEEP_IMPUTE |
| ieee | `C14` | double | 0.00 | 1,108 | 0 | 1,429 | Counting feature (masked; Vesta: e.g. number of addresses linked to the card) | numeric-count; heavy tail -> log1p (MG00) | 1_KEEP_IMPUTE |
| ieee | `D1` | double | 0.21 | 641 | 0 | 640 | Timedelta-type feature (masked; Vesta: e.g. days since previous transaction) | numeric-timedelta (MG03) | 1_KEEP_IMPUTE |
| ieee | `D2` | double | 47.55 | 641 | 0 | 640 | Timedelta-type feature (masked; Vesta: e.g. days since previous transaction) | numeric-timedelta (MG22) | 2_KEEP_MISSING_INDICATOR |
| ieee | `D3` | double | 44.51 | 649 | 0 | 819 | Timedelta-type feature (masked; Vesta: e.g. days since previous transaction) | numeric-timedelta (MG19) | 1_KEEP_IMPUTE |
| ieee | `D4` | double | 28.60 | 808 | -122 | 869 | Timedelta-type feature (masked; Vesta: e.g. days since previous transaction) | numeric-timedelta (MG16) | 1_KEEP_IMPUTE |
| ieee | `D5` | double | 52.47 | 688 | 0 | 819 | Timedelta-type feature (masked; Vesta: e.g. days since previous transaction) | numeric-timedelta (MG24) | 1_KEEP_IMPUTE |
| ieee | `D6` | double | 87.61 | 829 | -83 | 873 | Timedelta-type feature (masked; Vesta: e.g. days since previous transaction) | numeric-timedelta (MG56) | 2_KEEP_MISSING_INDICATOR |
| ieee | `D7` | double | 93.41 | 597 | 0 | 843 | Timedelta-type feature (masked; Vesta: e.g. days since previous transaction) | numeric-timedelta (MG62) | 2_KEEP_MISSING_INDICATOR |
| ieee | `D8` | double | 87.31 | 12,353 | 0 | 1,707.79 | Timedelta-type feature (masked; Vesta: e.g. days since previous transaction) | numeric-timedelta (MG54) | 5_NEEDS_INVESTIGATION |
| ieee | `D9` | double | 87.31 | 24 | 0 | 0.9583 | Timedelta-type feature (masked; Vesta: e.g. days since previous transaction) | numeric-timedelta (MG54) | 5_NEEDS_INVESTIGATION |
| ieee | `D10` | double | 12.87 | 818 | 0 | 876 | Timedelta-type feature (masked; Vesta: e.g. days since previous transaction) | numeric-timedelta (MG10) | 1_KEEP_IMPUTE |
| ieee | `D11` | double | 47.29 | 676 | -53 | 670 | Timedelta-type feature (masked; Vesta: e.g. days since previous transaction) | numeric-timedelta (MG21) | 2_KEEP_MISSING_INDICATOR |
| ieee | `D12` | double | 89.04 | 635 | -83 | 648 | Timedelta-type feature (masked; Vesta: e.g. days since previous transaction) | numeric-timedelta (MG58) | 2_KEEP_MISSING_INDICATOR |
| ieee | `D13` | double | 89.51 | 577 | 0 | 847 | Timedelta-type feature (masked; Vesta: e.g. days since previous transaction) | numeric-timedelta (MG60) | 2_KEEP_MISSING_INDICATOR |
| ieee | `D14` | double | 89.47 | 802 | -193 | 878 | Timedelta-type feature (masked; Vesta: e.g. days since previous transaction) | numeric-timedelta (MG59) | 2_KEEP_MISSING_INDICATOR |
| ieee | `D15` | double | 15.09 | 859 | -83 | 879 | Timedelta-type feature (masked; Vesta: e.g. days since previous transaction) | numeric-timedelta (MG13) | 1_KEEP_IMPUTE |
| ieee | `M1` | string | 45.91 | 2 |  |  | Match flag (masked; Vesta: e.g. name/address match) [top level: T = 100.0% of non-null] | categorical-binary (T/F) or M0/M1/M2 (MG20) | 3_KEEP_CATEGORICAL |
| ieee | `M2` | string | 45.91 | 2 |  |  | Match flag (masked; Vesta: e.g. name/address match) [top level: T = 89.4% of non-null] | categorical-binary (T/F) or M0/M1/M2 (MG20) | 3_KEEP_CATEGORICAL |
| ieee | `M3` | string | 45.91 | 2 |  |  | Match flag (masked; Vesta: e.g. name/address match) [top level: T = 78.8% of non-null] | categorical-binary (T/F) or M0/M1/M2 (MG20) | 3_KEEP_CATEGORICAL |
| ieee | `M4` | string | 47.66 | 3 |  |  | Match flag (masked; Vesta: e.g. name/address match) [top level: M0 = 63.5% of non-null] | categorical-binary (T/F) or M0/M1/M2 (MG23) | 3_KEEP_CATEGORICAL |
| ieee | `M5` | string | 59.35 | 2 |  |  | Match flag (masked; Vesta: e.g. name/address match) [top level: F = 55.2% of non-null] | categorical-binary (T/F) or M0/M1/M2 (MG27) | 3_KEEP_CATEGORICAL |
| ieee | `M6` | string | 28.68 | 2 |  |  | Match flag (masked; Vesta: e.g. name/address match) [top level: F = 54.1% of non-null] | categorical-binary (T/F) or M0/M1/M2 (MG18) | 3_KEEP_CATEGORICAL |
| ieee | `M7` | string | 58.64 | 2 |  |  | Match flag (masked; Vesta: e.g. name/address match) [top level: F = 86.5% of non-null] | categorical-binary (T/F) or M0/M1/M2 (MG26) | 3_KEEP_CATEGORICAL |
| ieee | `M8` | string | 58.63 | 2 |  |  | Match flag (masked; Vesta: e.g. name/address match) [top level: F = 63.6% of non-null] | categorical-binary (T/F) or M0/M1/M2 (MG25) | 3_KEEP_CATEGORICAL |
| ieee | `M9` | string | 58.63 | 2 |  |  | Match flag (masked; Vesta: e.g. name/address match) [top level: T = 84.2% of non-null] | categorical-binary (T/F) or M0/M1/M2 (MG25) | 3_KEEP_CATEGORICAL |
| ieee | `V1` | double | 47.29 | 2 | 0 | 1 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG21) | 4_DROP_LOW_INFO |
| ieee | `V2` | double | 47.29 | 9 | 0 | 8 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG21) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V3` | double | 47.29 | 10 | 0 | 9 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG21) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V4` | double | 47.29 | 7 | 0 | 6 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG21) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V5` | double | 47.29 | 7 | 0 | 6 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG21) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V6` | double | 47.29 | 10 | 0 | 9 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG21) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V7` | double | 47.29 | 10 | 0 | 9 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG21) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V8` | double | 47.29 | 9 | 0 | 8 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG21) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V9` | double | 47.29 | 9 | 0 | 8 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG21) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V10` | double | 47.29 | 5 | 0 | 4 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG21) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V11` | double | 47.29 | 6 | 0 | 5 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG21) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V12` | double | 12.88 | 4 | 0 | 3 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG11) | 1_KEEP_IMPUTE |
| ieee | `V13` | double | 12.88 | 7 | 0 | 6 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG11) | 1_KEEP_IMPUTE |
| ieee | `V14` | double | 12.88 | 2 | 0 | 1 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG11) | 4_DROP_LOW_INFO |
| ieee | `V15` | double | 12.88 | 8 | 0 | 7 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG11) | 1_KEEP_IMPUTE |
| ieee | `V16` | double | 12.88 | 15 | 0 | 15 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG11) | 1_KEEP_IMPUTE |
| ieee | `V17` | double | 12.88 | 16 | 0 | 15 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG11) | 1_KEEP_IMPUTE |
| ieee | `V18` | double | 12.88 | 16 | 0 | 15 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG11) | 1_KEEP_IMPUTE |
| ieee | `V19` | double | 12.88 | 8 | 0 | 7 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG11) | 1_KEEP_IMPUTE |
| ieee | `V20` | double | 12.88 | 15 | 0 | 15 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG11) | 1_KEEP_IMPUTE |
| ieee | `V21` | double | 12.88 | 6 | 0 | 5 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG11) | 1_KEEP_IMPUTE |
| ieee | `V22` | double | 12.88 | 9 | 0 | 8 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG11) | 1_KEEP_IMPUTE |
| ieee | `V23` | double | 12.88 | 14 | 0 | 13 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG11) | 1_KEEP_IMPUTE |
| ieee | `V24` | double | 12.88 | 14 | 0 | 13 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG11) | 1_KEEP_IMPUTE |
| ieee | `V25` | double | 12.88 | 7 | 0 | 7 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG11) | 1_KEEP_IMPUTE |
| ieee | `V26` | double | 12.88 | 13 | 0 | 13 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG11) | 1_KEEP_IMPUTE |
| ieee | `V27` | double | 12.88 | 4 | 0 | 4 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG11) | 4_DROP_LOW_INFO |
| ieee | `V28` | double | 12.88 | 4 | 0 | 4 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG11) | 4_DROP_LOW_INFO |
| ieee | `V29` | double | 12.88 | 6 | 0 | 5 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG11) | 1_KEEP_IMPUTE |
| ieee | `V30` | double | 12.88 | 8 | 0 | 9 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG11) | 1_KEEP_IMPUTE |
| ieee | `V31` | double | 12.88 | 8 | 0 | 7 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG11) | 1_KEEP_IMPUTE |
| ieee | `V32` | double | 12.88 | 15 | 0 | 15 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG11) | 1_KEEP_IMPUTE |
| ieee | `V33` | double | 12.88 | 7 | 0 | 7 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG11) | 1_KEEP_IMPUTE |
| ieee | `V34` | double | 12.88 | 13 | 0 | 13 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG11) | 1_KEEP_IMPUTE |
| ieee | `V35` | double | 28.61 | 4 | 0 | 3 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG17) | 1_KEEP_IMPUTE |
| ieee | `V36` | double | 28.61 | 6 | 0 | 5 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG17) | 1_KEEP_IMPUTE |
| ieee | `V37` | double | 28.61 | 55 | 0 | 54 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG17) | 1_KEEP_IMPUTE |
| ieee | `V38` | double | 28.61 | 55 | 0 | 54 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG17) | 1_KEEP_IMPUTE |
| ieee | `V39` | double | 28.61 | 16 | 0 | 15 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG17) | 1_KEEP_IMPUTE |
| ieee | `V40` | double | 28.61 | 18 | 0 | 24 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG17) | 1_KEEP_IMPUTE |
| ieee | `V41` | double | 28.61 | 2 | 0 | 1 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG17) | 4_DROP_LOW_INFO |
| ieee | `V42` | double | 28.61 | 9 | 0 | 8 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG17) | 1_KEEP_IMPUTE |
| ieee | `V43` | double | 28.61 | 9 | 0 | 8 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG17) | 1_KEEP_IMPUTE |
| ieee | `V44` | double | 28.61 | 49 | 0 | 48 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG17) | 1_KEEP_IMPUTE |
| ieee | `V45` | double | 28.61 | 49 | 0 | 48 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG17) | 1_KEEP_IMPUTE |
| ieee | `V46` | double | 28.61 | 7 | 0 | 6 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG17) | 1_KEEP_IMPUTE |
| ieee | `V47` | double | 28.61 | 9 | 0 | 12 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG17) | 1_KEEP_IMPUTE |
| ieee | `V48` | double | 28.61 | 6 | 0 | 5 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG17) | 1_KEEP_IMPUTE |
| ieee | `V49` | double | 28.61 | 6 | 0 | 5 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG17) | 1_KEEP_IMPUTE |
| ieee | `V50` | double | 28.61 | 6 | 0 | 5 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG17) | 1_KEEP_IMPUTE |
| ieee | `V51` | double | 28.61 | 7 | 0 | 6 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG17) | 1_KEEP_IMPUTE |
| ieee | `V52` | double | 28.61 | 9 | 0 | 12 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG17) | 1_KEEP_IMPUTE |
| ieee | `V53` | double | 13.06 | 6 | 0 | 5 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG12) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V54` | double | 13.06 | 7 | 0 | 6 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG12) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V55` | double | 13.06 | 18 | 0 | 17 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG12) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V56` | double | 13.06 | 52 | 0 | 51 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG12) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V57` | double | 13.06 | 7 | 0 | 6 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG12) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V58` | double | 13.06 | 11 | 0 | 10 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG12) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V59` | double | 13.06 | 17 | 0 | 16 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG12) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V60` | double | 13.06 | 17 | 0 | 16 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG12) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V61` | double | 13.06 | 7 | 0 | 6 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG12) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V62` | double | 13.06 | 11 | 0 | 10 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG12) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V63` | double | 13.06 | 8 | 0 | 7 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG12) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V64` | double | 13.06 | 8 | 0 | 7 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG12) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V65` | double | 13.06 | 2 | 0 | 1 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG12) | 4_DROP_LOW_INFO |
| ieee | `V66` | double | 13.06 | 8 | 0 | 7 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG12) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V67` | double | 13.06 | 9 | 0 | 8 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG12) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V68` | double | 13.06 | 3 | 0 | 2 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG12) | 4_DROP_LOW_INFO |
| ieee | `V69` | double | 13.06 | 6 | 0 | 5 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG12) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V70` | double | 13.06 | 7 | 0 | 6 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG12) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V71` | double | 13.06 | 7 | 0 | 6 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG12) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V72` | double | 13.06 | 11 | 0 | 10 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG12) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V73` | double | 13.06 | 8 | 0 | 7 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG12) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V74` | double | 13.06 | 9 | 0 | 8 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG12) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V75` | double | 15.10 | 5 | 0 | 4 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG14) | 1_KEEP_IMPUTE |
| ieee | `V76` | double | 15.10 | 7 | 0 | 6 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG14) | 1_KEEP_IMPUTE |
| ieee | `V77` | double | 15.10 | 31 | 0 | 30 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG14) | 1_KEEP_IMPUTE |
| ieee | `V78` | double | 15.10 | 32 | 0 | 31 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG14) | 1_KEEP_IMPUTE |
| ieee | `V79` | double | 15.10 | 8 | 0 | 7 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG14) | 1_KEEP_IMPUTE |
| ieee | `V80` | double | 15.10 | 20 | 0 | 19 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG14) | 1_KEEP_IMPUTE |
| ieee | `V81` | double | 15.10 | 20 | 0 | 19 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG14) | 1_KEEP_IMPUTE |
| ieee | `V82` | double | 15.10 | 8 | 0 | 7 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG14) | 1_KEEP_IMPUTE |
| ieee | `V83` | double | 15.10 | 8 | 0 | 7 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG14) | 1_KEEP_IMPUTE |
| ieee | `V84` | double | 15.10 | 8 | 0 | 7 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG14) | 1_KEEP_IMPUTE |
| ieee | `V85` | double | 15.10 | 8 | 0 | 7 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG14) | 1_KEEP_IMPUTE |
| ieee | `V86` | double | 15.10 | 31 | 0 | 30 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG14) | 1_KEEP_IMPUTE |
| ieee | `V87` | double | 15.10 | 31 | 0 | 30 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG14) | 1_KEEP_IMPUTE |
| ieee | `V88` | double | 15.10 | 2 | 0 | 1 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG14) | 4_DROP_LOW_INFO |
| ieee | `V89` | double | 15.10 | 3 | 0 | 2 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG14) | 4_DROP_LOW_INFO |
| ieee | `V90` | double | 15.10 | 6 | 0 | 5 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG14) | 1_KEEP_IMPUTE |
| ieee | `V91` | double | 15.10 | 7 | 0 | 6 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG14) | 1_KEEP_IMPUTE |
| ieee | `V92` | double | 15.10 | 8 | 0 | 7 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG14) | 1_KEEP_IMPUTE |
| ieee | `V93` | double | 15.10 | 8 | 0 | 7 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG14) | 1_KEEP_IMPUTE |
| ieee | `V94` | double | 15.10 | 3 | 0 | 2 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG14) | 1_KEEP_IMPUTE |
| ieee | `V95` | double | 0.05 | 881 | 0 | 880 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG02) | 1_KEEP_IMPUTE |
| ieee | `V96` | double | 0.05 | 1,410 | 0 | 1,410 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG02) | 1_KEEP_IMPUTE |
| ieee | `V97` | double | 0.05 | 976 | 0 | 976 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG02) | 1_KEEP_IMPUTE |
| ieee | `V98` | double | 0.05 | 13 | 0 | 12 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG02) | 1_KEEP_IMPUTE |
| ieee | `V99` | double | 0.05 | 89 | 0 | 88 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG02) | 1_KEEP_IMPUTE |
| ieee | `V100` | double | 0.05 | 29 | 0 | 28 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG02) | 1_KEEP_IMPUTE |
| ieee | `V101` | double | 0.05 | 870 | 0 | 869 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG02) | 1_KEEP_IMPUTE |
| ieee | `V102` | double | 0.05 | 1,285 | 0 | 1,285 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG02) | 1_KEEP_IMPUTE |
| ieee | `V103` | double | 0.05 | 928 | 0 | 928 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG02) | 1_KEEP_IMPUTE |
| ieee | `V104` | double | 0.05 | 16 | 0 | 15 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG02) | 1_KEEP_IMPUTE |
| ieee | `V105` | double | 0.05 | 100 | 0 | 99 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG02) | 1_KEEP_IMPUTE |
| ieee | `V106` | double | 0.05 | 56 | 0 | 55 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG02) | 1_KEEP_IMPUTE |
| ieee | `V107` | double | 0.05 | 2 | 0 | 1 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG02) | 4_DROP_LOW_INFO |
| ieee | `V108` | double | 0.05 | 8 | 0 | 7 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG02) | 1_KEEP_IMPUTE |
| ieee | `V109` | double | 0.05 | 8 | 0 | 7 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG02) | 1_KEEP_IMPUTE |
| ieee | `V110` | double | 0.05 | 8 | 0 | 7 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG02) | 1_KEEP_IMPUTE |
| ieee | `V111` | double | 0.05 | 10 | 0 | 9 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG02) | 1_KEEP_IMPUTE |
| ieee | `V112` | double | 0.05 | 10 | 0 | 9 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG02) | 1_KEEP_IMPUTE |
| ieee | `V113` | double | 0.05 | 10 | 0 | 9 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG02) | 1_KEEP_IMPUTE |
| ieee | `V114` | double | 0.05 | 7 | 0 | 6 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG02) | 1_KEEP_IMPUTE |
| ieee | `V115` | double | 0.05 | 7 | 0 | 6 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG02) | 1_KEEP_IMPUTE |
| ieee | `V116` | double | 0.05 | 7 | 0 | 6 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG02) | 1_KEEP_IMPUTE |
| ieee | `V117` | double | 0.05 | 4 | 0 | 3 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG02) | 1_KEEP_IMPUTE |
| ieee | `V118` | double | 0.05 | 4 | 0 | 3 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG02) | 1_KEEP_IMPUTE |
| ieee | `V119` | double | 0.05 | 4 | 0 | 3 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG02) | 1_KEEP_IMPUTE |
| ieee | `V120` | double | 0.05 | 4 | 0 | 3 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG02) | 1_KEEP_IMPUTE |
| ieee | `V121` | double | 0.05 | 4 | 0 | 3 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG02) | 1_KEEP_IMPUTE |
| ieee | `V122` | double | 0.05 | 4 | 0 | 3 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG02) | 1_KEEP_IMPUTE |
| ieee | `V123` | double | 0.05 | 14 | 0 | 13 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG02) | 1_KEEP_IMPUTE |
| ieee | `V124` | double | 0.05 | 14 | 0 | 13 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG02) | 1_KEEP_IMPUTE |
| ieee | `V125` | double | 0.05 | 14 | 0 | 13 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG02) | 1_KEEP_IMPUTE |
| ieee | `V126` | double | 0.05 | 10,299 | 0 | 160,000 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG02) | 1_KEEP_IMPUTE |
| ieee | `V127` | double | 0.05 | 24,414 | 0 | 160,000 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG02) | 1_KEEP_IMPUTE |
| ieee | `V128` | double | 0.05 | 14,507 | 0 | 160,000 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG02) | 1_KEEP_IMPUTE |
| ieee | `V129` | double | 0.05 | 1,968 | 0 | 55,125 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG02) | 1_KEEP_IMPUTE |
| ieee | `V130` | double | 0.05 | 12,332 | 0 | 55,125 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG02) | 1_KEEP_IMPUTE |
| ieee | `V131` | double | 0.05 | 4,444 | 0 | 55,125 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG02) | 1_KEEP_IMPUTE |
| ieee | `V132` | double | 0.05 | 6,560 | 0 | 93,736 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG02) | 1_KEEP_IMPUTE |
| ieee | `V133` | double | 0.05 | 9,949 | 0 | 133,915 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG02) | 1_KEEP_IMPUTE |
| ieee | `V134` | double | 0.05 | 8,178 | 0 | 98,476 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG02) | 1_KEEP_IMPUTE |
| ieee | `V135` | double | 0.05 | 3,724 | 0 | 90,750 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG02) | 1_KEEP_IMPUTE |
| ieee | `V136` | double | 0.05 | 4,852 | 0 | 90,750 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG02) | 1_KEEP_IMPUTE |
| ieee | `V137` | double | 0.05 | 4,252 | 0 | 90,750 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG02) | 1_KEEP_IMPUTE |
| ieee | `V138` | double | 86.12 | 23 | 0 | 22 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG49) | 1_KEEP_IMPUTE |
| ieee | `V139` | double | 86.12 | 34 | 0 | 33 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG49) | 1_KEEP_IMPUTE |
| ieee | `V140` | double | 86.12 | 34 | 0 | 33 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG49) | 1_KEEP_IMPUTE |
| ieee | `V141` | double | 86.12 | 6 | 0 | 5 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG49) | 1_KEEP_IMPUTE |
| ieee | `V142` | double | 86.12 | 10 | 0 | 9 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG49) | 1_KEEP_IMPUTE |
| ieee | `V143` | double | 86.12 | 870 | 0 | 869 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG48) | 1_KEEP_IMPUTE |
| ieee | `V144` | double | 86.12 | 63 | 0 | 62 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG48) | 1_KEEP_IMPUTE |
| ieee | `V145` | double | 86.12 | 260 | 0 | 297 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG48) | 1_KEEP_IMPUTE |
| ieee | `V146` | double | 86.12 | 25 | 0 | 24 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG49) | 1_KEEP_IMPUTE |
| ieee | `V147` | double | 86.12 | 27 | 0 | 26 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG49) | 1_KEEP_IMPUTE |
| ieee | `V148` | double | 86.12 | 21 | 0 | 20 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG49) | 1_KEEP_IMPUTE |
| ieee | `V149` | double | 86.12 | 21 | 0 | 20 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG49) | 1_KEEP_IMPUTE |
| ieee | `V150` | double | 86.12 | 1,996 | 1 | 3,389 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG48) | 1_KEEP_IMPUTE |
| ieee | `V151` | double | 86.12 | 56 | 1 | 57 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG48) | 1_KEEP_IMPUTE |
| ieee | `V152` | double | 86.12 | 39 | 1 | 69 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG48) | 1_KEEP_IMPUTE |
| ieee | `V153` | double | 86.12 | 19 | 0 | 18 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG49) | 1_KEEP_IMPUTE |
| ieee | `V154` | double | 86.12 | 19 | 0 | 18 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG49) | 1_KEEP_IMPUTE |
| ieee | `V155` | double | 86.12 | 25 | 0 | 24 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG49) | 1_KEEP_IMPUTE |
| ieee | `V156` | double | 86.12 | 25 | 0 | 24 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG49) | 1_KEEP_IMPUTE |
| ieee | `V157` | double | 86.12 | 25 | 0 | 24 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG49) | 1_KEEP_IMPUTE |
| ieee | `V158` | double | 86.12 | 25 | 0 | 24 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG49) | 1_KEEP_IMPUTE |
| ieee | `V159` | double | 86.12 | 6,663 | 0 | 55,125 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG48) | 1_KEEP_IMPUTE |
| ieee | `V160` | double | 86.12 | 9,621 | 0 | 641,511.44 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG48) | 1_KEEP_IMPUTE |
| ieee | `V161` | double | 86.12 | 79 | 0 | 3,300 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG49) | 1_KEEP_IMPUTE |
| ieee | `V162` | double | 86.12 | 185 | 0 | 3,300 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG49) | 1_KEEP_IMPUTE |
| ieee | `V163` | double | 86.12 | 106 | 0 | 3,300 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG49) | 1_KEEP_IMPUTE |
| ieee | `V164` | double | 86.12 | 1,978 | 0 | 93,736 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG48) | 1_KEEP_IMPUTE |
| ieee | `V165` | double | 86.12 | 2,547 | 0 | 98,476 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG48) | 1_KEEP_IMPUTE |
| ieee | `V166` | double | 86.12 | 987 | 0 | 104,060 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG48) | 1_KEEP_IMPUTE |
| ieee | `V167` | double | 76.36 | 873 | 0 | 872 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG37) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V168` | double | 76.36 | 965 | 0 | 964 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG37) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V169` | double | 76.32 | 20 | 0 | 19 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG36) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V170` | double | 76.32 | 49 | 0 | 48 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG36) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V171` | double | 76.32 | 62 | 0 | 61 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG36) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V172` | double | 76.36 | 32 | 0 | 31 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG37) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V173` | double | 76.36 | 8 | 0 | 7 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG37) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V174` | double | 76.32 | 9 | 0 | 8 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG36) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V175` | double | 76.32 | 15 | 0 | 14 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG36) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V176` | double | 76.36 | 49 | 0 | 48 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG37) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V177` | double | 76.36 | 862 | 0 | 861 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG37) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V178` | double | 76.36 | 1,236 | 0 | 1,235 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG37) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V179` | double | 76.36 | 921 | 0 | 920 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG37) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V180` | double | 76.32 | 84 | 0 | 83 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG36) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V181` | double | 76.36 | 25 | 0 | 24 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG37) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V182` | double | 76.36 | 84 | 0 | 83 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG37) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V183` | double | 76.36 | 42 | 0 | 41 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG37) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V184` | double | 76.32 | 17 | 0 | 16 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG36) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V185` | double | 76.32 | 32 | 0 | 31 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG36) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V186` | double | 76.36 | 39 | 0 | 38 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG37) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V187` | double | 76.36 | 215 | 0 | 218 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG37) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V188` | double | 76.32 | 31 | 0 | 30 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG36) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V189` | double | 76.32 | 31 | 0 | 30 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG36) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V190` | double | 76.36 | 43 | 0 | 42 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG37) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V191` | double | 76.36 | 22 | 0 | 21 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG37) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V192` | double | 76.36 | 45 | 0 | 44 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG37) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V193` | double | 76.36 | 38 | 0 | 37 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG37) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V194` | double | 76.32 | 8 | 0 | 7 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG36) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V195` | double | 76.32 | 17 | 0 | 16 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG36) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V196` | double | 76.36 | 39 | 0 | 38 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG37) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V197` | double | 76.32 | 15 | 0 | 14 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG36) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V198` | double | 76.32 | 22 | 0 | 21 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG36) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V199` | double | 76.36 | 46 | 0 | 45 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG37) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V200` | double | 76.32 | 46 | 0 | 45 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG36) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V201` | double | 76.32 | 56 | 0 | 55 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG36) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V202` | double | 76.36 | 10,970 | 0 | 104,060 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG37) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V203` | double | 76.36 | 14,951 | 0 | 139,777 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG37) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V204` | double | 76.36 | 12,858 | 0 | 104,060 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG37) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V205` | double | 76.36 | 2,240 | 0 | 55,125 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG37) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V206` | double | 76.36 | 1,780 | 0 | 55,125 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG37) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V207` | double | 76.36 | 3,246 | 0 | 55,125 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG37) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V208` | double | 76.32 | 2,552 | 0 | 3,300 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG36) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V209` | double | 76.32 | 3,451 | 0 | 8,050 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG36) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V210` | double | 76.32 | 2,836 | 0 | 3,300 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG36) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V211` | double | 76.36 | 7,624 | 0 | 92,888 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG37) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V212` | double | 76.36 | 8,868 | 0 | 129,006 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG37) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V213` | double | 76.36 | 8,317 | 0 | 97,628 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG37) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V214` | double | 76.36 | 2,282 | 0 | 104,060 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG37) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V215` | double | 76.36 | 2,747 | 0 | 104,060 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG37) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V216` | double | 76.36 | 2,532 | 0 | 104,060 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG37) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V217` | double | 77.91 | 304 | 0 | 303 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG43) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V218` | double | 77.91 | 401 | 0 | 400 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG43) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V219` | double | 77.91 | 379 | 0 | 378 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG43) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V220` | double | 76.05 | 26 | 0 | 25 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG30) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V221` | double | 76.05 | 77 | 0 | 384 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG30) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V222` | double | 76.05 | 76 | 0 | 384 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG30) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V223` | double | 77.91 | 17 | 0 | 16 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG43) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V224` | double | 77.91 | 79 | 0 | 144 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG43) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V225` | double | 77.91 | 35 | 0 | 51 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG43) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V226` | double | 77.91 | 81 | 0 | 242 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG43) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V227` | double | 76.05 | 50 | 0 | 360 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG30) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V228` | double | 77.91 | 55 | 0 | 54 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG43) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V229` | double | 77.91 | 91 | 0 | 176 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG43) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V230` | double | 77.91 | 66 | 0 | 65 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG43) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V231` | double | 77.91 | 294 | 0 | 293 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG43) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V232` | double | 77.91 | 338 | 0 | 337 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG43) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V233` | double | 77.91 | 333 | 0 | 332 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG43) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V234` | double | 76.05 | 122 | 0 | 121 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG30) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V235` | double | 77.91 | 24 | 0 | 23 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG43) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V236` | double | 77.91 | 46 | 0 | 45 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG43) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V237` | double | 77.91 | 40 | 0 | 39 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG43) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V238` | double | 76.05 | 24 | 0 | 23 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG30) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V239` | double | 76.05 | 24 | 0 | 23 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG30) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V240` | double | 77.91 | 6 | 0 | 7 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG43) | 4_DROP_LOW_INFO |
| ieee | `V241` | double | 77.91 | 5 | 0 | 5 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG43) | 4_DROP_LOW_INFO |
| ieee | `V242` | double | 77.91 | 21 | 0 | 20 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG43) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V243` | double | 77.91 | 43 | 0 | 57 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG43) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V244` | double | 77.91 | 23 | 0 | 22 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG43) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V245` | double | 76.05 | 58 | 0 | 262 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG30) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V246` | double | 77.91 | 46 | 0 | 45 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG43) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V247` | double | 77.91 | 19 | 0 | 18 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG43) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V248` | double | 77.91 | 23 | 0 | 36 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG43) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V249` | double | 77.91 | 23 | 0 | 22 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG43) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V250` | double | 76.05 | 19 | 0 | 18 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG30) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V251` | double | 76.05 | 19 | 0 | 18 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG30) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V252` | double | 77.91 | 25 | 0 | 24 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG43) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V253` | double | 77.91 | 66 | 0 | 163 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG43) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V254` | double | 77.91 | 45 | 0 | 60 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG43) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V255` | double | 76.05 | 46 | 0 | 87 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG30) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V256` | double | 76.05 | 48 | 0 | 87 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG30) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V257` | double | 77.91 | 49 | 0 | 48 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG43) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V258` | double | 77.91 | 67 | 0 | 66 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG43) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V259` | double | 76.05 | 68 | 0 | 285 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG30) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V260` | double | 77.91 | 9 | 0 | 8 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG43) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V261` | double | 77.91 | 41 | 0 | 49 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG43) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V262` | double | 77.91 | 21 | 0 | 20 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG43) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V263` | double | 77.91 | 10,422 | 0 | 153,600 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG43) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V264` | double | 77.91 | 13,358 | 0 | 153,600 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG43) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V265` | double | 77.91 | 11,757 | 0 | 153,600 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG43) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V266` | double | 77.91 | 2,178 | 0 | 55,125 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG43) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V267` | double | 77.91 | 3,616 | 0 | 55,125 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG43) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V268` | double | 77.91 | 2,756 | 0 | 55,125 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG43) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V269` | double | 77.91 | 151 | 0 | 55,125 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG43) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V270` | double | 76.05 | 2,340 | 0 | 4,000 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG30) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V271` | double | 76.05 | 2,787 | 0 | 4,000 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG30) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V272` | double | 76.05 | 2,507 | 0 | 4,000 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG30) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V273` | double | 77.91 | 7,177 | 0 | 51,200 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG43) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V274` | double | 77.91 | 8,315 | 0 | 66,000 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG43) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V275` | double | 77.91 | 7,776 | 0 | 51,200 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG43) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V276` | double | 77.91 | 2,263 | 0 | 104,060 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG43) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V277` | double | 77.91 | 2,540 | 0 | 104,060 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG43) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V278` | double | 77.91 | 2,398 | 0 | 104,060 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG43) | 2_KEEP_MISSING_INDICATOR |
| ieee | `V279` | double | 0.00 | 881 | 0 | 880 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG01) | 1_KEEP_IMPUTE |
| ieee | `V280` | double | 0.00 | 975 | 0 | 975 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG01) | 1_KEEP_IMPUTE |
| ieee | `V281` | double | 0.21 | 23 | 0 | 22 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG03) | 1_KEEP_IMPUTE |
| ieee | `V282` | double | 0.21 | 33 | 0 | 32 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG03) | 1_KEEP_IMPUTE |
| ieee | `V283` | double | 0.21 | 62 | 0 | 68 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG03) | 1_KEEP_IMPUTE |
| ieee | `V284` | double | 0.00 | 13 | 0 | 12 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG01) | 1_KEEP_IMPUTE |
| ieee | `V285` | double | 0.00 | 96 | 0 | 95 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG01) | 1_KEEP_IMPUTE |
| ieee | `V286` | double | 0.00 | 9 | 0 | 8 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG01) | 1_KEEP_IMPUTE |
| ieee | `V287` | double | 0.00 | 32 | 0 | 31 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG01) | 1_KEEP_IMPUTE |
| ieee | `V288` | double | 0.21 | 11 | 0 | 10 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG03) | 1_KEEP_IMPUTE |
| ieee | `V289` | double | 0.21 | 13 | 0 | 12 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG03) | 1_KEEP_IMPUTE |
| ieee | `V290` | double | 0.00 | 58 | 1 | 67 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG01) | 1_KEEP_IMPUTE |
| ieee | `V291` | double | 0.00 | 219 | 1 | 1,055 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG01) | 1_KEEP_IMPUTE |
| ieee | `V292` | double | 0.00 | 173 | 1 | 323 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG01) | 1_KEEP_IMPUTE |
| ieee | `V293` | double | 0.00 | 870 | 0 | 869 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG01) | 1_KEEP_IMPUTE |
| ieee | `V294` | double | 0.00 | 1,286 | 0 | 1,286 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG01) | 1_KEEP_IMPUTE |
| ieee | `V295` | double | 0.00 | 928 | 0 | 928 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG01) | 1_KEEP_IMPUTE |
| ieee | `V296` | double | 0.21 | 94 | 0 | 93 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG03) | 1_KEEP_IMPUTE |
| ieee | `V297` | double | 0.00 | 13 | 0 | 12 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG01) | 1_KEEP_IMPUTE |
| ieee | `V298` | double | 0.00 | 94 | 0 | 93 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG01) | 1_KEEP_IMPUTE |
| ieee | `V299` | double | 0.00 | 50 | 0 | 49 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG01) | 1_KEEP_IMPUTE |
| ieee | `V300` | double | 0.21 | 12 | 0 | 11 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG03) | 1_KEEP_IMPUTE |
| ieee | `V301` | double | 0.21 | 14 | 0 | 13 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG03) | 1_KEEP_IMPUTE |
| ieee | `V302` | double | 0.00 | 17 | 0 | 16 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG01) | 1_KEEP_IMPUTE |
| ieee | `V303` | double | 0.00 | 21 | 0 | 20 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG01) | 1_KEEP_IMPUTE |
| ieee | `V304` | double | 0.00 | 17 | 0 | 16 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG01) | 1_KEEP_IMPUTE |
| ieee | `V305` | double | 0.00 | 2 | 1 | 2 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG01) | 4_DROP_LOW_INFO |
| ieee | `V306` | double | 0.00 | 16,210 | 0 | 108,800 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG01) | 1_KEEP_IMPUTE |
| ieee | `V307` | double | 0.00 | 37,367 | 0 | 145,765 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG01) | 1_KEEP_IMPUTE |
| ieee | `V308` | double | 0.00 | 23,064 | 0 | 108,800 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG01) | 1_KEEP_IMPUTE |
| ieee | `V309` | double | 0.00 | 4,236 | 0 | 55,125 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG01) | 1_KEEP_IMPUTE |
| ieee | `V310` | double | 0.00 | 19,136 | 0 | 55,125 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG01) | 1_KEEP_IMPUTE |
| ieee | `V311` | double | 0.00 | 3,098 | 0 | 55,125 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG01) | 1_KEEP_IMPUTE |
| ieee | `V312` | double | 0.00 | 8,068 | 0 | 55,125 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG01) | 1_KEEP_IMPUTE |
| ieee | `V313` | double | 0.21 | 5,529 | 0 | 4,817.47 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG03) | 1_KEEP_IMPUTE |
| ieee | `V314` | double | 0.21 | 11,377 | 0 | 7,519.87 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG03) | 1_KEEP_IMPUTE |
| ieee | `V315` | double | 0.21 | 6,973 | 0 | 4,817.47 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG03) | 1_KEEP_IMPUTE |
| ieee | `V316` | double | 0.00 | 9,814 | 0 | 93,736 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG01) | 1_KEEP_IMPUTE |
| ieee | `V317` | double | 0.00 | 15,184 | 0 | 134,021 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG01) | 1_KEEP_IMPUTE |
| ieee | `V318` | double | 0.00 | 12,309 | 0 | 98,476 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG01) | 1_KEEP_IMPUTE |
| ieee | `V319` | double | 0.00 | 4,799 | 0 | 104,060 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG01) | 1_KEEP_IMPUTE |
| ieee | `V320` | double | 0.00 | 6,439 | 0 | 104,060 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG01) | 1_KEEP_IMPUTE |
| ieee | `V321` | double | 0.00 | 5,560 | 0 | 104,060 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG01) | 1_KEEP_IMPUTE |
| ieee | `V322` | double | 86.05 | 881 | 0 | 880 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG47) | 1_KEEP_IMPUTE |
| ieee | `V323` | double | 86.05 | 1,411 | 0 | 1,411 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG47) | 1_KEEP_IMPUTE |
| ieee | `V324` | double | 86.05 | 976 | 0 | 976 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG47) | 1_KEEP_IMPUTE |
| ieee | `V325` | double | 86.05 | 13 | 0 | 12 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG47) | 1_KEEP_IMPUTE |
| ieee | `V326` | double | 86.05 | 45 | 0 | 44 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG47) | 1_KEEP_IMPUTE |
| ieee | `V327` | double | 86.05 | 19 | 0 | 18 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG47) | 1_KEEP_IMPUTE |
| ieee | `V328` | double | 86.05 | 16 | 0 | 15 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG47) | 1_KEEP_IMPUTE |
| ieee | `V329` | double | 86.05 | 100 | 0 | 99 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG47) | 1_KEEP_IMPUTE |
| ieee | `V330` | double | 86.05 | 56 | 0 | 55 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG47) | 1_KEEP_IMPUTE |
| ieee | `V331` | double | 86.05 | 1,758 | 0 | 160,000 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG47) | 1_KEEP_IMPUTE |
| ieee | `V332` | double | 86.05 | 2,453 | 0 | 160,000 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG47) | 1_KEEP_IMPUTE |
| ieee | `V333` | double | 86.05 | 1,971 | 0 | 160,000 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG47) | 1_KEEP_IMPUTE |
| ieee | `V334` | double | 86.05 | 143 | 0 | 55,125 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG47) | 1_KEEP_IMPUTE |
| ieee | `V335` | double | 86.05 | 672 | 0 | 55,125 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG47) | 1_KEEP_IMPUTE |
| ieee | `V336` | double | 86.05 | 356 | 0 | 55,125 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG47) | 1_KEEP_IMPUTE |
| ieee | `V337` | double | 86.05 | 254 | 0 | 104,060 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG47) | 1_KEEP_IMPUTE |
| ieee | `V338` | double | 86.05 | 380 | 0 | 104,060 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG47) | 1_KEEP_IMPUTE |
| ieee | `V339` | double | 86.05 | 334 | 0 | 104,060 | Vesta-engineered feature (masked: ranking/counting/entity relations) | numeric (count/flag-like) (MG47) | 1_KEEP_IMPUTE |
| ieee | `id_01` | double | 75.58 | 77 | -100 | 0 | Identity numeric score/delta (masked) | numeric (MG29) | 2_KEEP_MISSING_INDICATOR |
| ieee | `id_02` | double | 76.15 | 115,655 | 1 | 999,595 | Identity attribute (masked; competition lists id_12-id_38 as categorical) | categorical (numeric-coded) (MG33) | 5_NEEDS_INVESTIGATION |
| ieee | `id_03` | double | 88.77 | 24 | -13 | 10 | Identity numeric score/delta (masked) | numeric (MG57) | 2_KEEP_MISSING_INDICATOR |
| ieee | `id_04` | double | 88.77 | 15 | -28 | 0 | Identity numeric score/delta (masked) | numeric (MG57) | 2_KEEP_MISSING_INDICATOR |
| ieee | `id_05` | double | 76.82 | 93 | -72 | 52 | Identity numeric score/delta (masked) | numeric (MG42) | 2_KEEP_MISSING_INDICATOR |
| ieee | `id_06` | double | 76.82 | 101 | -100 | 0 | Identity numeric score/delta (masked) | numeric (MG42) | 2_KEEP_MISSING_INDICATOR |
| ieee | `id_07` | double | 99.13 | 84 | -46 | 61 | Identity numeric score/delta (masked) | numeric (MG67) | 5_NEEDS_INVESTIGATION |
| ieee | `id_08` | double | 99.13 | 94 | -100 | 0 | Identity numeric score/delta (masked) | numeric (MG67) | 5_NEEDS_INVESTIGATION |
| ieee | `id_09` | double | 87.31 | 46 | -36 | 25 | Identity numeric score/delta (masked) | numeric (MG54) | 2_KEEP_MISSING_INDICATOR |
| ieee | `id_10` | double | 87.31 | 62 | -100 | 0 | Identity numeric score/delta (masked) | numeric (MG54) | 2_KEEP_MISSING_INDICATOR |
| ieee | `id_11` | double | 76.13 | 365 | 90 | 100 | Identity numeric score/delta (masked) | numeric (MG32) | 2_KEEP_MISSING_INDICATOR |
| ieee | `id_12` | string | 75.58 | 2 |  |  | Identity/device-network flag (string level) [top level: NotFound = 85.3% of non-null] | categorical-low-card (MG29) | 3_KEEP_CATEGORICAL |
| ieee | `id_13` | double | 78.44 | 54 | 10 | 64 | Identity attribute (masked; competition lists id_12-id_38 as categorical) | categorical (numeric-coded) (MG45) | 3_KEEP_CATEGORICAL |
| ieee | `id_14` | double | 86.45 | 25 | -660 | 720 | Identity attribute (masked; competition lists id_12-id_38 as categorical) | categorical (numeric-coded) (MG50) | 3_KEEP_CATEGORICAL |
| ieee | `id_15` | string | 76.13 | 3 |  |  | Identity/device-network flag (string level) [top level: Found = 48.0% of non-null] | categorical-low-card (MG31) | 3_KEEP_CATEGORICAL |
| ieee | `id_16` | string | 78.10 | 2 |  |  | Identity/device-network flag (string level) [top level: Found = 51.3% of non-null] | categorical-low-card (MG44) | 3_KEEP_CATEGORICAL |
| ieee | `id_17` | double | 76.40 | 104 | 100 | 229 | Identity attribute (masked; competition lists id_12-id_38 as categorical) | categorical (numeric-coded) (MG38) | 3_KEEP_CATEGORICAL |
| ieee | `id_18` | double | 92.36 | 18 | 10 | 29 | Identity attribute (masked; competition lists id_12-id_38 as categorical) | categorical (numeric-coded) (MG61) | 3_KEEP_CATEGORICAL |
| ieee | `id_19` | double | 76.41 | 522 | 100 | 671 | Identity attribute (masked; competition lists id_12-id_38 as categorical) | categorical (numeric-coded) (MG39) | 3_KEEP_CATEGORICAL |
| ieee | `id_20` | double | 76.42 | 394 | 100 | 661 | Identity attribute (masked; competition lists id_12-id_38 as categorical) | categorical (numeric-coded) (MG40) | 3_KEEP_CATEGORICAL |
| ieee | `id_21` | double | 99.13 | 490 | 100 | 854 | Identity attribute (masked; competition lists id_12-id_38 as categorical) | categorical (numeric-coded) (MG66) | 3_KEEP_CATEGORICAL |
| ieee | `id_22` | double | 99.12 | 25 | 10 | 44 | Identity attribute (masked; competition lists id_12-id_38 as categorical) | categorical (numeric-coded) (MG64) | 5_NEEDS_INVESTIGATION |
| ieee | `id_23` | string | 99.12 | 3 |  |  | Identity/device-network flag (string level) [top level: IP_PROXY:TRANSPARENT = 67.5% of non-null] | categorical-low-card (MG64) | 3_KEEP_CATEGORICAL |
| ieee | `id_24` | double | 99.20 | 12 | 11 | 26 | Identity attribute (masked; competition lists id_12-id_38 as categorical) | categorical (numeric-coded) (MG69) | 3_KEEP_CATEGORICAL |
| ieee | `id_25` | double | 99.13 | 341 | 100 | 548 | Identity attribute (masked; competition lists id_12-id_38 as categorical) | categorical (numeric-coded) (MG68) | 3_KEEP_CATEGORICAL |
| ieee | `id_26` | double | 99.13 | 95 | 100 | 216 | Identity attribute (masked; competition lists id_12-id_38 as categorical) | categorical (numeric-coded) (MG65) | 5_NEEDS_INVESTIGATION |
| ieee | `id_27` | string | 99.12 | 2 |  |  | Identity/device-network flag (string level) [top level: Found = 99.7% of non-null] | categorical-low-card (MG64) | 4_DROP_LOW_INFO |
| ieee | `id_28` | string | 76.13 | 2 |  |  | Identity/device-network flag (string level) [top level: Found = 54.1% of non-null] | categorical-low-card (MG32) | 3_KEEP_CATEGORICAL |
| ieee | `id_29` | string | 76.13 | 2 |  |  | Identity/device-network flag (string level) [top level: Found = 53.1% of non-null] | categorical-low-card (MG32) | 3_KEEP_CATEGORICAL |
| ieee | `id_30` | string | 86.87 | 75 |  |  | Identity string: OS / browser / screen resolution [top level: Windows 10 = 27.3% of non-null] | categorical-high-card (MG53) | 3_KEEP_CATEGORICAL |
| ieee | `id_31` | string | 76.25 | 130 |  |  | Identity string: OS / browser / screen resolution [top level: chrome 63.0 = 15.7% of non-null] | categorical-high-card (MG35) | 3_KEEP_CATEGORICAL |
| ieee | `id_32` | double | 86.86 | 4 | 0 | 32 | Identity attribute (masked; competition lists id_12-id_38 as categorical) | categorical (numeric-coded) (MG52) | 3_KEEP_CATEGORICAL |
| ieee | `id_33` | string | 87.59 | 260 |  |  | Identity string: OS / browser / screen resolution [top level: 1920x1080 = 23.0% of non-null] | categorical-high-card (MG55) | 3_KEEP_CATEGORICAL |
| ieee | `id_34` | string | 86.82 | 4 |  |  | Identity/device-network flag (string level) [top level: match_status:2 = 77.1% of non-null] | categorical-low-card (MG51) | 3_KEEP_CATEGORICAL |
| ieee | `id_35` | string | 76.13 | 2 |  |  | Identity/device-network flag (string level) [top level: T = 55.2% of non-null] | categorical-low-card (MG31) | 3_KEEP_CATEGORICAL |
| ieee | `id_36` | string | 76.13 | 2 |  |  | Identity/device-network flag (string level) [top level: F = 95.1% of non-null] | categorical-low-card (MG31) | 3_KEEP_CATEGORICAL |
| ieee | `id_37` | string | 76.13 | 2 |  |  | Identity/device-network flag (string level) [top level: T = 78.3% of non-null] | categorical-low-card (MG31) | 3_KEEP_CATEGORICAL |
| ieee | `id_38` | string | 76.13 | 2 |  |  | Identity/device-network flag (string level) [top level: F = 52.4% of non-null] | categorical-low-card (MG31) | 3_KEEP_CATEGORICAL |
| ieee | `DeviceType` | string | 76.16 | 2 |  |  | Device type (desktop/mobile) [top level: desktop = 60.5% of non-null] | categorical-low-card (MG34) | 3_KEEP_CATEGORICAL |
| ieee | `DeviceInfo` | string | 79.91 | 1,786 |  |  | Device string (e.g. OS/build/model) [top level: Windows = 40.2% of non-null] | categorical-high-card (normalise: prefix/brand) (MG46) | 3_KEEP_CATEGORICAL |

---

## 4. PART 4 — Data quality assessment

### 4.1 PaySim — what Member 2 must know

| # | Finding | Evidence | Consequence for Member 2 |
|---|---|---|---|
| P1 | **`nameOrig` is single-use** | 99.85 % of origin accounts appear once; max 3; only 28 fraud origin accounts appear twice | No sender history/velocity/baseline features. Do not build them and pretend they work (they would be 0/NULL for 99.85 % of rows). |
| P2 | **Destination has real history** | 2,722,362 dest accounts (571,961 customers + 2,150,401 merchants); customer destinations receive up to 113 txns | Destination fan-in / novelty is the viable behavioural axis. **First-seen destination**: 5,524 of 8,213 frauds (67.3 %) go to a destination with **no earlier transaction**; fraud rate 1.019 % there vs 0.162 % / 0.109 % / 0.086 % for 1–2 / 3–10 / 11+ prior transactions (TRANSFER + CASH_OUT rows, strictly earlier `step`). |
| P3 | **Prior-fraud-on-destination is nearly empty** | Only **44 of 8,213** frauds hit a destination with an earlier fraud | "Fraud history" features add ~nothing here **and** carry leakage risk. Low priority (Part 7 shows how to do it safely if wanted). |
| P4 | **Balance arithmetic separates the classes** | Among TRANSFER/CASH_OUT: `newbalanceOrig = oldbalanceOrg − amount` holds for 99.45 % of fraud vs 9.49 % of legitimate rows; `amount == oldbalanceOrg` for 97.82 % of fraud vs 0.00 % of legitimate; **90.10 % of legitimate rows have `amount > oldbalanceOrg`** (physically impossible); destination `old = new = 0` for 49.63 % of fraud vs 0.06 % of legitimate | The simulator's legitimate rows violate accounting; its fraud rows are "drain the account" transfers. Model metrics will be near-perfect. Report this honestly; provide feature **tiers** and ablations (Part 6, 7, 9). |
| P5 | **Fraud volume is constant per step, not proportional to traffic** | ≈216–320 frauds per simulated day whether the day has 272 or 574,255 rows; steps with <100 txns: 79.2 % fraud; last simulated day (steps 721–743) has 272 rows, **all fraud** | Step/hour volume features and global velocity = label leakage. Random vs time splits behave very differently (Part 7). |
| P6 | **Time semantics are synthetic** | `step` only; `step_hour` diurnal traffic pattern is strong, but fraud is ~uniform across hours (274–372 per hour) | `step_hour` predicts fraud only because volume varies while fraud count doesn't. Treat as a weak/artifact feature; never claim "fraudsters act at night". |
| P7 | **`isFlaggedFraud` is a 16-row rule output** | 16 ones, all TRANSFER, all fraud; 409,110 TRANSFERs exceed 200,000 and only 16 are flagged | Exclude from model inputs. Keep as a "rules baseline" reference column (recall 16 / 8,213 = 0.19 %). |
| P8 | **16 zero-amount rows** | All CASH_OUT and all fraud | Keep; do not "clean". A flag is optional. |
| P9 | **`amount` cap at 10,000,000** | 3,207 rows exactly at the cap (287 fraud) | Simulator artifact; consider `amount_at_cap` only as a diagnostic, not a feature. |
| P10 | **Merchant destination balances are always 0** | `nameDest LIKE 'M%'` ⇒ `oldbalanceDest = newbalanceDest = 0` | Zeros in dest balance are structural for PAYMENT, not "missing". Dest-balance features are meaningful only for non-PAYMENT rows. |
| P11 | **No transaction ID, ties inside a step** | 290,449 TRANSFER/CASH_OUT rows share `(nameDest, step)` | Mint deterministic `txn_id`; use step-granular strictly-prior windows. |
| P12 | **`nameDest` 'M' ⇔ `type = PAYMENT`** | Verified row-for-row | Do not create a `dest_is_merchant` feature (duplicate of `type`). |

**Identifiers that must NOT be used directly as ML features:** `nameOrig`, `nameDest`, `txn_id`. **Categorical:** `type`. **Numerical:** `amount`, four balance columns. **Transform:** `amount` and balances are heavy-tailed (p99 ≫ median) → `log1p`; use ratios rather than raw magnitudes. **Leakage-prone raw columns:** `newbalanceOrig`, `newbalanceDest` (post-transaction state), `isFlaggedFraud`, anything derived from per-step volume.

### 4.2 IEEE-CIS — cross-cutting quality findings

**High-null columns.** 196 columns are 75–90 % null and 12 are >90 % null (`dist2` 93.63 %, `D7` 93.41 %, `id_18` 92.36 %, and `id_07`, `id_08`, `id_21`–`id_27` at ≈99.1–99.2 %). Only **20 columns are 100 % populated** (`TransactionID`, `isFraud`, `TransactionDT`, `TransactionAmt`, `ProductCD`, `card1`, `C1`–`C14`). **Zero rows have zero nulls** — `df.dropna()` returns an empty DataFrame, and even dropping only rows with a null in `card2`–`card6` removes 11,042 rows containing 590 frauds (5.3 % fraud rate, above the 3.5 % base).

**Constant columns.** None (no column has a single value). **Exact duplicate columns.** None (all 434 value vectors are distinct).

**Near-constant columns** (top value ≥ 99 % of non-null): `addr2`, `C3`, `M1`, `V1`, `V14`, `V27`, `V28`, `V41`, `V65`, `V68`, `V88`, `V89`, `V107`, `V108`, `V110`–`V114`, `V117`–`V122`, `V240`, `V241`, `V305`, `id_04` (99.1 % zeros; kept via its identity-block indicator), `id_27`. **But** the minority rows of many of these are highly predictive (table below) — so the decision is made per column, not by a blanket rule.

| Column | Null % | Majority value | Minority rows | Fraud % in minority | vs 3.5 % base | Decision |
|---|---:|---|---:|---:|---:|---|
| `V111` | 0.05 | 1 | 1,727 | 43.1 | 12.3× | keep |
| `V113` | 0.05 | 1 | 2,058 | 37.1 | 10.6× | keep |
| `V117` / `V119` | 0.05 | 1 | 727 / 776 | 27.9 / 26.7 | ≈8× | keep |
| `V112` / `V108` | 0.05 | 1 | 2,993 / 2,984 | 27.6 / 25.1 | 7.9× / 7.2× | keep |
| `V118` | 0.05 | 1 | 988 | 21.4 | 6.1× | keep |
| `V114` / `V110` | 0.05 | 1 | 5,538 / 4,665 | 18.5 / 17.6 | 5.3× / 5.0× | keep |
| `C3` | 0 | 0 | 2,429 non-zero | 0.21 | 0.06× | keep (non-zero rows almost never fraud) |
| `addr2` | 11.13 | 87 | 4,353 (non-87) | 10.2 | 2.9× | keep (categorical) |
| `M1` | 45.91 | T | 25 (`F`) | 0.00 | – | keep as categorical — its value is constant; its *missingness* (`MG20`) is the signal |
| `V1`, `V14`, `V27`, `V28`, `V41`, `V65`, `V68`, `V88`, `V89` | 13–47 | – | 17–422 | – | – | drop (tiny minority) |
| `V107`, `V305`, `V240`, `V241`, `id_27` | ≤0.05 or ≥77 | – | 4–248 | – | – | drop |

Full per-column numbers: `near_constant_minority.csv`.

**Duplicated information.**
- **Identical null masks:** 434 columns collapse into **70 mask groups** (Part 4.4). E.g. `D8`, `D9`, `id_09`, `id_10` are missing on *exactly the same rows* — even though `D8/D9` live in the transaction table and `id_09/id_10` in the identity table; all 74,926 non-null rows sit inside identity rows. Unexplained → *needs investigation*.
- **Identity coupling of V:** `V167–V216` are non-null in 139,631 rows, **all** of which have identity; `V217–V278` 130,430 non-null (130,269 with identity); `V322–V339` 82,351 non-null (82,041 with identity). So for these blocks "missing" ≈ "no identity record".
- **Value redundancy (sample-based, 25 % seeded sample):** within each null-mask block, greedy |r| ≥ 0.98 clustering retains **298 of 339** V columns (41 redundant); |r| ≥ 0.95 → 260; |r| ≥ 0.90 → 210. (`V104`/`V135` and `V286`/`V311` and `V297`/`V319` show identical minority counts — mirror columns.) Mapping in `v_redundancy_r098.csv`. This is **advisory only**; recompute on the training window before dropping.
- Card columns overlap heavily: `card1`–`card6` jointly define 14,893 combinations vs 13,553 values of `card1` alone.

**Suspicious leakage.** No single column looks like direct target leakage: the strongest single-feature AUC on non-null rows is 0.745 (`V258`; `D5` at 0.255 is 0.745 inverted). `TransactionID` and `TransactionDT` carry AUC 0.522 — a mild time drift, not a leak. **However**, `C*`, `D*`, `V*` were engineered by the data provider using transaction history and cannot be audited from the ZIP. Detect hidden temporal leakage empirically: compare validation scores under a random split vs a time split (a large gap ⇒ leakage or drift).

**Identifiers / columns that must not be used as raw model inputs:** `TransactionID` (key), `TransactionDT` (use only to derive hour/day and to order/split; raw value is a monotone drift proxy), `id_02` (behaves like a hashed identifier: 115,655 distinct in 140,872 non-null), `card1` as a *number* (it is a code), `DeviceInfo` as raw text (1,786 levels; 899 of them occur <5 times).

**Should be categorical although stored as double:** `card2`, `card3`, `card5`, `addr1`, `addr2`, and the numeric-coded `id_13`, `id_14`, `id_17`–`id_21`, `id_24`–`id_26`, `id_32` (the competition lists `card1–card6`, `addr1–addr2`, `M1–M9`, emails, `DeviceType/Info`, `id_12–id_38` as categorical). **Numerical:** `TransactionAmt`, `C1–C14`, `D1–D15`, `dist1–2`, `V*`, `id_01`, `id_03–id_11`.

**Transformations:** `TransactionAmt` → `log1p`; `C1–C14` (max up to 4,685) → `log1p`; `dist1/2` → `log1p`; `D*` contain a handful of **negative** values (`D4` 15 rows, `D15` 15, `D11` 7, `D6` 3, `D14` 3, `D12` 2 — anomalies for a "days since" quantity; do not silently clip, flag them); `D1` is 0 in 280,130 rows and exceeds the elapsed day count in 179,300 rows (it measures time from before the observation window — keep as is); `V*` are count/flag-like, no scaling needed for trees.

**Columns where missingness itself is informative.** Comparing fraud rate when null vs present, **202 of 323 partially-missing columns differ by ≥2 percentage points** and 301 by ≥1 pp. Largest: `D7` (present 14.88 % vs null 2.70 %), `addr1/addr2` (null 11.78 % vs present 2.46 %), `D12`–`D14`, `id_03/id_04`, `D6`, `D8/D9`, `id_09/id_10` (present ≈ 10.4–11.7 %), `dist2`, `R_emaildomain`, and the whole identity block (present 7.85–8.3 % vs null ≈ 2.1 %).

### 4.3 IEEE-CIS — sparse-column classification (no blanket `dropna`)

Decision rules (applied per column, evidence in `ieee_schema.csv`; thresholds are defaults for Member 4 to validate by ablation, not truths):

| Class | Rule | Columns |
|---|---|---:|
| **1. Keep + impute** | Numeric; null <1 % (negligible), **or** partially null with abs(Δ fraud rate) < 2 pp; **or** near-constant but minority rows are informative (≥ 500 rows and fraud lift ≥ 2× or ≤ 0.5×) | **207** |
| **2. Keep + missingness indicator** | Numeric; null ≥ 5 % and abs(Δ fraud rate, null vs present) ≥ 2 pp. Add **block-level** indicators (Part 6B), not one per column | **157** |
| **3. Keep as categorical** | Vesta-declared categorical or string dtype; encode NaN as an explicit `MISSING` level (which is itself the indicator) | **46** |
| **4. Drop — excessively sparse / low information** | Near-constant with <500 minority rows (no evidence of signal), or ≥ 99 % null with AUC within 0.05 of 0.5 | **14** |
| **5. Needs further investigation** | Anomalous semantics or 99 % null but non-trivial AUC | **7** |
| *Special roles* | `TransactionID` (exclude), `isFraud` (target), `TransactionDT` (derive time features) | 3 |

**Class members (compact):**
- **1. Keep + impute (207):** `TransactionAmt`; `C1–C14`; `D1, D3, D4, D5, D10, D15`; V ranges `V12–V13, V15–V26, V29–V40, V42–V52, V75–V87, V90–V106, V108–V166, V279–V304, V306–V339`. *Reason:* null <1 % or missingness carries <2 pp signal; `V108–V125`/`C3` are kept because their rare values are strongly (anti-)predictive.
- **2. Keep + missingness indicator (157):** `D2, D6, D7, D11, D12, D13, D14`; `dist1, dist2`; `id_01, id_03, id_04, id_05, id_06, id_09, id_10, id_11`; V ranges `V2–V11, V53–V64, V66–V67, V69–V74, V167–V239, V242–V278`. *Reason:* ≥ 5 % null with 2–12 pp fraud-rate gap; missingness marks "identity absent" / "history absent".
- **3. Keep as categorical (46):** `ProductCD`, `card1–card6`, `addr1`, `addr2`, `P_emaildomain`, `R_emaildomain`, `M1–M9`, `DeviceType`, `DeviceInfo`, `id_12–id_21`, `id_23–id_25`, `id_28–id_38`. *Reason:* codes/labels, not magnitudes. High-cardinality ones (`card1` 13,553; `card2` 500; `DeviceInfo` 1,786; `addr1` 332; emails 59/60; `id_31` 130; `id_33` 260; `id_30` 75) → frequency / train-only target encoding, never one-hot.
- **4. Drop (14):** `V1, V14, V27, V28, V41, V65, V68, V88, V89, V107, V240, V241, V305, id_27`. *Reason:* ≥ 99.9 % one value with only 4–422 minority rows (or 99.1 % null); their *missingness* (V1 → block V1–V11; V14/V27/V28 → block V12–V34; etc.) is preserved by the block indicators, so nothing informative is lost.
- **5. Needs investigation (7):** `D8`, `D9` (non-integer; `D9` in [0, 0.958] looks like a fraction; null mask identical to `id_09/id_10`), `id_02` (hash-like), `id_07`, `id_08`, `id_22`, `id_26` (≈ 99.1 % null, only ≈ 5,100 populated rows, yet AUC 0.40 / 0.44 / 0.56 / 0.60 on those rows). Default: keep the `_present` flag now; decide the values after Member 4's ablation.
- **Considered but *not* dropped despite being ≥ 99 % null:** `id_21`, `id_24`, `id_25` (AUC 0.53 / 0.47 / 0.53, within 0.05 of chance) are class **3** (categorical, cheap) — they could be dropped without loss; `id_23` (3 proxy levels, fraud 7.0–13.7 %) is kept.

**Optional dimensionality reduction (not required):** if width becomes a problem, apply the V-redundancy map (Part 4.2) fitted on the **training window only**; expected saving ≈ 41–129 V columns.

### 4.4 IEEE-CIS — identical-null-mask groups (structure of missingness)

Columns in the same group are missing on exactly the same rows. Fraud % is the fraud rate among rows where the group is null vs present. Use these groups to build **block-level** indicators/aggregates instead of hundreds of per-column flags.


| Mask group | Null % | # cols | Members | Fraud % if null | Fraud % if present |
|---|---:|---:|---|---:|---:|
| MG00 | 0.000 | 20 | TransactionID,isFraud,TransactionDT,TransactionAmt,ProductCD,card1,C1,C2,C3,C4,C5,C6,C7,C8,C9,C10,C11,C12,C13,C14 |  |  |
| MG01 | 0.002 | 32 | V279-V280,V284-V287,V290-V295,V297-V299,V302-V312,V316-V321 | 16.67 | 3.50 |
| MG02 | 0.053 | 43 | V95-V137 | 5.41 | 3.50 |
| MG03 | 0.215 | 12 | V281-V283,V288-V289,V296,V300-V301,V313-V315,D1 | 3.62 | 3.50 |
| MG04 | 0.265 | 1 | card3 | 2.49 | 3.50 |
| MG05 | 0.266 | 1 | card6 | 2.48 | 3.50 |
| MG06 | 0.267 | 1 | card4 | 2.60 | 3.50 |
| MG07 | 0.721 | 1 | card5 | 4.93 | 3.49 |
| MG08 | 1.513 | 1 | card2 | 4.74 | 3.48 |
| MG09 | 11.126 | 2 | addr1,addr2 | 11.78 | 2.46 |
| MG10 | 12.873 | 1 | D10 | 5.11 | 3.26 |
| MG11 | 12.882 | 23 | V12-V34 | 5.11 | 3.26 |
| MG12 | 13.055 | 22 | V53-V74 | 5.85 | 3.15 |
| MG13 | 15.090 | 1 | D15 | 4.80 | 3.27 |
| MG14 | 15.099 | 20 | V75-V94 | 4.80 | 3.27 |
| MG15 | 15.995 | 1 | P_emaildomain | 2.95 | 3.60 |
| MG16 | 28.605 | 1 | D4 | 3.61 | 3.46 |
| MG17 | 28.613 | 18 | V35-V52 | 3.61 | 3.46 |
| MG18 | 28.679 | 1 | M6 | 7.07 | 2.06 |
| MG19 | 44.515 | 1 | D3 | 4.20 | 2.94 |
| MG20 | 45.907 | 3 | M1,M2,M3 | 5.28 | 1.99 |
| MG21 | 47.293 | 12 | V1-V11,D11 | 5.21 | 1.96 |
| MG22 | 47.549 | 1 | D2 | 4.56 | 2.54 |
| MG23 | 47.659 | 1 | M4 | 1.86 | 4.99 |
| MG24 | 52.467 | 1 | D5 | 3.16 | 3.88 |
| MG25 | 58.633 | 2 | M8,M9 | 4.58 | 1.97 |
| MG26 | 58.635 | 1 | M7 | 4.58 | 1.97 |
| MG27 | 59.349 | 1 | M5 | 3.74 | 3.15 |
| MG28 | 59.652 | 1 | dist1 | 4.52 | 2.00 |
| MG29 | 75.576 | 2 | id_01,id_12 | 2.09 | 7.85 |
| MG30 | 76.053 | 16 | V220-V222,V227,V234,V238-V239,V245,V250-V251,V255-V256,V259,V270-V272 | 2.16 | 7.77 |
| MG31 | 76.126 | 5 | id_15,id_35,id_36,id_37,id_38 | 2.10 | 7.96 |
| MG32 | 76.127 | 3 | id_11,id_28,id_29 | 2.10 | 7.96 |
| MG33 | 76.145 | 1 | id_02 | 2.10 | 7.97 |
| MG34 | 76.156 | 1 | DeviceType | 2.10 | 7.96 |
| MG35 | 76.245 | 1 | id_31 | 2.11 | 7.96 |
| MG36 | 76.324 | 19 | V169-V171,V174-V175,V180,V184-V185,V188-V189,V194-V195,V197-V198,V200-V201,V208-V210 | 2.13 | 7.92 |
| MG37 | 76.355 | 31 | V167-V168,V172-V173,V176-V179,V181-V183,V186-V187,V190-V193,V196,V199,V202-V207,V211-V216 | 2.13 | 7.91 |
| MG38 | 76.400 | 1 | id_17 | 2.13 | 7.93 |
| MG39 | 76.408 | 1 | id_19 | 2.13 | 7.92 |
| MG40 | 76.418 | 1 | id_20 | 2.13 | 7.92 |
| MG41 | 76.752 | 1 | R_emaildomain | 2.08 | 8.18 |
| MG42 | 76.824 | 2 | id_05,id_06 | 2.13 | 8.02 |
| MG43 | 77.913 | 46 | V217-V219,V223-V226,V228-V233,V235-V237,V240-V244,V246-V249,V252-V254,V257-V258,V260-V269,V273-V278 | 2.28 | 7.81 |
| MG44 | 78.098 | 1 | id_16 | 2.28 | 7.85 |
| MG45 | 78.440 | 1 | id_13 | 2.17 | 8.32 |
| MG46 | 79.906 | 1 | DeviceInfo | 2.55 | 7.25 |
| MG47 | 86.055 | 18 | V322-V339 | 3.34 | 4.48 |
| MG48 | 86.123 | 11 | V143-V145,V150-V152,V159-V160,V164-V166 | 3.34 | 4.46 |
| MG49 | 86.124 | 18 | V138-V142,V146-V149,V153-V158,V161-V163 | 3.35 | 4.45 |
| MG50 | 86.446 | 1 | id_14 | 3.35 | 4.47 |
| MG51 | 86.825 | 1 | id_34 | 3.35 | 4.48 |
| MG52 | 86.862 | 1 | id_32 | 3.36 | 4.45 |
| MG53 | 86.865 | 1 | id_30 | 3.36 | 4.44 |
| MG54 | 87.312 | 4 | D8,D9,id_09,id_10 | 2.49 | 10.45 |
| MG55 | 87.589 | 1 | id_33 | 3.34 | 4.59 |
| MG56 | 87.607 | 1 | D6 | 2.50 | 10.55 |
| MG57 | 88.769 | 2 | id_03,id_04 | 2.59 | 10.72 |
| MG58 | 89.041 | 1 | D12 | 2.48 | 11.74 |
| MG59 | 89.469 | 1 | D14 | 2.55 | 11.60 |
| MG60 | 89.509 | 1 | D13 | 2.62 | 11.04 |
| MG61 | 92.361 | 1 | id_18 | 3.10 | 8.28 |
| MG62 | 93.410 | 1 | D7 | 2.70 | 14.88 |
| MG63 | 93.628 | 1 | dist2 | 3.06 | 9.92 |
| MG64 | 99.125 | 3 | id_22,id_23,id_27 | 3.46 | 8.24 |
| MG65 | 99.126 | 1 | id_26 | 3.46 | 8.23 |
| MG66 | 99.126 | 1 | id_21 | 3.46 | 8.26 |
| MG67 | 99.127 | 2 | id_07,id_08 | 3.46 | 8.26 |
| MG68 | 99.131 | 1 | id_25 | 3.46 | 8.13 |
| MG69 | 99.196 | 1 | id_24 | 3.46 | 8.47 |



---

## 5. PART 5 — Member 1 → Member 2 handoff

### 5.1 INPUTS AVAILABLE TO MEMBER 2

| # | Input | Location in ZIP | Usable as-is? | Notes |
|---|---|---|---|---|
| 1 | `paysim_clean.parquet` | 4 Snappy parts, 6,362,620 rows × 12 cols, 268 MB | **Yes** (read the folder, not a single part) | No key, no timestamp; `step_hour` pre-derived; schema = Part 3.1 |
| 2 | `ieee_clean.parquet` | 4 Snappy parts, 590,540 rows × 434 cols, 86.5 MB | **Yes** | Transaction ⟕ identity on `TransactionID`; labelled train data only |
| 3 | `data_documentation.md` | root | Partly | Numeric claims are correct; incomplete (Part 2.4) |
| 4 | Written **schemas** | – | **No such file** | Member 2 uses `paysim_schema.csv` / `ieee_schema.csv` from this audit |
| 5 | Hive tables `fraud_detection.paysim_clean`, `fraud_detection.ieee_clean` | not in ZIP | **Unverified** | Ask Member 1 for DDL + `SHOW CREATE TABLE`, HDFS/warehouse paths |
| 6 | Raw data (`raw_data/`) | not in ZIP | **Not available** | Needed only if a cleaning step must be re-checked |
| 7 | Cleaning code / validation script | not in ZIP | **Not available** | Ask Member 1 to commit it (Part 11) |

**Format facts Member 2 can rely on:** Spark 4.0.4 output, Parquet-mr 1.15.2, Snappy, 1 row group per part, schema identical across parts, string columns are UTF-8, missing values are true Parquet nulls. **Verified:** no float `NaN`/`Inf` in any of the 404 double columns; no empty-string or `nan`/`none`/`null`/`na`/`?`-style literals in any of the 34 string columns; no `-999`/`-9999`/`999999` sentinels in any IEEE numeric column. So `isNull()` is the only missing-value test Member 2 needs.

### 5.2 WHAT MEMBER 2 SHOULD NOT REDO

| Do not redo | Why it is already settled (evidence) |
|---|---|
| Duplicate removal (PaySim) | 0 full-row duplicates; 0 duplicates on `(nameOrig, step, type, amount, nameDest)` |
| Duplicate removal (IEEE) | `TransactionID` unique (590,540). The 3 rows identical in every other column are legitimate repeat purchases — do **not** dedupe them |
| Null cleaning (PaySim) | 0 nulls in all 12 columns |
| Null cleaning of IEEE key columns | 0 nulls in `TransactionID`, `isFraud`, `TransactionAmt`, `ProductCD`, `card1`; also `TransactionDT` and `C1–C14` |
| **Imputing IEEE sparsity at cleaning time / `dropna`** | Deliberately retained by Member 1; Member 2 handles it at *feature* level, and it must **not** be `dropna`'d (0 rows would remain) |
| `ProductCD` validation | Exactly {C, W, R, H, S} |
| Negative-amount check | None (PaySim min 0; IEEE min 0.251) |
| `amount` → double cast (PaySim) | Already `double` |
| Transaction ⟕ identity join | Done; 144,233 identity rows matched |
| `step_hour` derivation | Already `step % 24` (Member 2 may *encode* it cyclically, not recompute) |
| Partitioning/format conversion of inputs | Already columnar Parquet; do not re-export to CSV |

**What Member 2 *should* still verify (cheap, once, in the pipeline's schema-validation step):** row counts, column list/dtypes vs contract, `TransactionID` uniqueness, PaySim 5-tuple uniqueness, `isFraud ∈ {0,1}`, `type` ∈ 5 values. Then trust and move on.

### 5.3 WHAT MEMBER 2 MUST BUILD (derived only from the actual files)

1. **Contracts and validation.** Input schema contracts (JSON) for both datasets and a Spark validation job that fails fast on mismatch.
2. **PaySim surrogate key and ordering.** Deterministic `txn_id` (no ID exists) and a documented ordering rule (`step` + tie-break policy).
3. **Time-based split assignment** for both datasets (Part 7) and persisted as a `split` column.
4. **PaySim features:** amount/balance ratios and flags; cyclical hour; **destination-side** behavioural features (first-seen, prior counts, windowed counts, unique senders, amount z-score, time since last); optional origin-side flags; a separated **leakage-tiered** set for post-transaction balance features.
5. **IEEE-CIS features:** time features from `TransactionDT`; amount fingerprint features; block-level missingness indicators and identity completeness; V/C/D/M aggregates; email/device/browser/resolution parsing; frequency encodings (train-fit); **card-proxy entity** history and velocity features.
6. **Train-fit artifacts** (frequency maps, medians, per-`type`/`ProductCD` statistics) saved as small reference tables so Member 3 can apply the *same* logic in streaming.
7. **Feature validation job** (null/range/leakage screens, offline-vs-online parity spot check).
8. **Feature Parquet outputs + manifest** per Part 9, with column-level metadata (tier, online-computable, type).
9. **Docs + reproducibility:** feature specification, config file, run instructions (Part 10–11).

**Not Member 2's job** (do not drift into it): model training/threshold selection (Member 4), Kafka topics/stream job (Member 3), PostgreSQL/FastAPI/Power BI (Member 5), re-cleaning raw data (Member 1).



---

## 6. PART 6 — Recommended feature engineering

**Rules for this section.** Only columns that exist in the files are used as sources. Every derived feature name below is a *proposal* (it does not exist yet). "Computation type" values: **Row** = pure row-wise expression (no shuffle) · **Broadcast** = row-wise using a small train-fit lookup table · **Window** = per-entity window over strictly-earlier rows · **Agg-join** = groupBy aggregate joined back · **Train-fit** = statistic fitted on the training split only. **Tier A** = knowable at authorization time; **Tier B** = uses post-transaction state (leakage-prone). "Online?" = can Member 3 compute it in streaming with keyed state.

### 6A. PaySim features

**Feasibility verdict per requested feature idea (based on the data, not on hope):**

| Idea | Feasible? | Why |
|---|---|---|
| Transaction amount features | **Yes** | `amount` complete, heavy-tailed |
| Temporal features | **Yes, but weak/artifactual** | `step_hour`, `step`; fraud is ~uniform across hours |
| Customer/source-account behaviour | **No** | 99.85 % of `nameOrig` appear once |
| Destination behaviour | **Yes — the main behavioural axis** | 2.72 M accounts, ≤113 txns each; first-seen strongly separates |
| Transaction frequency / velocity | **Destination side only** | Origin-side velocity is degenerate; global per-step volume is leakage |
| Historical behaviour | **Destination side only** | Same reason |
| Amount deviation | **Yes vs. destination history and vs. `type` norms (train-fit)**; not vs. customer | |
| Transaction-type behaviour | **Yes** | Fraud confined to TRANSFER/CASH_OUT; mix of types received by a destination |
| Account-interaction features | **Weak** | 1,769 accounts are both origin and destination; (`nameOrig`,`nameDest`) pairs are unique per origin |

**A1. Amount and balance features (row-wise)**

| Feature | Source column(s) | Logic | Why useful | Computation type |
|---|---|---|---|---|
| `amount_log1p` | `amount` | `log1p(amount)` | tames skew (median 74.9 k, p99 1.6 M, max 92 M) | Row · Tier A · Online |
| `amount_zero_flag` | `amount` | `amount = 0` | 16 rows, all fraud (rare-event indicator) | Row · A · Online |
| `amt_to_oldbalOrg` | `amount`, `oldbalanceOrg` | `amount / (oldbalanceOrg + 1)` | relative size of the movement | Row · A · Online |
| `orig_drain_flag` | `amount`, `oldbalanceOrg` | `oldbalanceOrg > 0 AND abs(amount − oldbalanceOrg) < 0.005` | 8,018 fraud vs **0** legit → near-perfect artifact; keep, but **flag as artifact** | Row · A · Online |
| `amount_exceeds_orig_balance_flag` | `amount`, `oldbalanceOrg` | `amount > oldbalanceOrg` | 90 % of legit TRANSFER/CASH_OUT (simulator inconsistency) vs 0.35 % of fraud | Row · A · Online |
| `orig_balance_zero_flag` | `oldbalanceOrg` | `oldbalanceOrg = 0` | among TRANSFER/CASH_OUT with `amount>0`, 1,308,541 legitimate rows have an empty sender balance vs only **25** fraud rows (simulator artifact, but a strong split) | Row · A · Online |
| `dest_balance_zero_flag` | `oldbalanceDest`, `type` | `oldbalanceDest = 0 AND type != 'PAYMENT'` | merchants are structurally 0, so exclude PAYMENT | Row · A · Online |
| `amt_to_oldbalDest` | `amount`, `oldbalanceDest` | `amount / (oldbalanceDest + 1)` | size relative to recipient's holdings | Row · A · Online |
| `amount_z_by_type` | `amount`, `type` | `(amount − mean_type)/std_type` with per-`type` stats **fitted on train** | deviation from the type's norm | Broadcast (5-row table) · Train-fit · A · Online |
| `amount_pct_of_type_p99_flag` | `amount`, `type` | `amount > p99_type(train)` | large-transfer indicator | Broadcast · A · Online |

**A2. Post-transaction balance features (Tier B — separate, ablatable)**

| Feature | Source column(s) | Logic | Why useful | Computation type |
|---|---|---|---|---|
| `err_balance_orig` | `newbalanceOrig`, `amount`, `oldbalanceOrg` | `newbalanceOrig + amount − oldbalanceOrg` | 0 for 99.45 % of fraud vs 9.49 % of legit TRANSFER/CASH_OUT | Row · **Tier B** |
| `err_balance_dest` | `oldbalanceDest`, `amount`, `newbalanceDest` | `oldbalanceDest + amount − newbalanceDest` | recipient accounting error | Row · **B** |
| `orig_new_balance_zero_flag` | `newbalanceOrig` | `newbalanceOrig = 0` | account emptied | Row · **B** |
| raw `newbalanceOrig`, `newbalanceDest` | same | pass-through | included only so Member 4 can run the ablation | Row · **B** |

*Why a separate tier:* these values are the **outcome** of the transaction. A real-time scorer at authorization time would not have them. Whether they are "available" in the Kafka replay depends on how Member 3 emits events; the tier flag lets the team state honestly which results depend on them.

**A3. Temporal features**

| Feature | Source column(s) | Logic | Why useful | Computation type |
|---|---|---|---|---|
| `hour_sin`, `hour_cos` | `step_hour` | `sin/cos(2π·step_hour/24)` | cyclical encoding (23 and 0 are adjacent) | Row · A · Online |
| `sim_day` *(metadata, not a model feature)* | `step` | `(step − 1) div 24` | grouping/reporting/split bookkeeping | Row |
| `type_*` one-hot ×5 | `type` | `type = 'CASH_IN' …` | fraud only in TRANSFER/CASH_OUT | Row · A · Online |

**A4. Destination behaviour (window over strictly-earlier `step`, partition `nameDest`)** — *the only real behavioural signal in PaySim.*

| Feature | Source column(s) | Logic | Why useful | Computation type |
|---|---|---|---|---|
| `dest_prior_txn_count` | `nameDest`, `step` | count of rows for same `nameDest` with `step` < current | 0 for 541,957 TRANSFER/CASH_OUT rows containing 5,524 frauds | Window · A · Online (keyed state) |
| `dest_first_seen_flag` | same | `dest_prior_txn_count = 0` | 1.02 % fraud vs 0.09–0.16 % otherwise | Window · A · Online |
| `dest_txn_count_prev_24h` / `_168h` | `nameDest`, `step` | count in `step ∈ [t−24, t−1]` / `[t−168, t−1]` | destination velocity (fan-in burst) | Window (`rangeBetween`) · A · Online |
| `dest_prior_unique_senders_approx` | `nameDest`, `nameOrig`, `step` | `approx_count_distinct(nameOrig)` over prior rows | mule-like fan-in (many senders) | Window · A · Online (HLL state) |
| `dest_prior_amount_mean`, `dest_prior_amount_std` | `nameDest`, `amount`, `step` | mean / std over prior rows | recipient's normal inflow | Window · A · Online |
| `dest_amount_zscore` | `amount` + above | `(amount − mean)/std`, NULL if prior count < 3 | amount deviation vs recipient history | Window · A · Online |
| `dest_steps_since_last_txn` | `nameDest`, `step` | `step − max(prior step)`; NULL if first | recency / dormancy | Window · A · Online |
| `dest_prior_type_mix` (share CASH_OUT among prior received) | `nameDest`, `type` | prior CASH_OUT count / prior count | recipient role pattern | Window · A · Online |

**A5. Optional / low-value (build last, expect ≈0 lift):** `orig_prior_txn_count` (0–2; positive for 0.15 % of rows), `orig_was_dest_before_flag` (1,769 overlapping accounts), `dest_prior_fraud_count_lagged` (44 of 8,213 frauds; **label-leakage-prone**, Part 7).

**A6. Explicitly NOT recommended:** per-step/hour **volume** or **fraud-rate** features and "global velocity" (leak the label, Part 4 P5) · `isFlaggedFraud` as an input · any feature from `nameOrig`/`nameDest` *text* (IDs) · `dest_is_merchant` (duplicate of `type = PAYMENT`) · one-hot or embeddings of account IDs.

### 6B. IEEE-CIS features

**B1. Transaction and amount**

| Feature | Source column(s) | Logic | Why useful | Computation type |
|---|---|---|---|---|
| `amt_log1p` | `TransactionAmt` | `log1p` | skew (median 68.8, max 31,937) | Row · Online |
| `amt_decimals_gt2_flag` | `TransactionAmt` | `abs(amt·100 − round(amt·100)) > 1e-6` | 90.4 % true for `ProductCD='C'`, 0 % otherwise (currency-conversion fingerprint) | Row · Online |
| `amt_cents` | `TransactionAmt` | `round((amt − floor(amt))·100)` | price-point pattern (e.g. .00 / .95 / .99) | Row · Online |
| `amt_is_whole_flag` | `TransactionAmt` | `amt = floor(amt)` | card-testing / round-amount pattern | Row · Online |
| `amt_z_by_product` | `TransactionAmt`, `ProductCD` | z-score with per-product stats fitted on train | `ProductCD C` mean 42.9 vs W 153.2 | Broadcast · Train-fit · Online |
| `product_x_card6` | `ProductCD`, `card6` | string concat | debit/credit behave differently per product | Row · Online |

**B2. Temporal (from `TransactionDT`; reference instant unknown, so all are *relative*)**

| Feature | Source column(s) | Logic | Why useful | Computation type |
|---|---|---|---|---|
| `hour_of_day` | `TransactionDT` | `floor(DT/3600) mod 24` | fraud rate 2.4 % (h14) to 10.1 % (h9): strong | Row · Online |
| `hour_sin`, `hour_cos` | above | cyclical | | Row · Online |
| `day_index` | `TransactionDT` | `floor(DT/86400)` | drift bookkeeping / split (keep as metadata) | Row |
| `day_of_week_rel` | `day_index` | `day_index mod 7` (relative weekday, *not* calendar-anchored) | weekly cycle | Row · Online |
| `secs_since_prev_txn_global` | — | **do not build** (global order is a leakage/volume proxy) | | – |

**B3. Card-entity behaviour (proxy entity; see safety notes)** — partition key `card_key = hash(card1, card2, card3, card4, card5, card6)` (14,893 groups, 99.3 % of rows in groups of ≥2, largest group 14,112 rows); `card_addr_key = hash(card_key, addr1)` (43,018 groups, 97.1 %). Ordering: `(TransactionDT, TransactionID)`, windows strictly earlier.

| Feature | Source column(s) | Logic | Why useful | Computation type |
|---|---|---|---|---|
| `card_prior_txn_count` | card1–card6, `TransactionDT` | rows of same `card_key` before this one | new-card indicator | Window · Online (keyed state) |
| `card_first_seen_flag` | same | prior count = 0 | first use of a card in data (cold-start marker) | Window · Online |
| `card_txn_count_1h`, `_24h`, `_7d` | same | count in `DT ∈ [t−W, t−1]`, W = 3,600 / 86,400 / 604,800 s | velocity | Window (`rangeBetween`) · Online |
| `card_secs_since_last_txn` | same | `DT − previous DT`; NULL if first | recency | Window · Online |
| `card_prior_amt_mean`, `card_prior_amt_std` | + `TransactionAmt` | over prior rows | spending baseline | Window · Online |
| `amt_zscore_vs_card` | above | `(amt − mean)/std`; NULL if prior < 3 | amount deviation | Window · Online |
| `card_prior_distinct_addr1_approx` | + `addr1` | `approx_count_distinct` over prior rows | one card, many addresses | Window · Online (HLL) |
| `card_prior_distinct_pemail_approx` | + `P_emaildomain` | same | many emails per card | Window · Online |
| `card_prior_distinct_device_approx` | + `DeviceInfo` | same (non-null only) | many devices per card | Window · Online |
| `card_addr_prior_txn_count` | `card_addr_key` | prior rows | finer entity | Window · Online |
| `card_addr_change_flag` | `addr1`, prior `addr1` for card | `addr1 <> lag(addr1)` when both present | location change | Window · Online |

**B4. Device / browser**

| Feature | Source column(s) | Logic | Why useful | Computation type |
|---|---|---|---|---|
| `has_device_flag` | `DeviceType`, `DeviceInfo` | either non-null | device data present | Row · Online |
| `device_brand` | `DeviceInfo` | leading token / regex bucket (e.g. Windows, iOS, SM-, Moto, …), rare → `RARE` | 1,786 raw levels → ~30 buckets | Row (regexp) |
| `os_family` | `id_30` (75 levels), `DeviceInfo` | regex family (Windows/iOS/Android/Mac/other) | | Row |
| `browser_family`, `browser_version_num` | `id_31` (130 levels) | split name / numeric version | | Row |
| `screen_w`, `screen_h`, `screen_pixels` | `id_33` (`WxH`, 260 levels) | split on `x` | resolution fingerprint | Row |
| `device_info_freq`, `id_31_freq`, `id_33_freq` | same | frequency encoding (train) | rare device = risk | Broadcast · Train-fit |

**B5. Email**

| Feature | Source column(s) | Logic | Why useful | Computation type |
|---|---|---|---|---|
| `p_email_provider`, `p_email_tld` | `P_emaildomain` | split at first / last `.` | groups 59 domains; `protonmail.com` 76 rows at 40.8 % fraud, `hotmail.com` 5.3 %, `gmail.com` 4.35 % | Row |
| `p_r_email_match_flag` | `P_emaildomain`, `R_emaildomain` | equal (NULL if either missing) | 81.2 % equal when both present (126,227 rows) | Row |
| `p_email_freq`, `r_email_freq` | same | frequency (train) | | Broadcast · Train-fit |

**B6. Address**

| Feature | Source column(s) | Logic | Why useful | Computation type |
|---|---|---|---|---|
| `addr1_addr2` | `addr1`, `addr2` | concat | region×country | Row |
| `addr2_non87_flag` | `addr2` | `addr2 ≠ 87` (non-null) | 10.2 % fraud vs 2.4 % | Row |
| `dist1_log1p`, `dist2_log1p` | `dist1`, `dist2` | `log1p` | tails to 11,623 | Row |
| `addr1_freq` | `addr1` | frequency (train) | | Broadcast · Train-fit |

**B7. Identity completeness and missingness**

| Feature | Source column(s) | Logic | Why useful | Computation type |
|---|---|---|---|---|
| `has_identity_flag` | `id_01–id_38`, `DeviceType`, `DeviceInfo` | any non-null | 7.85 % vs 2.09 % fraud | Row |
| `identity_nonnull_count` | same 40 columns | count non-null (0–40) | completeness gradient | Row |
| `identity_completeness` | above | `count / 40` | | Row |
| `ind_V1_11`, `ind_V12_34`, `ind_V35_52`, `ind_V53_74`, `ind_V75_94`, `ind_V138_166`, `ind_V167_278`, `ind_V322_339` | representative V per block | `col IS NOT NULL` for the block's mask group | block presence (null rates 47 %/13 %/29 %/13 %/15 %/86 %/76–78 %/86 %) | Row |
| `ind_addr`, `ind_dist1`, `ind_dist2`, `ind_Pemail`, `ind_Remail` | `addr1`, `dist1`, `dist2`, `P_/R_emaildomain` | `IS NOT NULL` | addr null: 11.8 % vs present 2.5 % fraud | Row |
| `ind_D2`, `ind_D3`, `ind_D4`, `ind_D5`, `ind_D6`, `ind_D7`, `ind_D10`, `ind_D11`, `ind_D12`, `ind_D13`, `ind_D14`, `ind_D15` | `D*` | `IS NOT NULL` (per column; masks differ) | `D7` present 14.9 % vs null 2.7 % | Row |
| `ind_M1_3`, `ind_M4`, `ind_M5`, `ind_M6`, `ind_M7`, `ind_M8_9` | `M*` | `IS NOT NULL` by mask group | | Row |

*Budget:* ≈ 30 indicators — **not** 157 (one per column). Per-column indicators for columns sharing a mask are perfectly collinear.

**B8. Aggregates of masked families**

| Feature | Source column(s) | Logic | Why useful | Computation type |
|---|---|---|---|---|
| `m_true_cnt`, `m_false_cnt`, `m_null_cnt` | `M1–M9` | counts of `T` / `F` / null | compresses 9 correlated flags | Row |
| `c_sum_log1p`, `c_max`, `c_nonzero_cnt` | `C1–C14` | sum / max / non-zero count | C's are counts; a mass indicator | Row |
| `d_nonnull_cnt`, `d_min`, `d_max`, `d_mean` | `D1–D15` (excl. `D8`, `D9` pending investigation) | across non-null Ds | recency summary | Row |
| `d1_start_day_rel` | `TransactionDT`, `D1` | `floor(DT/86400) − D1` | proxy "first-seen day" (semantics unverified; validate before use) | Row |
| `vgrp_<range>_mean`, `vgrp_<range>_nonnull_cnt` for `V1–V11`, `V12–V34`, `V35–V52`, `V53–V74`, `V75–V94`, `V95–V137`, `V138–V166`, `V167–V216`, `V217–V278`, `V279–V321`, `V322–V339` | V columns of the range | mean / count of non-null | dimensionality reduction over 339 masked columns | Row |

**B9. Frequency / interaction**

| Feature | Source column(s) | Logic | Why useful | Computation type |
|---|---|---|---|---|
| `card1_freq`, `card2_freq`, `card4_freq` … | `card*` | share of train rows with this value | 7,041 `card1` values appear <5 times | Broadcast · Train-fit |
| `card_key_freq` | `card_key` | train frequency (else "expanding count" as in B3) | | Broadcast · Train-fit |
| `product_x_pemail_provider` | `ProductCD`, `P_emaildomain` | concat | | Row |

**Recommended build order (IEEE):** P0 = B1, B2, B7 (identity + indicators) → P1 = B4, B5, B6, B8, B9 → P2 = B3 (entity behaviour; needs the most care). Everything in P0/P1 is a single Spark `select` with no shuffle.

### 6C. Cross-dataset feature concepts

The two datasets come from different generative processes (0.13 % vs 3.5 % fraud; simulated vs real masked data) and **must not be pooled for training**. What they share is a *concept vocabulary* so that Member 3 can implement one streaming engine with two feature configs:

| Concept | PaySim realisation | IEEE-CIS realisation |
|---|---|---|
| Amount magnitude & shape | `amount_log1p`, `amt_to_oldbalOrg`, `amount_z_by_type` | `amt_log1p`, `amt_decimals_gt2_flag`, `amt_z_by_product` |
| Cyclical time | `hour_sin/cos` from `step_hour` | `hour_sin/cos` from `TransactionDT` |
| Novelty of counterparty/instrument | `dest_first_seen_flag`, `dest_prior_txn_count` | `card_first_seen_flag`, `card_prior_txn_count`, `card_addr_prior_txn_count` |
| Velocity | `dest_txn_count_prev_24h/168h` | `card_txn_count_1h/24h/7d` |
| Deviation from entity baseline | `dest_amount_zscore` | `amt_zscore_vs_card` |
| Recency | `dest_steps_since_last_txn` | `card_secs_since_last_txn` |
| Data-completeness signal | *(none: PaySim has no nulls)* | `has_identity_flag`, `identity_nonnull_count`, block indicators |
| Type/product norm | `type_*`, `amount_z_by_type` | `ProductCD`, `card4/6` |

Shared **naming contract** for the streaming layer: `event_id`, `event_time` (synthetic from `step` / `TransactionDT` — never presented as real), `entity_id`, `counterparty_id`, `amount`, `label`.



---

## 7. PART 7 — Feature-engineering safety (leakage, contamination, real-time realism)

Goal: every number the team reports must survive the question *"could the system have known this at the time it scored the transaction, and was any test information used to build the training features?"*

### 7.1 Train / validation / test strategy (decide **before** building features)

**Use time-ordered splits, never random row splits**, for both datasets. Persist the result as a `split` column so Member 4 cannot re-split differently.

| Dataset | Recommended cut (config-driven) | Resulting prevalence | Comment |
|---|---|---|---|
| **IEEE-CIS** | train: `TransactionDT` ≤ ≈ day 130.2 (75 % of rows, 442,905 rows, 15,563 fraud); validation: later (147,635 rows, 5,100 fraud). Optionally carve the last 15 % as test. Leave a gap (e.g. 7 days) between splits. | **3.514 % train / 3.454 % valid** — stable | Real time drift is mild (monthly fraud 2.7–4.3 %). |
| **PaySim** | train: steps 1–500 (6,061,807 rows, 5,561 fraud, 0.092 %); valid: 501–600 (197,240 rows, 1,052 fraud, **0.533 %**); test: 601–743 (103,573 rows, 1,600 fraud, **1.545 %**) | prevalence rises **≈ 6× / 17×** across splits | **A simulator artifact** (constant fraud per day, shrinking traffic; Part 4 P5). Must be reported, and metrics must be prevalence-aware (PR-AUC, recall@fixed precision, not accuracy/ROC-AUC alone). Also run a secondary check (e.g. 5 forward-chaining folds over steps) to show conclusions are not an artifact of one cut. |

*Cut points are proposals to agree with Member 4; the config must make them changeable.*

### 7.2 Dangerous features and how to compute them safely

| Feature / operation | Danger | Safe computation |
|---|---|---|
| **Fraud history** (`dest_prior_fraud_count`, `card_prior_fraud_count`, fraud rate of a device/email/card) | **Target leakage** + **label delay**: a real system learns a fraud label days–months after the event (IEEE labels are described publicly as chargeback-derived; PaySim labels are instantaneous only because it is simulated). Using labels newer than the label delay is impossible online. | Include only rows with `time ≤ t − label_lag` (config `label_lag`, e.g. 24 steps for PaySim as a conservative default; ≥ several weeks for IEEE). Implement as a conditional sum over `rangeBetween(unboundedPreceding, −label_lag)`. Never compute from the full table. In PaySim only 44 of 8,213 frauds have any earlier fraud on the destination — **skip** unless time permits. |
| **Rolling windows** (counts, sums in last N hours/seconds) | **Future-information leakage** if the frame includes the current row, later rows, or same-step ties | `rangeBetween(−W, −1)` on `step` (PaySim) or `TransactionDT` (IEEE); **exclude current row**; for IEEE order by `(TransactionDT, TransactionID)`. Same-step / same-DT peers are excluded in PaySim by using step ranges (they cannot be ordered). |
| **Customer / card historical statistics** (mean, std, z-score) | Leakage if computed by `groupBy(entity).agg(...)` over the whole dataset and joined back (includes the row itself and the future) | Expanding window over strictly-earlier rows only; NULL until a minimum history (e.g. ≥ 3); add `first_seen_flag`. **Ban the groupBy-and-join pattern for behavioural features.** |
| **Destination statistics** (`dest_prior_*`) | Same as above; plus unbounded state for streaming | Same window recipe; for online parity give every counter a **bounded horizon** (24 h / 168 h) or define TTL; cumulative "since dataset start" features need keyed state without expiry — document the memory cost. |
| **Aggregate / static encoders** (frequency maps, target encoding, medians, per-`type`/`ProductCD` stats, scaler, quantile bins, V-redundancy mapping) | **Train/test contamination**: fitting on all rows leaks validation/test distribution (frequency of a value in the test period is information) | Fit on `split = 'train'` only → save as reference tables → **broadcast-join** to all splits. Target encodings, if used, must be time-blocked out-of-fold and smoothed; never on IDs. |
| **Per-step / per-hour volume, global velocity, per-step fraud rate** (PaySim) | **Label leakage through the simulator**: volume is inversely tied to fraud prevalence (steps with <100 txns are 79.2 % fraud) | Do not use as model inputs. If shown on the dashboard, label it as descriptive. |
| **Post-transaction balances** (`newbalanceOrig`, `newbalanceDest`, `err_balance_*`) | Outcome-of-transaction information; not available at authorization | Keep in **Tier B**; ship features so that Member 4 can train Tier A only and Tier A+B and report both. |
| **`isFlaggedFraud`** | Output of the simulator's own rule engine (fires on 16 rows) | Exclude from inputs; use only as a rules-baseline comparison. |
| **`TransactionID`, raw `TransactionDT`, `nameOrig`, `nameDest`** | Monotone with time / identify entities → model memorises time or accounts | Exclude from features; keep as keys/ordering columns. |
| **IEEE `D*`, `C*`, `V*`** | Vendor-engineered from history; cannot be audited from the ZIP (may embed information not available at scoring time) | Keep, but run the **random-split vs time-split gap check** and report it. If the gap is large, investigate the offending blocks by ablation. |
| **IEEE `card_key`/UID proxies** | A proxy entity can merge different people (false grouping) or fragment one; UIDs built from `D1` rely on a vendor column | Primary key = `card1–card6` (+`addr1` as a second key). Treat `D1`-based start-day as experimental; validate on the time split. |
| **Warm-up / cold start** | The first `W` hours of the dataset have artificially "empty" histories → first-seen flags are true by construction | Add `history_window_complete_flag` (`step ≥ W + 1`, or `DT ≥ min(DT) + W`) and either exclude warm-up rows from training or let the model see the flag. |
| **Duplicates across splits** | 3 IEEE near-duplicate rows (same everything, different ID) could straddle a split | Negligible; note only. |

### 7.3 Unrealistic real-time behaviour to avoid

- **Features needing future rows** (`lead`, whole-table percentiles/ranks, dedupe on full data).
- **Whole-dataset ordering assumptions** — PaySim is globally step-sorted only by accident; IEEE parts are 4 interleaved runs. Define ordering explicitly.
- **Same-step semantics mismatch.** Offline PaySim features use "strictly earlier step". Member 3 must compute *as of the end of the previous step* (or the team must define an explicit in-step arrival order and use it in **both** places). Otherwise online features differ from offline features (train/serve skew).
- **Unbounded state** (`unique senders` since the start of time) → streaming state grows without limit; prefer bounded windows or TTL.
- **Joins to large tables at scoring time** — only small broadcast reference tables (frequency maps, medians) are acceptable online.
- **Features that need post-transaction information** (Tier B) presented as if real-time.

### 7.4 Interview-defensible checklist (all must be true before handing off)

1. Every behavioural feature is computed from rows **strictly earlier** than the scored row (unit test: shuffle input order → identical output).
2. All fitted statistics come from `split = 'train'` only and are stored as artifacts.
3. No feature uses `isFraud` of a row within `label_lag` of the scored row.
4. Splits are time-ordered; prevalence per split is printed in the validation report.
5. Tier B features are separable; PaySim results are reported with and without them.
6. The PaySim simulator artifacts (drain rule, constant fraud/step, `isFlaggedFraud`) are stated in the documentation.
7. A random-vs-time split comparison is included for IEEE.
8. A parity test compares offline features with a row-by-row (streaming-style) recomputation on a sample.



---

## 8. PART 8 — Spark implementation plan (design only; no full implementation)

### 8.1 Pipeline

```
RAW CLEAN DATA  (Member 1 Parquet, read-only)
   paysim_clean.parquet  6,362,620 x 12      ieee_clean.parquet  590,540 x 434
        |
        v
SPARK READ            spark.read.schema(<explicit StructType>).parquet(dir)   (folder, not a single part)
        |
        v
SCHEMA VALIDATION     compare to contracts/*_input_schema.json; assert row count, dtypes,
                      key uniqueness, isFraud in {0,1}, type in 5 values  -> FAIL FAST
        |
        v
BASE PREP             PaySim: txn_id (hash), sim_day        IEEE: card_key, card_addr_key, parsed strings
                      (deterministic ordering columns; no shuffle)
        |
        v
ROW-WISE FEATURES     one wide select(): amounts, ratios, flags, cyclical time, missingness indicators,
                      block aggregates, regexp parsing            (0 shuffles)
        |
        v
SPLIT ASSIGNMENT      time-based `split` column (config cut points)
        |
        v
TRAIN-FIT ARTIFACTS   groupBy on split='train' -> tiny tables (freq maps, medians, per-type/product stats)
                      -> saved as parquet/json -> broadcast joins
        |
        v
BEHAVIOURAL FEATURES  window functions per entity (PaySim: nameDest; IEEE: card_key) - strictly-prior frames
        |
        v
VELOCITY / ROLLING    rangeBetween on step (PaySim) / TransactionDT (IEEE); same window spec, one exchange
        |
        v
FEATURE VALIDATION    nulls, ranges, per-split prevalence, leakage screens, offline-vs-online parity sample
        |
        v
PARQUET OUTPUT        features/<dataset>/ (+ manifest.json, encoders/, validation_report)
        |
        v
MEMBER 4 ML
```

### 8.2 Spark technique recommendations (with reasons grounded in the data)

| Topic | Recommendation | Rationale (from the audit) |
|---|---|---|
| **Versions / environment** | Use **PySpark 4.x** (Member 1's files were written by Spark 4.0.4) with **Java 17 or 21** and Python ≥ 3.10; pin exact versions in `requirements.txt`; state whether running local or against the HDFS cluster | Team-wide version alignment prevents Parquet/Arrow surprises. *(Spark 4.x dropped Java 8/11 support — confirm against the installed distribution's docs.)* |
| **Reading** | Read the **directory** with an explicit schema (`StructType`) instead of `inferSchema`; `mergeSchema=false`; read only needed columns for IEEE (columnar pruning) | Schema identical across parts; IEEE has 434 columns, many unused per step |
| **DataFrame ops** | Prefer built-in column expressions (`F.when`, `F.regexp_extract`, `F.split`, `F.log1p`, `F.hash/xxhash64/sha2`) over Python UDFs; build the row-wise features in **one** `select`/`withColumns` to keep a single narrow stage; for the ~30 indicators/aggregates over V blocks use `F.expr("(v1 is not null)::int + ...")` style array/expressions rather than 339 separate `withColumn` calls (which bloat the plan) | avoids Python serialization cost and analyzer blow-up on 434-column frames |
| **Window functions** | `Window.partitionBy("nameDest").orderBy("step").rangeBetween(-W, -1)` (PaySim); `Window.partitionBy("card_key").orderBy("TransactionDT","TransactionID").rangeBetween(-W, -1)` (IEEE); cumulative: `rangeBetween(Window.unboundedPreceding, -1)` on PaySim (ties excluded) / `rowsBetween(unboundedPreceding, -1)` on IEEE (total order via ID tiebreak). Use `approx_count_distinct` (distinct aggregates such as `countDistinct` are **not** supported over windows). | ties inside a `step` cannot be ordered (290,449 rows share `(nameDest, step)`) → `range` frames; 33,932 IEEE rows share a `TransactionDT` → tiebreak by ID |
| **Sharing shuffles** | Declare all windows of one entity with the **identical** partition/order spec so Spark plans **one** exchange + sort. Compute PaySim dest windows in one pass; IEEE `card_key` windows in one pass; only add a second pass for `card_addr_key` if the feature proves useful. Do **not** create origin-side windows (99.85 % singletons: a full shuffle for ≈ 0 information). | window = shuffle by key; minimise distinct keys |
| **Partitioning** | Input is 4 partitions (≈ 70 MB each PaySim). Before window stages: `repartition(N, "nameDest")` / `repartition(N, "card_key")` with **N = 16–32 on a laptop**, ≈ 64–128 in a cluster (target ≈ 100–200 MB per shuffle partition). Set `spark.sql.shuffle.partitions` explicitly (default 200 gives tiny tasks for 6 M rows); leave AQE on. | Group sizes are small (PaySim max 113 rows per `nameDest`; IEEE max 14,112 rows per `card_key`) → **no severe skew**; do not add salting |
| **High-cardinality columns** | Never one-hot `card1` (13,553), `card2` (500), `DeviceInfo` (1,786), `addr1` (332), emails (59/60), `id_31/id_33/id_30`, `nameOrig/nameDest`. Use frequency encoding (train-fit, broadcast), rare-level bucketing (`< 50` train rows → `RARE`), and regexp bucketing for `DeviceInfo`. Keep IDs as keys only. | Prevents column explosion; IDs are not features |
| **Broadcast joins** | **Only** for small train-fit artifacts: per-`type` stats (5 rows), per-`ProductCD` stats (5 rows), frequency maps (≤ 13.6 k rows), imputation medians. **Never** broadcast entity histories or the base table. | Autobroadcast threshold default (10 MB) is fine for these |
| **Caching** | PaySim: **no** caching until the final feature DataFrame; then `persist(MEMORY_AND_DISK)` once before the validation actions + write, `unpersist` after. IEEE: cache **after** the row-wise stage only if it is used by ≥ 2 actions (train-fit stats pass + window pass); select the needed columns first (434 doubles × 590 k rows ≈ 2 GB decompressed). | Cache only when reused by multiple actions; avoid caching a 434-col frame you never re-scan |
| **Repartition / coalesce for output** | PaySim: `repartition(8)` (≈ 6.4 M rows, ≈ 40–80 MB per file); IEEE: `coalesce(4)`; optionally `partitionBy("split")` (2–3 values) — **never** partition by `step` (743 tiny dirs) or by any high-cardinality column. Sort within partitions by `(step)` / `(TransactionDT, TransactionID)` for time-ordered reads. | Few, medium files; a wide-shuffle `repartition` is acceptable at the very end, `coalesce` avoids it for IEEE |
| **Avoiding unnecessary shuffles** | Row-wise features first (narrow), windows next (one exchange per entity), no `orderBy` on the whole DataFrame, no `distinct`/`dropDuplicates` (inputs already de-duplicated), no `collect()` of big frames, no joins for behavioural features (use windows) | |
| **Determinism** | `txn_id = sha2(concat_ws('\|', step, type, amount, nameOrig, nameDest), 256)` — unique in the data (the 5-tuple has 0 duplicates). Sorting by `(step, txn_id)` gives reproducible order even though Spark doesn't preserve file order. | PaySim has no ID and only hour-level time |
| **Local-mode sizing** | `local[*]`, driver memory 6–8 GB, `spark.sql.shuffle.partitions=16..32`, `spark.sql.parquet.compression.codec=snappy` | PaySim ≈ 268 MB Parquet; fits a laptop |
| **Testing** | Small fixture: 200 PaySim rows + 500 IEEE rows committed to GitHub as CSV/Parquet; unit tests for window semantics (shuffled input → same output) and for "no row sees itself or the future" | Interview-defensible correctness |

### 8.3 Suggested job decomposition

| Job | Reads | Writes | Shuffles |
|---|---|---|---|
| `00_validate_inputs` | Member 1 Parquet | `validation/input_report.json` | 0–1 (uniqueness) |
| `10_paysim_base_and_rowwise` | paysim_clean | temp `paysim_stage1` | 0 |
| `20_paysim_fit_artifacts` | stage1 (train split) | `encoders/paysim/*` | 1 small groupBy |
| `30_paysim_behaviour_windows` | stage1 + encoders | `paysim_features` | 1 exchange (by `nameDest`) |
| `40_ieee_base_and_rowwise` | ieee_clean (needed cols) | temp `ieee_stage1` | 0 |
| `50_ieee_fit_artifacts` | stage1 (train) | `encoders/ieee/*` | a few small groupBy |
| `60_ieee_behaviour_windows` | stage1 + encoders | `ieee_features` | 1 exchange (by `card_key`) |
| `70_validate_features` | both feature tables | `validation/feature_report.json` | small aggregates |



---

## 9. PART 9 — Output contract for Member 4

**General rules (both datasets):** Parquet, Snappy, folder output (`_SUCCESS` present), one row per transaction, column names exactly as below, **no imputation applied** (nulls preserved; medians shipped separately), categoricals shipped as **strings** (with literal `MISSING` for null) *plus* numeric frequency encodings where stated, flags as `tinyint` (0/1), counts as `int`/`bigint`, continuous as `double`. Every column carries metadata in `manifest.json`: `role`, `tier`, `dtype`, `online_computable`, `null_policy`. Member 4 selects columns **by role/tier from the manifest**, never by guessing.

### 9.1 PaySim — `features/paysim_features.parquet` (grain: 1 row = 1 transaction)

**Keys, target, bookkeeping (never features)**

| Column | Data type | Meaning | Required? |
|---|---|---|---|
| `txn_id` | string | Deterministic surrogate key (sha2 of `step,type,amount,nameOrig,nameDest`); unique | **Yes** |
| `step` | int | Simulation hour (1–743); ordering axis | **Yes** |
| `nameOrig` | string | Sender ID (entity key, **not a feature**) | Yes |
| `nameDest` | string | Recipient ID (entity key, **not a feature**) | Yes |
| `isFraud` | int (0/1) | **Target label** | **Yes** |
| `split` | string {`train`,`valid`,`test`} | Time-based split assignment | **Yes** |
| `sim_day` | int | `(step-1) div 24` (bookkeeping/reporting) | Yes |
| `history_window_complete_flag` | tinyint | 1 if `step` ≥ longest window + 1 (warm-up marker) | Yes |
| `isFlaggedFraud` | int | Simulator rule flag — **reference only, do not train on** | Optional |

**Tier A — features knowable at authorization**

| Column | Data type | Meaning | Required? |
|---|---|---|---|
| `type` | string | CASH_IN / CASH_OUT / DEBIT / PAYMENT / TRANSFER (categorical) | Yes |
| `type_cash_in`, `type_cash_out`, `type_debit`, `type_payment`, `type_transfer` | tinyint | One-hot of `type` | Yes |
| `amount` | double | Raw amount | Yes |
| `amount_log1p` | double | `log1p(amount)` | Yes |
| `amount_zero_flag` | tinyint | `amount = 0` (16 rows) | Optional |
| `oldbalanceOrg` | double | Sender balance before | Yes |
| `oldbalanceDest` | double | Recipient balance before (0 for merchants) | Yes |
| `amt_to_oldbalOrg` | double | `amount/(oldbalanceOrg+1)` | Yes |
| `amt_to_oldbalDest` | double | `amount/(oldbalanceDest+1)` | Yes |
| `orig_drain_flag` | tinyint | `oldbalanceOrg>0 and amount≈oldbalanceOrg` (**simulator artifact; near-perfect separator**) | Yes |
| `amount_exceeds_orig_balance_flag` | tinyint | `amount > oldbalanceOrg` | Yes |
| `orig_balance_zero_flag` | tinyint | `oldbalanceOrg = 0` | Yes |
| `dest_balance_zero_flag` | tinyint | `oldbalanceDest = 0 and type != PAYMENT` | Yes |
| `amount_z_by_type` | double | z-score with train-fit per-type stats | Yes |
| `step_hour` | int | 0–23 (from Member 1) | Yes |
| `hour_sin`, `hour_cos` | double | Cyclical `step_hour` | Yes |
| `dest_prior_txn_count` | int | Earlier transactions to this `nameDest` (steps < t) | Yes |
| `dest_first_seen_flag` | tinyint | `dest_prior_txn_count = 0` | Yes |
| `dest_txn_count_prev_24h`, `dest_txn_count_prev_168h` | int | Count in `[t−24,t−1]`, `[t−168,t−1]` | Yes |
| `dest_prior_unique_senders_approx` | int | HLL distinct `nameOrig` among prior rows | Yes |
| `dest_prior_amount_mean`, `dest_prior_amount_std` | double | NULL if no prior | Yes |
| `dest_amount_zscore` | double | NULL if prior < 3 | Yes |
| `dest_steps_since_last_txn` | int | NULL if first | Yes |
| `dest_prior_cashout_share` | double | Prior CASH_OUT share | Optional |

**Tier B — post-transaction (leakage-prone; ablate)**

| Column | Data type | Meaning | Required? |
|---|---|---|---|
| `newbalanceOrig`, `newbalanceDest` | double | Balances after the transaction | Yes (tier B) |
| `err_balance_orig`, `err_balance_dest` | double | Accounting errors | Yes (tier B) |
| `orig_new_balance_zero_flag` | tinyint | `newbalanceOrig = 0` | Yes (tier B) |

**Feature-family summary for Member 4:** numerical = `amount*`, balances, ratios, z-scores, counts · categorical = `type` (+ one-hots) · temporal = `step_hour`, `hour_sin/cos` (`step`, `sim_day` are bookkeeping) · behavioural = all `dest_*` · **excluded from training:** keys, `isFraud`, `split`, `isFlaggedFraud`, bookkeeping.

### 9.2 IEEE-CIS — `features/ieee_features.parquet` (grain: 1 row = 1 transaction)

**Keys, target, bookkeeping (never features)**

| Column | Data type | Meaning | Required? |
|---|---|---|---|
| `TransactionID` | int | Primary key (unique) | **Yes** |
| `isFraud` | int (0/1) | **Target label** | **Yes** |
| `TransactionDT` | int | Seconds from undisclosed reference; ordering only | **Yes** |
| `split` | string | `train` / `valid` / `test` (time-based) | **Yes** |
| `day_index` | int | `floor(DT/86400)` | Yes |
| `card_key`, `card_addr_key` | string | Entity keys (hash of `card1–card6` / + `addr1`) | Yes |
| `history_window_complete_flag` | tinyint | Warm-up marker | Yes |

**Retained raw columns** (unchanged names, dtype, nulls preserved) — exactly the columns whose Part 3.3 treatment is `1_KEEP_IMPUTE`, `2_KEEP_MISSING_INDICATOR`, or `3_KEEP_CATEGORICAL`, i.e. **all columns except** the 14 dropped, `TransactionID`, `isFraud`, `TransactionDT`, and (pending investigation) `D8`, `D9`, `id_02`, `id_07`, `id_08`, `id_22`, `id_26`, which are shipped in a separate `ieee_features_experimental.parquet` keyed by `TransactionID`.

| Group | Columns | Type | Required? |
|---|---|---|---|
| Amount | `TransactionAmt` | double | Yes |
| Counting | `C1–C14` | double | Yes |
| Timedelta | `D1–D7, D10–D15` | double | Yes |
| Distance | `dist1`, `dist2` | double | Yes |
| Vesta | `V*` except `V1, V14, V27, V28, V41, V65, V68, V88, V89, V107, V240, V241, V305` (326 columns) | double (float32 acceptable) | Yes |
| Identity numeric | `id_01, id_03–id_06, id_09–id_11` | double | Yes |
| Categorical (string, `MISSING` for null) | `ProductCD, card1–card6, addr1, addr2, P_emaildomain, R_emaildomain, M1–M9, DeviceType, DeviceInfo, id_12–id_21, id_23–id_25, id_28–id_38` | string | Yes |

**Engineered columns**

| Column(s) | Data type | Meaning | Required? |
|---|---|---|---|
| `amt_log1p`, `amt_cents` | double / int | Amount transforms | Yes |
| `amt_decimals_gt2_flag`, `amt_is_whole_flag` | tinyint | Amount fingerprints | Yes |
| `amt_z_by_product` | double | Train-fit per-product z-score | Yes |
| `hour_of_day`, `hour_sin`, `hour_cos`, `day_of_week_rel` | int / double | Relative temporal features | Yes |
| `has_identity_flag`, `identity_nonnull_count`, `identity_completeness` | tinyint / int / double | Identity completeness | Yes |
| `ind_*` (≈ 30 block indicators listed in Part 6B7) | tinyint | Missingness block presence | Yes |
| `m_true_cnt`, `m_false_cnt`, `m_null_cnt` | int | M aggregates | Yes |
| `c_sum_log1p`, `c_max`, `c_nonzero_cnt` | double / int | C aggregates | Yes |
| `d_nonnull_cnt`, `d_min`, `d_max`, `d_mean` | int / double | D aggregates | Yes |
| `vgrp_<range>_mean`, `vgrp_<range>_nonnull_cnt` (11 ranges) | double / int | V-family aggregates | Optional |
| `p_email_provider`, `p_email_tld` | string | Email parsing | Yes |
| `p_r_email_match_flag` | tinyint (nullable) | P/R domains equal | Yes |
| `device_brand`, `os_family`, `browser_family` | string | Normalised device strings | Yes |
| `browser_version_num`, `screen_w`, `screen_h`, `screen_pixels` | double / int | Parsed strings | Yes |
| `addr1_addr2`, `addr2_non87_flag`, `dist1_log1p`, `dist2_log1p`, `product_x_card6` | string / tinyint / double | Address & interaction features | Yes |
| `card1_freq`, `card2_freq`, `addr1_freq`, `P_emaildomain_freq`, `R_emaildomain_freq`, `DeviceInfo_freq`, `id_31_freq`, `id_33_freq` | double | Train-fit frequency encodings | Yes |
| `card_prior_txn_count`, `card_first_seen_flag`, `card_txn_count_1h/24h/7d`, `card_secs_since_last_txn`, `card_prior_amt_mean/std`, `amt_zscore_vs_card`, `card_prior_distinct_addr1/pemail/device_approx`, `card_addr_prior_txn_count`, `card_addr_change_flag` | int / double / tinyint | Behavioural (Part 6B3) | Yes (phase P2) |

**Feature-family summary for Member 4:** numerical = amounts, `C*`, `D*`, `V*`, `dist*`, aggregates, z-scores, counts · categorical = strings above (frequency-encode high-cardinality) · temporal = `hour_*`, `day_of_week_rel` · behavioural = `card_*`, `card_addr_*` · missingness = `ind_*`, `identity_*`, `has_identity_flag` · **excluded from training:** `TransactionID`, `TransactionDT`, `day_index`, `card_key`, `card_addr_key`, `split`, `isFraud`, `history_window_complete_flag`.

### 9.3 Sidecar artifacts

| File | Content | Consumer |
|---|---|---|
| `manifest.json` (per dataset) | for every column: `dtype`, `role` (key/target/feature/bookkeeping), `tier` (A/B), `family`, `online_computable` (Y/N), `null_policy`, `source_columns`, `version` | Members 3, 4, 5 |
| `encoders/<dataset>/*.parquet` or `.json` | Train-fit frequency maps, per-type/product stats, imputation medians, rare-level lists | Member 3 (streaming parity), Member 4 |
| `validation/feature_report.json` | row counts, null %, per-split prevalence, leakage-screen results | Everyone |
| `contracts/*_input_schema.json` | Expected Member 1 input schemas | Member 2 (validation) |



---

## 10. PART 10 — Recommended directory structure

**What actually exists:** the ZIP root contains `paysim_clean.parquet/`, `ieee_clean.parquet/` and `data_documentation.md`. The documentation *describes* a data-lake layout (`raw_data/`, `processed_data/`, `hive_warehouse/`, `docs/`) that was not delivered. Member 2 should adopt that layout for **data** (kept out of Git) and add code beside it **without modifying anything Member 1 produced**.

```
fraud-intelligence-platform/                 <- Git repository root
├── README.md                                <- how to run everything end-to-end
├── Makefile                                 <- make validate | make paysim | make ieee | make test
├── .gitignore                               <- see Part 11.3
├── docs/
│   ├── member1/
│   │   ├── data_documentation.md            <- as delivered, UNCHANGED
│   │   └── MEMBER1_AUDIT_AND_HANDOFF_REPORT.md   <- this report
│   └── member2/
│       ├── feature_spec.md                  <- every feature: source cols, logic, tier, online?
│       ├── leakage_and_split_policy.md      <- Part 7 decisions once agreed with Member 4
│       └── runbook.md
├── member1/                                 <- Member 1's territory (do not edit)
│   └── (cleaning code, Hive DDL — TO BE COMMITTED BY MEMBER 1)
├── spark/                                   <- Member 2's territory
│   ├── common/                              <- spark_session.py, io.py, schema.py, splits.py, windows.py
│   ├── paysim_features/                     <- 10_rowwise.py, 20_fit_artifacts.py, 30_dest_windows.py
│   ├── ieee_features/                       <- 40_rowwise.py, 50_fit_artifacts.py, 60_card_windows.py
│   ├── validation/                          <- 00_validate_inputs.py, 70_validate_features.py
│   ├── configs/                             <- paths.yaml, split.yaml, paysim_features.yaml, ieee_features.yaml
│   └── tests/                               <- pytest + fixtures/ (≈200 PaySim rows, ≈500 IEEE rows)
├── contracts/                               <- input schemas (Member 1 -> 2) and output schemas/manifests (2 -> 3/4/5)
│   ├── paysim_input_schema.json   ieee_input_schema.json
│   └── paysim_features_manifest.json   ieee_features_manifest.json
├── audit/                                   <- scripts + CSVs shipped with this report
│   ├── run_all.sh   audit_table_level_checks.py   profile_ieee.py   derive_ieee_tables.py
│   ├── v_redundancy.py   build_ieee_schema.py   build_paysim_schema.py
│   └── paysim_schema.csv  ieee_schema.csv  ieee_missingness_groups.csv  v_redundancy_r098.csv  near_constant_minority.csv
├── environment/                             <- requirements.txt (pinned), optional Dockerfile, .env.example
├── data/                                    <- **GIT-IGNORED**; mirrored on HDFS
│   ├── raw_data/            paysim.csv  train_transaction.csv  train_identity.csv
│   ├── processed_data/      paysim_clean.parquet/  ieee_clean.parquet/      <- Member 1 output (read-only)
│   └── features/            paysim_features.parquet/  ieee_features.parquet/  encoders/  validation/   <- Member 2 output
└── output/                  <- scratch/reports (git-ignored except small summaries)
```

**HDFS mirror (suggested):** `hdfs:///fraud/processed_data/…` (Member 1), `hdfs:///fraud/features/…` (Member 2). Register external Hive tables `fraud_detection.paysim_features` and `fraud_detection.ieee_features` over the feature paths (DDL is Member 2's deliverable; Member 1's two tables should be documented the same way).

**Companion files shipped with this report:** `paysim_schema.csv`, `ieee_schema.csv` (all 434 columns with treatment), `ieee_missingness_groups.csv`, `v_redundancy_r098.csv`, `near_constant_minority.csv`, and the scripts `run_all.sh` (one command), `audit_table_level_checks.py`, `profile_ieee.py`, `derive_ieee_tables.py`, `v_redundancy.py`, `build_ieee_schema.py`, `build_paysim_schema.py`. **Tested end-to-end in the audit environment:** a fresh run of `run_all.sh` regenerated all five CSVs byte-for-byte equal in content to the ones used in this report. They need `duckdb`, `pyarrow`, `pandas`, `numpy`; no Spark; ≈ 1–2 min on 1 CPU / 3 GB RAM.

---

## 11. PART 11 — Integration plan

### 11.1 Member-to-member interfaces

| Hand-off | Format | Schema / contract | Location | What the receiver can rely on |
|---|---|---|---|---|
| **Member 1 → Member 2** | Parquet folders (Snappy, Spark 4.0.4) | Part 3 (12 / 434 columns); `contracts/*_input_schema.json` (to be generated from the CSV schemas) | `data/processed_data/` (HDFS: `/fraud/processed_data/`) | 6,362,620 and 590,540 rows; 0 nulls (PaySim); unique `TransactionID`; no dedupe/`dropna` needed |
| **Member 2 → Member 3 (Kafka + Structured Streaming)** | (a) event schemas (JSON), (b) **online feature spec** (`manifest.json`, `online_computable = Y`), (c) train-fit **encoders** (Parquet/JSON), (d) replay files | Event = raw columns (PaySim 11 + `txn_id`; IEEE selected raw columns + `TransactionID`); features re-computed statefully by the same definitions | `contracts/`, `data/features/encoders/` | Same feature definitions and **same time semantics** ("strictly earlier; PaySim as of end of previous `step`"); Tier-B features flagged; which state is bounded (24 h / 168 h) vs unbounded; synthetic `event_time` mapping is a config, not real time |
| **Member 2 → Member 4 (ML)** | Feature Parquet folders + manifest | Part 9 | `data/features/…` | `split` already assigned; keys/target/bookkeeping columns identified; NULLs preserved; medians in `encoders/`; class imbalance printed in `validation/feature_report.json` |
| **Member 2 → Member 5 (PostgreSQL / FastAPI / Power BI)** | Keys + selected feature columns as Parquet/CSV extracts; Hive external tables | `txn_id` / `TransactionID` = primary keys; `nameOrig`/`nameDest`; `split`; `type` / `ProductCD` for dashboard slices | `data/features/…`, Hive `fraud_detection.*_features` | Stable, unique keys; documented column meanings; reference-only columns flagged |
| **Member 4 → Member 3/5** | Model artifact + scored output (`txn_id`, `score`, `label`) | Owned by Member 4 | – | Must consume **exactly** the manifest-listed feature columns in the manifest order |

### 11.2 Reproducing the work on another laptop

1. Install **Java 17 or 21**, **Python ≥ 3.10**, then `pip install -r environment/requirements.txt` (pin `pyspark==4.0.x`, `pyarrow`, `pandas`, `pyyaml`, `pytest`, plus `duckdb` for audits). Confirm with `java -version`, `python -c "import pyspark; print(pyspark.__version__)"`.
2. Obtain the data **outside Git**: copy Member 1's two Parquet folders into `data/processed_data/` (delete `__MACOSX/` and `*.crc` if unzipping on macOS/Linux) or point `configs/paths.yaml` to HDFS.
3. `make validate` → runs `00_validate_inputs` and compares with the contract (row counts 6,362,620 / 590,540, dtypes, uniqueness).
4. `make paysim && make ieee` → writes `data/features/…` and `validation/…`.
5. `make test` → unit tests on the small committed fixtures (window semantics, no self/future leakage).
6. Optional: `audit/run_all.sh data/processed_data/paysim_clean.parquet data/processed_data/ieee_clean.parquet output/audit` re-verifies this audit's key numbers and regenerates every CSV.

**Configuration that must be explicit (in YAML, not hard-coded):** input/output paths (local vs HDFS), Spark master/memory, `shuffle_partitions`, split cut points and gap, `label_lag`, window lengths, rare-level threshold, random seed, synthetic time origin for streaming, Spark/Java/Python versions.

### 11.3 GitHub policy

| **Store in GitHub** | **Do NOT store in GitHub** |
|---|---|
| All code (`spark/`, `audit/`), configs (YAML), `requirements.txt`, `Dockerfile`, `Makefile` | The Parquet datasets (PaySim = 268 MB, and single part files are 72–73 MB, close to GitHub's 100 MB per-file limit) — use HDFS / shared drive / release asset / Git LFS |
| Docs (`docs/`), this report, feature spec, runbook | Raw data (`paysim.csv`, `train_transaction.csv`, `train_identity.csv`) — large, and the IEEE-CIS data is Kaggle competition data with its own redistribution terms |
| Contracts (`*_schema.json`, manifests), small `encoders/` JSON if ≤ a few MB | `__MACOSX/`, `.DS_Store`, `*.crc`, `_SUCCESS`, `.part-*.crc` |
| Tiny test fixtures (a few hundred rows), the CSV schemas/profiles from this audit (≈ 100 KB) | Model artifacts, checkpoints (Kafka/Spark streaming), `metastore_db/`, `derby.log`, `spark-warehouse/`, virtualenvs, `.env`, credentials |
| `.gitignore` covering all of the right column | Full feature Parquet outputs (regenerate with `make`) |

---

## 12. PART 12 — Final executive summary

### MEMBER 1 COMPLETION STATUS

- **Data: complete and accurate.** `paysim_clean.parquet` (6,362,620 × 12) and `ieee_clean.parquet` (590,540 × 434) exist, are internally consistent, and **every claim that can be verified from the files is true**: row counts, 0 nulls (PaySim; IEEE key columns), no duplicates (PaySim full-row; IEEE `TransactionID`), no negative amounts, `ProductCD` ∈ {C, W, R, H, S}, identity coverage 144,233 rows, `step_hour = step % 24`.
- **Engineering handoff: incomplete.** The ZIP has no cleaning code, no schemas/DDL, no Hive artifacts, no raw data, no tests or environment file; the documentation refers to folders and Hive tables that were not delivered (**FAIL: not verifiable**), and omits critical dataset properties (Part 2.4).
- **Not a data-quality problem but must be known:** the PaySim simulator artifacts (single-use senders, constant fraud per step, drain-the-account separability, 16-row `isFlaggedFraud`) and the structured, identity-coupled IEEE sparsity.

### MEMBER 2 INPUTS

- `paysim_clean.parquet/` — 4 Snappy parts, 268 MB, Spark 4.0.4, no key, no timestamp beyond `step`.
- `ieee_clean.parquet/` — 4 Snappy parts, 86.5 MB, transaction ⟕ identity, labelled training data only (days 1–183).
- `data_documentation.md` (27 lines; numeric claims correct).
- From this audit: `paysim_schema.csv`, `ieee_schema.csv` (all 434 columns with default treatment), `ieee_missingness_groups.csv`, `v_redundancy_r098.csv`, `near_constant_minority.csv`, four tested audit scripts.
- **Missing and to be requested from Member 1:** cleaning code, Hive DDL/warehouse paths, raw file names, Spark/Java versions used.

### MEMBER 2 RESPONSIBILITIES

Validate inputs against contracts; mint PaySim `txn_id`; assign time-based splits; build **PaySim** row-wise, tiered (A/B) and **destination-side** behavioural features; build **IEEE-CIS** time, amount-fingerprint, block-missingness, identity-completeness, V/C/D/M aggregate, email/device parsing, frequency-encoded and **card-proxy** behavioural features; fit encoders on train only and save them; validate features (nulls, ranges, per-split prevalence, leakage screens, offline-vs-online parity); publish Parquet + manifests; document the feature spec and make everything reproducible. **Not** to re-clean data, train models, or build Kafka/PostgreSQL/API layers.

### MEMBER 2 OUTPUTS

- `features/paysim_features.parquet/` (1 row per transaction; keys `txn_id`, `nameOrig`, `nameDest`; target `isFraud`; `split`; Tier-A and Tier-B features per Part 9.1).
- `features/ieee_features.parquet/` and `ieee_features_experimental.parquet/` (keys `TransactionID`; target `isFraud`; `split`; retained raw columns per Part 4.3; ≈ 100+ engineered columns per Part 9.2).
- `manifest.json` per dataset, `encoders/` (train-fit maps/medians), `validation/feature_report.json`, `contracts/*.json`.
- `spark/` code + configs + tests, `docs/member2/feature_spec.md`, `runbook.md`, Hive external table DDL for the feature tables.

### RISKS / ISSUES

1. **PaySim is a simulator with strong artifacts** — near-perfect separability from balance arithmetic; fraud constant per step; late-simulation prevalence up to 17× the training prevalence. Present metrics prevalence-aware and with/without Tier B; never claim real-world behavioural insight from `step_hour` or step volume.
2. **Sender-side behavioural features are not viable** in PaySim (99.85 % single-use `nameOrig`). Don't build them.
3. **Label / time leakage** in rolling, historical and aggregate features; use strictly-earlier frames, train-only fits and a `label_lag`; avoid `groupBy`-and-join for behaviour.
4. **IEEE has no customer ID** — card-based proxy keys can mis-group; `D1`-based UIDs are unverified.
5. **Structured sparsity:** never `dropna` (0 rows survive); use block-level indicators; near-constant ≠ useless (V108–V125, C3).
6. **Unaudited vendor features** (`C*`, `D*`, `V*`) may hide time leakage; run the random-vs-time split check.
7. **Ordering is not guaranteed** (PaySim step-sorted by accident; IEEE 4 interleaved runs); ties within `step` (290,449 rows) and `TransactionDT` (33,932 rows).
8. **Train/serve skew** between offline "strictly earlier step" features and Member 3's streaming state; agree the semantics now.
9. **Member 1's work is not reproducible** from the ZIP; Hive tables unverified.
10. **Environment/versioning:** Spark 4.x + Java 17/21; align the whole team. GitHub hygiene (no data, no `__MACOSX`/`.crc`).
11. **Audit limits:** no Spark was executed here; IEEE column meanings come from the public description; V-redundancy and AUCs are sample/one-feature diagnostics, not model conclusions.

### NEXT 5 ACTIONS

1. **Secure and verify the inputs (today).** Copy the two Parquet folders to `data/processed_data/` (drop `__MACOSX` and `*.crc`), run `audit_table_level_checks.py`, and confirm 6,362,620 / 590,540 rows. Message Member 1 to commit their cleaning code, Hive DDL, HDFS/warehouse paths and Spark/Java versions.
2. **Create the repo skeleton and environment.** Pin Java 17/21 + PySpark 4.0.x, create `spark/`, `contracts/`, `configs/`, `.gitignore`; generate `contracts/*_input_schema.json` from the two schema CSVs and write `00_validate_inputs.py`.
3. **Freeze the decisions with Members 3 and 4** in `configs/split.yaml` and `docs/member2/leakage_and_split_policy.md`: time-based split cuts (IEEE ≈ day 130; PaySim steps 500/600), `label_lag`, Tier-A/Tier-B policy, "strictly earlier step" semantics, and the output contract in Part 9.
4. **Build PaySim first (small, high-value):** `txn_id`, row-wise features and Tier-B separation (`10_rowwise`), then destination windows (`30_dest_windows`) — with unit tests proving that shuffled input gives identical output and that no row sees itself or the future.
5. **Build IEEE P0/P1, ship a first sample, then P2:** row-wise + identity/missingness indicators + train-fit encoders → write `ieee_features` + manifest, hand a 1 % sample to Member 4 for early integration → then card-entity windows (`60_card_windows`) and the random-vs-time split check.
