# Member 3 Roadmap: Kafka + Spark Structured Streaming
## Autonomous execution plan: PaySim closeout, then IEEE-CIS, then handoff
 
> **How to use this file:** Give it to the agent with one instruction: *"Execute `roadmap.md` from the first unchecked chunk. Follow the Global Rules. Do not stop between chunks unless a HUMAN-STOP condition is met."*
> The agent works chunk by chunk. A chunk is finished only when **every gate check passes with raw evidence**. Then it moves to the next chunk automatically.
 
---
 
## 0. Project Context (read first, every session)
 
- **Project:** Real-Time Financial Fraud Intelligence System (5-member Big Data project). I am **Member 3**: the streaming layer between Member 2 (PySpark batch features) and Member 4 (ML).
- **Repo:** https://github.com/shreyy92/Real-Time-Financial-Fraud-Intelligence, branch `member3-streaming`
- **Environment:** Windows, Docker Desktop, Java 17, Python 3.12, PySpark 4.1.1 in `venv/` at repo root, Kafka `apache/kafka:3.9.0` (KRaft) at `localhost:9092`.
- **Source of truth (read-only, never modify):**
  - `member2_spark/docs/OUTPUT_CONTRACT.md`
  - `member2_spark/docs/FEATURE_DICTIONARY.md`
  - `member2_spark/config/feature_config.yaml`
  - `member2_spark/src/**` (the actual implementation; **code wins over docs if they disagree, but report the disagreement**)
  - `member_audit1/schemas/paysim_schema.csv`, `ieee_schema.csv`
- **Ground truth data (gitignored, local):** `data/processed/{paysim,ieee}_clean.parquet`, `data/features/{paysim,ieee}_features.parquet`
- **Existing Member 3 code in `member3_streaming/`:** `docker-compose.yml`, `create_topics.py`, `smoke_test.py`, `smoke_producer.py`, `producer_paysim.py`, `consumer_paysim.py`, `verify_stateful_paysim.py`, `send_bad_messages.py`, `README.md`
### Status at start of this roadmap
 
| Item | Status |
|---|---|
| Phase 0: Kafka + PySpark environment | Done, verified |
| PaySim producer | Done, pushed (aeb27ea) |
| PaySim stateless features | Done, exact match to Member 2 |
| PaySim stateful features (`applyInPandasWithState`, key `nameDest`) | Done, zero mismatches on 20k rows and on 300 long-span keys |
| PaySim output schema (41 cols, Tier B removed), output topic, DLQ topic | Done |
| PaySim crash recovery | Passed, but kill landed on a batch boundary, and RocksDB was only configured, not proven |
| PaySim guards, contract, pytest suite, README | **Open** (Chunks 1 to 5) |
| IEEE-CIS track | **Not started** (Chunks 6 to 11) |
| Handoff, final integration | **Open** (Chunks 5 and 12) |
 
---
 
## 1. Global Rules (apply to every chunk)
 
### 1.1 Git rules
1. Work **only** on branch `member3-streaming`. Never push to `main`. Never force-push. Never rebase published commits.
2. **Never modify** anything under `member2_spark/` or `member_audit1/`. Read-only.
3. Commit only after a chunk's gate passes. Commit message prefix: `Chunk N:` (for example `Chunk 3: RocksDB crash tests`).
4. Push to `origin member3-streaming` after each passing chunk. Paste or save the push output.
5. Never commit data files, checkpoints, `venv/`, or large outputs. Check `.gitignore` before `git add`.
### 1.2 Honesty and evidence rules
1. **Real Kafka and real Spark only.** No mocks, no simulated loops, no in-memory fakes. Every chunk report states: `Real Kafka: yes/no`, `Real Spark Structured Streaming query: yes/no`. A "no" on either fails the gate unless the check is a pure unit test with no I/O.
2. **Raw, untruncated output.** Save the full command output into the chunk report. No `...`, no summarising in place of output.
3. **A claim without raw output is unverified** and counts as a failed gate.
4. **No over-claiming.** Do not write "perfect", "guaranteed", "fully verified" unless the exact test proving it ran and its output is attached. State limits plainly (example: "kill landed on a batch boundary, so mid-batch duplicates are untested").
5. **Do not skip silently.** Anything not done goes under `NOT DONE` in the chunk report with the reason.
6. **Never weaken a test to make it pass.** If a test fails, fix the code. Changing a tolerance, shrinking the sample, or deleting an assertion is not a fix.
7. **Parity means numeric parity with Member 2's batch Parquet**, joined on the key given in the chunk (`txn_id` for PaySim, `TransactionID` for IEEE). Tolerance `1e-6` for floats, exact match for ints, strings and flags, and null must equal null.
### 1.3 Autonomy policy (the core of this roadmap)
The agent **continues without asking** through everything except HUMAN-STOP conditions (section 1.4). Specifically:
 
- **When a gate fails:** diagnose, fix, rerun. Allowed up to **3 fix attempts per failing gate item**. Log each attempt in the chunk report (what failed, root cause, change made).
- **When a design choice is open** (naming, key choice, buffer layout, sink options): pick the option that best matches Member 2's conventions and the existing PaySim pattern. Record it in `member3_streaming/DECISIONS.md` (date, chunk, choice, alternatives, reason). Do **not** ask.
- **When docs and code disagree:** follow the code, log it in `DECISIONS.md` and in the chunk report under `Surprises`.
- **When the environment misbehaves** (Kafka container down, port busy, stale topics, stale checkpoints, temp-dir delete errors on Windows): run the standard reset (section 1.6), retry. Windows `Failed to delete ... snappy jar` errors on shutdown are harmless; ignore them.
- **Do not pause to report between chunks.** Write the report file, update `PROGRESS.md`, commit, push, start the next chunk.
### 1.4 HUMAN-STOP conditions (the only reasons to pause)
Stop immediately, write `member3_streaming/reports/HUMAN_STOP.md` (what happened, what was tried, exactly what is needed from me), and wait. Stop **only** for:
 
1. **A fix would require modifying `member2_spark/` or `member_audit1/`**, or a parity mismatch remains after 3 fix attempts and the root cause is demonstrably in Member 2's logic or data.
2. **Environment unrecoverable:** Docker or Kafka cannot be brought up after the standard reset **and** a full container recreate, or Spark cannot start, or disk space is below 5 GB.
3. **A destructive action outside `member3_streaming/` would be required** (deleting `data/`, rewriting git history, force-push, touching `main`).
4. **Credentials, secrets or paid external services** are needed.
5. **Required input is missing and cannot be reconstructed** (for example `ieee_features.parquet` is absent or corrupt).
6. **A gate item has failed 3 fix attempts** and no further hypothesis remains. Report the evidence, do not guess.
Everything else (including "Member 4 has not replied", "design choice unclear", "test is slow") is **not** a stop condition.
 
### 1.5 Chunk lifecycle (every chunk follows this)
```
1. DISCOVER   read the relevant Member 2 code/docs; write findings to the chunk report
2. BUILD      implement only what the chunk lists; nothing extra
3. VERIFY     run every gate check; save raw output
4. FIX LOOP   on failure: diagnose, fix, rerun (max 3 attempts per item)
5. RECORD     write reports/CNN_report.md; update PROGRESS.md; update DECISIONS.md if needed
6. COMMIT     only if all gate checks passed; push
7. NEXT       start the next chunk immediately
```
 
### 1.6 Standard environment reset
Run before any "clean" test:
1. Stop any running streaming job (`taskkill /F /IM python.exe` on Windows if needed).
2. Delete **and recreate** the relevant Kafka topics (1 partition each) and **delete the relevant checkpoint directory**. The input topic accumulates: re-running a producer appends duplicates and out-of-order data, so never rerun a producer onto an old topic.
3. Verify the topics are empty (`kafka-get-offsets.sh` shows 0) before producing.
4. Kafka CLI inside Docker: `docker exec <container> /opt/kafka/bin/<tool>.sh --bootstrap-server localhost:9092 ...`
### 1.7 Report format (`member3_streaming/reports/CNN_report.md`)
Every chunk report contains, in this order:
1. **Chunk, start time, end time**
2. **Real Kafka: yes/no. Real Spark query: yes/no.**
3. **Discovery findings** (with quoted code or doc lines)
4. **Files created or changed**
5. **Gate results table:** `check | PASS/FAIL | evidence pointer`
6. **Raw output** of every command, untruncated
7. **Fix attempts** (if any)
8. **Surprises / limitations**
9. **NOT DONE**
10. **git output** (`status`, `log -3 --oneline`, push result)
### 1.8 `PROGRESS.md` ledger
Keep `member3_streaming/PROGRESS.md` as a table: `chunk | status (pending / in-progress / passed / blocked) | commit hash | report path | date`. Update it after every chunk. If a session is interrupted, **resume from the first chunk that is not `passed`**; do not redo passed chunks unless a later chunk exposes a regression.
 
---
 
## 2. Roadmap
 
---
 
### CHUNK 1: Git sync and PaySim guard hardening
**Goal:** Make sure all PaySim work is saved, and close the remaining gaps in input validation and the late-event guard.
 
**Tasks**
1. Run `git status` and `git log -8 --oneline`. If Phase 3 or hardening work is uncommitted or unpushed, commit and push it first.
2. **Late-event guard fix:** the guard must compare against `max(committed last step, pending step)`, not only the committed step.
3. **Type validation:** a `type` outside `{TRANSFER, CASH_OUT, PAYMENT, CASH_IN, DEBIT}` (for example `TELEPORT`) goes to the DLQ with reason code `BAD_TYPE`.
4. **Distinct code for wrong JSON type:** a field present but with the wrong JSON type (for example `step` as a string) goes to the DLQ as `BAD_FIELD_TYPE`. Update the consumer, `send_bad_messages.py` and the README so reason codes are consistent.
5. **`split` column decision:** read `OUTPUT_CONTRACT.md` for the `split` column (train steps 1-500, valid 501-600, test 601-743). Quote the lines. If Member 2's Tier A output includes it, add it to the streaming output (derived from `step`) and re-verify parity. Log the decision in `DECISIONS.md`.
**Gate (all must PASS)**
| # | Check | Pass condition |
|---|---|---|
| 1.1 | `git status` clean after commit; `git log` shows prior Phase 3 work on `origin/member3-streaming` | Raw output attached |
| 1.2 | Fresh key, committed step 3, pending step 5: send step 4, then 5, then 6 | Step 4 → DLQ `LATE_EVENT`. Step 5 → accepted (same-step tie, features equal to what batch gives). Step 6 → accepted and pending folds into committed. Raw output rows and DLQ records attached |
| 1.3 | Send `type="TELEPORT"` | One DLQ record, `reason_code = BAD_TYPE` |
| 1.4 | Send `step` as a string | One DLQ record, `reason_code = BAD_FIELD_TYPE` |
| 1.5 | All existing DLQ codes re-run (`MALFORMED_JSON`, `MISSING_FIELD`, `NEGATIVE_AMOUNT`, `INVALID_STEP`, `INVALID_LABEL`, `LATE_EVENT`, plus the two new ones) | Exactly one DLQ record per bad message, correct code each, **query still active afterwards**, no valid records lost |
| 1.6 | Re-run parity (20k rows, 40 micro-batches) after the changes | 0 mismatches across every output column |
| 1.7 | `split` decision | Contract lines quoted, decision logged, parity re-verified if column added |
 
---
 
### CHUNK 2: RocksDB verified and mid-batch crash recovery (PaySim)
**Goal:** Prove the state store actually used is RocksDB, and prove recovery when the kill lands **inside** a micro-batch.
 
**Tasks**
1. Establish which state store provider the earlier crash test used. Prove it from the Spark log line or the effective config dump, not from the code alone.
2. Make sure the RocksDB provider is active in the consumer. Confirm in the logs of the next run.
3. Run the crash test:
   - Standard reset (topics and checkpoint).
   - Produce 20,000 rows (`--limit 20000 --delay 0`).
   - Start the query (`maxOffsetsPerTrigger=500`).
   - Hard-kill (`taskkill /F`) at an output count that is **not a multiple of 500**. To make a mid-batch kill reliable, poll the output topic end offset and kill when `count % 500 != 0`; if needed, add a small configurable per-batch delay for the test only.
   - Restart from the same checkpoint and let it finish.
4. Analyse the output topic: total records, distinct `txn_id`, duplicates, missing, per-column mismatches vs `data/features/paysim_features.parquet`.
**Gate**
| # | Check | Pass condition |
|---|---|---|
| 2.1 | Provider proof | Log/config line showing `RocksDBStateStoreProvider` active during the crash test run |
| 2.2 | Kill position | Reported output count at kill time, and `count % 500 != 0`. If it cannot be achieved in 3 attempts, record "mid-batch kill not achieved" as a **limitation** (not a stop) and report the closest achieved |
| 2.3 | Completeness | Distinct `txn_id` == 20,000, missing == 0 |
| 2.4 | Duplicates | Duplicates reported honestly. Any duplicates must be **byte-identical** to their original. Non-identical duplicates = FAIL |
| 2.5 | Parity after recovery | After dedupe on `txn_id`: 0 mismatches vs batch, all columns |
| 2.6 | State survived | State-dependent columns (`dest_prior_txn_count`, `dest_prior_amount_sum`, `dest_txn_count_prev_*`) match batch for rows processed **after** the restart |
 
---
 
### CHUNK 3: Crash test on long-span keys (history buffer restore)
**Goal:** Prove the 168-step history buffer survives a restore from the checkpoint.
 
**Tasks**
1. Take the 300-key long-span subset (keys with activity spanning more than 168 steps; 3,979 rows).
2. Run through the real `readStream` query with RocksDB, kill mid-run (not on a batch boundary where possible), restart from the same checkpoint, finish.
3. Compare all stateful columns to batch.
**Gate**
| # | Check | Pass condition |
|---|---|---|
| 3.1 | Subset size | 300 keys, 3,979 rows (or the new exact counts, reported) |
| 3.2 | Window expiry exercised | Rows where `dest_txn_count_prev_24h != dest_prior_txn_count` > 0 **and** `dest_txn_count_prev_168h != dest_prior_txn_count` > 0 (report both counts) |
| 3.3 | Kill happened mid-run | Output count at kill time reported, between 1 and N-1 |
| 3.4 | Parity | 0 mismatches on all stateful columns after dedupe |
| 3.5 | No duplicates beyond byte-identical | Same rule as 2.4 |
 
---
 
### CHUNK 4: PaySim output contract, pytest suite, README
**Goal:** Everything Member 4 needs, plus automated tests. **Runs only after Chunks 1 to 3 pass.**
 
**Tasks**
1. Write `member3_streaming/MEMBER3_OUTPUT_CONTRACT.md` covering:
   - topic name, partitions, message key, encoding
   - **full column list with dtype and nullability** (all columns, from the real output schema)
   - null semantics (explicit JSON `null`, every field always present)
   - ordering guarantees and what is **not** guaranteed
   - **at-least-once delivery; consumers must dedupe on `txn_id`**
   - DLQ topic, record schema (`raw_value`, `reason_code`, `ingested_at`), and **every reason code with its meaning**
   - `feature_version`
   - Tier B exclusion (`newbalanceOrig`, `newbalanceDest`) and why
   - `isFraud` is a label only
   - how to replay from scratch
   - how to run the producer, consumer and tests
2. Write the pytest suite in `member3_streaming/tests/`:
   - Kafka connectivity (real broker)
   - producer output (JSON validity, field set, ordering)
   - schema parsing
   - every DLQ reason code
   - late-event guard (including the pending-step case)
   - stateful parity vs batch on a sample
   - checkpoint recovery
   - output schema conformance against the contract (column names and dtypes)
   - Tests needing Kafka use the real broker and create/clean their own topics with unique names.
3. Finish `member3_streaming/README.md`: setup, run order, architecture diagram, reset procedure, troubleshooting.
**Gate**
| # | Check | Pass condition |
|---|---|---|
| 4.1 | Contract completeness | Every item in the list above is present; the column list matches the **actual** output schema (programmatic check: contract columns == output columns) |
| 4.2 | `pytest -v` full run | All tests pass; raw output attached; test count reported; **no skipped tests without a stated reason** |
| 4.3 | Tests are real | At least one test per area listed above, none using mocks for Kafka or Spark |
| 4.4 | Regression | Parity re-run still gives 0 mismatches after any test-driven code changes |
 
---
 
### CHUNK 5: Member 4 handoff (non-blocking)
**Goal:** Make the PaySim output consumable, and tell Member 4. **This chunk never blocks later chunks.**
 
**Tasks**
1. Write `member3_streaming/HANDOFF_MEMBER4.md` (short): output topic name, contract path, how to start the stream, how to consume (a 15-line example consumer in Python that dedupes on `txn_id`), known limitations.
2. Write a ready-to-send message to Member 4 into the same file (topic, contract path, what is guaranteed, what is not, the question "do you want to consume this Kafka topic, or do you need a different sink?").
3. Default decision if no reply: **Kafka topic is the sink**. Log in `DECISIONS.md`.
4. Run the 15-line example consumer against the live output topic to prove it works.
**Gate**
| # | Check | Pass condition |
|---|---|---|
| 5.1 | Handoff file exists with message text | File present |
| 5.2 | Example consumer runs | Raw output shows it reading real records from `financial_features_paysim` and deduping |
| 5.3 | Default decision logged | Entry in `DECISIONS.md` |
 
> No reply from Member 4 is **not** a stop condition. If they later ask for a different sink, that becomes a new chunk.
 
---
 
### CHUNK 6: IEEE discovery (read-only)
**Goal:** Learn Member 2's exact IEEE logic before writing anything. **No code is written in this chunk.**
 
**Tasks:** read `OUTPUT_CONTRACT.md` and `FEATURE_DICTIONARY.md` (IEEE sections), `feature_config.yaml`, Member 2's IEEE source under `member2_spark/src/`, and `member_audit1/schemas/ieee_schema.csv`. Quote exact code or text for each finding.
 
Report all of the following in `reports/C06_report.md`:
- **a)** How `card_proxy_id` and `card_addr_proxy_id` are built: exact column list, separator, null handling (null vs `""` vs `"NA"`), casts, hash function. **Paste the code lines.**
- **b)** The input file Member 2 reads for IEEE (path, clean parquet or raw CSV) and whether `ieee_clean.parquet` has exactly the raw schema columns (list any differences).
- **c)** The ordering column, its units (seconds), and the tie-break rule.
- **d)** Which IEEE columns are Tier B (leakage) and must not appear in streaming output.
- **e)** The full IEEE output column list, grouped as raw / stateless / stateful / label, with total count.
- **f)** How `split` is derived and its boundaries for IEEE.
- **g)** The dtype of every raw column and the real raw column count.
- **h)** Exact definitions for: `amt_log1p`, `amt_cents`, `amt_decimals_gt2_flag`, `amt_is_whole_flag`, `hour_of_day`, `hour_sin/cos`, `day_index`, `day_of_week_rel`, `history_window_complete_flag`, identity/missingness indicators, `m_true_cnt/m_false_cnt/m_null_cnt`, `c_sum_log1p/c_nonzero_cnt/c_max`, `d_nonnull_cnt/d_min/d_max`.
- **i)** Exact stateful definitions for: `card_prior_txn_count`, `card_first_seen_flag`, `card_prior_amt_sum/mean/std/max`, `amt_zscore_vs_card`, `card_secs_since_last_txn`, `card_txn_count_1h/24h/7d`. Including the window frame (`rangeBetween`) and tie semantics, null and small-sample rules, and the z-score conditions.
- **j)** Dataset facts: distinct `card_proxy_id` count, maximum events per card in any 7-day window, TransactionDT span in days.
**Gate**
| # | Check | Pass condition |
|---|---|---|
| 6.1 | Items a) to j) all answered | Each has quoted code or numbers; paraphrase alone = FAIL |
| 6.2 | Doc vs code disagreements | Listed explicitly (even if the list is "none found") |
| 6.3 | No code files created | `git status` shows only the report |
 
---
 
### CHUNK 7: IEEE producer
**Goal:** Replay real IEEE rows as a live JSON stream.
 
**Tasks** (mirror `producer_paysim.py`)
1. Create `member3_streaming/producer_ieee.py`. Source: the file identified in Chunk 6(b).
2. Stable sort by `TransactionDT` using the tie-break from Chunk 6(c).
3. One JSON event per row to topic `financial_transactions_ieee`. Message key: a stable identifier you can justify (log the choice).
4. `NaN`/`NaT`/`pd.NA` → JSON `null`. Use `allow_nan=False` so a NaN can never leak. Convert numpy types (int64, float32, bool) to native Python.
5. Args: `--limit` (default 50000), `--delay` (default 0.05). Progress line every 5,000 events.
6. Keep `isFraud` as a raw field, handled the same way as in the PaySim producer.
**Gate**
| # | Check | Pass condition |
|---|---|---|
| 7.1 | Clean topic | Standard reset done, topic offset 0 before producing |
| 7.2 | Run `--limit 2000 --delay 0` | Exactly 2,000 messages in the topic (offset check) |
| 7.3 | CLI sample | First 3 messages consumed via `kafka-console-consumer.sh`, pasted raw and in full |
| 7.4 | Python consumer check on all 2,000 | `json.loads` failures = 0; `TransactionDT` non-decreasing = True; field count identical across all messages; JSON null count for a sample message reported with column names |
| 7.5 | Order vs parquet | First 2,000 `TransactionID`s match the parquet in `TransactionDT` order exactly: 0 mismatches |
| 7.6 | Types | No NaN strings (`"NaN"`, `"nan"`) anywhere in the payloads |
 
---
 
### CHUNK 8: IEEE stateless consumer and parity
**Goal:** All stateless IEEE features computed in streaming, matching Member 2 exactly.
 
**Tasks**
1. Create `member3_streaming/consumer_ieee.py` (mirror `consumer_paysim.py` structure).
2. Explicit `StructType` schema for `from_json` built from the raw IEEE schema (Chunk 6(g)). Use the exact dtypes needed for parity (as with PaySim, where `DoubleType` was required).
3. Compute every stateless feature from Chunk 6(h) with **Member 2's formulas verbatim**, including `card_proxy_id` and `card_addr_proxy_id` hashing with the exact separator and null handling found in Chunk 6(a).
4. Validation and DLQ (topic `financial_transactions_ieee_dlq`, fields `raw_value`, `reason_code`, `ingested_at`). Reason codes: `MALFORMED_JSON`, `MISSING_FIELD`, `BAD_FIELD_TYPE`, `NEGATIVE_AMOUNT`, `INVALID_LABEL`, `INVALID_TRANSACTIONDT` (negative or absurd), `LATE_EVENT` (added in Chunk 9). Use only fields that are actually required by the IEEE contract for the required-field check; optional columns being null is **not** an error.
5. Write `verify_stateless_ieee.py`: reads from the real Kafka topic through a real `readStream` query, joins to `data/features/ieee_features.parquet` on `TransactionID`.
**Gate**
| # | Check | Pass condition |
|---|---|---|
| 8.1 | Real streaming query | Report shows `readStream.format("kafka")` → features → sink, Real Kafka: yes |
| 8.2 | Parity on first 20,000 rows (by `TransactionDT`) | Every stateless column: 0 mismatches. Per-column table printed in full |
| 8.3 | Hash columns | `card_proxy_id` and `card_addr_proxy_id`: 0 mismatches (strict equality) |
| 8.4 | Null equivalence | Null ↔ null counted as equal; null ↔ non-null counted as mismatch |
| 8.5 | DLQ | One bad message per reason code → one DLQ record each with the right code; query stays active; valid records unaffected |
| 8.6 | Schema | Output column set == Member 2's stateless set from Chunk 6(e) (programmatic diff, reported) |
 
---
 
### CHUNK 9: IEEE stateful features (the hard chunk)
**Goal:** Reproduce Member 2's card-level window features in streaming, keyed on `card_proxy_id`, at three window sizes.
 
**Tasks**
1. Use `applyInPandasWithState` keyed on `card_proxy_id`, **reusing the PaySim pattern**: committed aggregates + pending-same-timestamp aggregates, bounded history buffer, `NoTimeout`, RocksDB provider.
2. **Tie semantics:** replicate exactly what Chunk 6(i) found for `rangeBetween` on `TransactionDT`. Rows with the same `TransactionDT` must follow the same inclusion rule as batch.
3. History buffer: `(TransactionDT, amount)` tuples for the last **604,800 s**; prune anything older. Compute `card_txn_count_1h` (3600 s), `card_txn_count_24h` (86,400 s), `card_txn_count_7d` (604,800 s) from the buffer.
4. Unbounded aggregates (`card_prior_txn_count`, `_sum`, `_max`, mean, std via sum-of-squares) in committed state, std and z-score null rules exactly as in batch.
5. Late-event guard: `TransactionDT < max(committed last, pending)` for the key → DLQ `LATE_EVENT`.
6. State-size note: report the Chunk 6(j) numbers and the estimated per-key state size (**in `DECISIONS.md`**).
7. Write `verify_stateful_ieee.py`.
**Gate**
| # | Check | Pass condition |
|---|---|---|
| 9.1 | Parity, 20,000 rows, many micro-batches (`maxOffsetsPerTrigger=500` → 40 batches) | All stateful columns: 0 mismatches vs `ieee_features.parquet` |
| 9.2 | Window expiry exercised | On a subset of cards spanning more than 7 days (~300 keys, size reported): rows where `card_txn_count_1h != card_prior_txn_count` > 0, `card_txn_count_24h != card_prior_txn_count` > 0, `card_txn_count_7d != card_prior_txn_count` > 0 (all three counts reported); parity 0 mismatches |
| 9.3 | Same-timestamp ties | At least one test with multiple rows sharing a `TransactionDT` for the same card, **across a micro-batch boundary**: parity holds |
| 9.4 | Late-event guard | Committed T=300, pending T=500: T=400 → `LATE_EVENT`; T=500 accepted; T=600 accepted |
| 9.5 | Null semantics | `card_prior_amt_mean/std/max`, `amt_zscore_vs_card`, `card_secs_since_last_txn` null exactly when batch is null: 0 mismatches including null-vs-non-null |
| 9.6 | No negative `card_secs_since_last_txn` | Count of negative values == 0 in the output |
 
---
 
### CHUNK 10: IEEE output topic, checkpoints and crash recovery
**Goal:** Production-shaped IEEE pipeline with fault tolerance proven.
 
**Tasks**
1. Output topic `financial_features_ieee`. Output schema = Member 2's Tier A IEEE columns + `feature_version` (+ `split` if the contract includes it, same decision rule as Chunk 1). Tier B columns removed. `to_json` with `ignoreNullFields=false`.
2. Separate checkpoint directory for IEEE (`member3_streaming/checkpoints/ieee_*`).
3. RocksDB confirmed active (log proof).
4. Crash test A (20,000 rows): standard reset, produce, run, kill at `count % 500 != 0`, restart, finish.
5. Crash test B (long-span ~300 card subset): kill mid-run, restart, finish.
6. Analyse both: distinct `TransactionID`, duplicates, missing, per-column mismatches vs batch.
**Gate**
| # | Check | Pass condition |
|---|---|---|
| 10.1 | Output schema | Column set and dtypes match Member 2's Tier A + `feature_version` (programmatic diff); no Tier B column present |
| 10.2 | RocksDB proof | Log/config line in the run |
| 10.3 | Crash A | Kill count reported (mid-batch if achievable, else limitation noted); distinct == input count; missing 0; duplicates byte-identical only; post-dedupe parity 0 mismatches |
| 10.4 | Crash B | Same checks on the long-span subset; window-expiry counts reported again; 0 mismatches |
| 10.5 | Explicit nulls | Every output message contains every field (field-count identical across all messages) |
| 10.6 | Isolation | PaySim pipeline still passes its parity check after IEEE work (no regression in shared code) |
 
---
 
### CHUNK 11: IEEE contract and pytest suite
**Goal:** Document and lock down IEEE behaviour. Runs after Chunk 10 passes.
 
**Tasks**
1. Extend `MEMBER3_OUTPUT_CONTRACT.md` with an IEEE section (same required content as Chunk 4, including every reason code, dedupe on `TransactionID`, column list with dtypes, and Tier B exclusion).
2. Add IEEE tests to `member3_streaming/tests/`: producer output, schema parsing, each DLQ code, late-event guard, stateless parity, stateful parity (including a window-expiry sample), checkpoint recovery, schema conformance.
3. Update the README with the IEEE run order.
**Gate**
| # | Check | Pass condition |
|---|---|---|
| 11.1 | Contract vs reality | Programmatic check: contract IEEE columns == actual output columns |
| 11.2 | `pytest -v` full run (PaySim + IEEE) | All pass, raw output attached, no unexplained skips |
| 11.3 | Real services | IEEE tests hit the real broker and real Spark, no mocks |
 
---
 
### CHUNK 12: Final integration, README, merge-ready branch
**Goal:** A reviewable, reproducible Member 3 deliverable.
 
**Tasks**
1. **Clean-room reproduction:** from a clean state (reset all topics and checkpoints), follow the README exactly, step by step, for PaySim and then IEEE. Fix any README step that does not work.
2. Run the full `pytest` suite once more.
3. Write `member3_streaming/FINAL_REPORT.md`: what was built, architecture diagram, results table (rows verified, mismatches, crash tests, dedupe stats), limitations, how Member 4 consumes the output.
4. Update the **Definition of Done** checklist below with evidence pointers.
5. `git log` review: commit messages follow the `Chunk N:` convention; no data or checkpoints committed.
6. Prepare the merge request text (title, summary, test evidence). **Do not merge and do not push to main.**
**Gate**
| # | Check | Pass condition |
|---|---|---|
| 12.1 | Clean-room run | README steps work verbatim for both datasets; raw output attached |
| 12.2 | Full pytest | All pass, raw output attached |
| 12.3 | Branch hygiene | `git status` clean, branch pushed, no large or ignored files tracked (`git ls-files` size check) |
| 12.4 | Final report | Exists; every number in it traces to a chunk report |
| 12.5 | Definition of Done | Every item ticked with a pointer, or explicitly marked not done with the reason |
 
---
 
## 3. Definition of Done (project level)
 
- [ ] Kafka configured, topics created (PaySim and IEEE: input, output, DLQ)
- [ ] Producers send real events at a controlled rate (both datasets)
- [ ] Spark Structured Streaming consumes, parses and validates (both datasets)
- [ ] Invalid messages handled safely via DLQ with reason codes; stream never crashes
- [ ] All Tier A stateless features generated and match Member 2's batch output
- [ ] All stateful windowed features generated and match Member 2's batch output (including window expiry)
- [ ] Tier B columns excluded from the streaming output
- [ ] Checkpointing configured; crash recovery proven (mid-batch kill, RocksDB confirmed)
- [ ] Output available to Member 4 with a written contract
- [ ] Pytest suite passes on the real stack
- [ ] README and final report complete
- [ ] All work on `member3-streaming`, pushed, main untouched
---
 
## 4. Time-pressure fallback
 
If time runs short, **finish PaySim completely (Chunks 1 to 5) and make the handoff to Member 4 first.** Then do IEEE in order (6 → 7 → 8 → 9 → 10 → 11 → 12). Stateless IEEE parity (Chunk 8) is a usable partial result if Chunk 9 is not reached. Record the cut-off honestly in `FINAL_REPORT.md` instead of claiming completion.
 
---
 
## 5. Known gotchas
 
- **The input topic accumulates.** Re-running a producer onto an old topic duplicates and disorders data. Always reset topics and checkpoints for clean tests.
- **Spark's Kafka sink is at-least-once.** Duplicates after a crash are possible. The contract requires consumers to dedupe (`txn_id` for PaySim, `TransactionID` for IEEE). Duplicates are acceptable only if byte-identical.
- **Parity tests use step-sorted (or `TransactionDT`-sorted) prefixes.** A random sample will not match batch because history is incomplete.
- **`txn_id` for PaySim** = `sha2(concat_ws('|', step, type, amount, nameOrig, nameDest), 256)` with **no cast** on `amount`; the schema must use `DoubleType` for amounts and balances.
- **Negative `*_since_last_txn` values** mean replayed or out-of-order rows reached the state. The late-event guard exists to prevent this; investigate any occurrence.
- **State size (PaySim):** 2,722,362 distinct `nameDest`, max 101 events in any 168-step window → a few hundred MB for the full run, hence RocksDB.
- **Windows:** use `taskkill /F` for hard-kill tests. Kafka CLI inside Docker lives at `/opt/kafka/bin/`. `Failed to delete ... snappy jar` on shutdown is harmless.
 