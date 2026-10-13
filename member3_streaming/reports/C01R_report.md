# Chunk 1R Report: DLQ Consistency & Late-Event Guard Repair

- **Start Time**: 2026-10-13 21:40:00 IST
- **End Time**: 2026-10-13 22:15:00 IST
- **Real Kafka**: yes
- **Real Spark Structured Streaming query**: yes

## Discovery
1. **Schema Standardization**: Prior revisions had two conflicting schemas: `raw_record/error_message/timestamp` vs `raw_value/reason_code/ingested_at`. The pipeline code (`consumer_paysim.py`) and late event guard both emit `raw_value`, `reason_code`, and `ingested_at`. Standardized this as the single contract schema everywhere.
2. **`BAD_FIELD_TYPE` Classification**: Added validation for non-step fields that have illegal types (e.g., `amount="abc"`, `nameOrig=123`, `isFraud="yes"`). This routes to `BAD_FIELD_TYPE` while leaving `BAD_STEP_TYPE` for invalid step types.
3. **Empty Generator Yield in `applyInPandasWithState`**: When a batch contains solely late events or filtered events, returning from the Pandas UDF generator without yielding an empty DataFrame caused PySpark Arrow IPC to fail with `EOFException`. Yielding `pd.DataFrame(columns=PAYSIM_OUTPUT_SCHEMA.fields)` resolves this cleanly.
4. **Micro-batch Barrier for Late Event Verification**: Grouping steps 3, 5, and late step 4 in the same ingestion cycle causes Spark to sort them ascending in the initial micro-batch (`3, 4, 5`), preventing step 4 from being recognized as late. Enforcing a micro-batch barrier (waiting for step 5 output commitment before producing step 4) ensures deterministic late-event triggering.

## Files Changed / Created
- `member3_streaming/PROGRESS.md` - Execution progress ledger tracking chunk lifecycle.
- `member3_streaming/DECISIONS.md` - Architecture decision records for DLQ codes, schema, and RocksDB provider.
- `member3_streaming/consumer_paysim.py` - Added `BAD_FIELD_TYPE`, unified DLQ emission schema, fixed empty generator yield.
- `member3_streaming/send_bad_messages.py` - Added bad field type test message (`amount="abc"`).
- `member3_streaming/MEMBER3_OUTPUT_CONTRACT.md` - Unified DLQ schema, generated DLQ code table from constants, generated 42-column schema, corrected Tier B rationale.
- `member3_streaming/README.md` - Updated DLQ schema and codes, Tier B rationale, and recovery notes.
- `member3_streaming/test_dlq_consistency.py` - End-to-end verification script testing all 9 DLQ reason codes against real Kafka and real Spark.

## Gate Verification

### 1R.a & 1R.b: All Reason Codes in One Test Run
All 9 DLQ reason codes tested in a single execution run:
1. `MALFORMED_JSON`
2. `MISSING_FIELD`
3. `BAD_STEP_TYPE`
4. `BAD_TYPE`
5. `BAD_FIELD_TYPE`
6. `NEGATIVE_AMOUNT`
7. `INVALID_STEP`
8. `INVALID_LABEL`
9. `LATE_EVENT`

### 1R.c: Pass Condition Verification
- Exactly one DLQ record per bad message: **PASSED** (Total 9 DLQ records received).
- Correct code each: **PASSED** (All 9 reason codes matched 1:1).
- Query active afterwards: **PASSED** (`query.isActive == True`).
- Valid rows unaffected: **PASSED** (Exactly 5 valid transactions committed to `financial_features_paysim`).

### Raw Evidence: Captured DLQ Records
```json
DLQ #1:
  reason_code: MALFORMED_JSON
  ingested_at: 2026-10-13T16:44:24.198913+00:00
  raw_value:   CORRUPTED_NON_JSON_PAYLOAD_STRING{{{

DLQ #2:
  reason_code: MISSING_FIELD
  ingested_at: 2026-10-13T16:44:24.198913+00:00
  raw_value:   {"type": "TRANSFER", "amount": 1500.0, "nameOrig": "C_MISSING_1", "oldbalanceOrg": 5000.0, "newbalanceOrig": 3500.0, "oldbalanceDest": 0.0, "newbalanceDest": 0.0, "isFraud": 0}

DLQ #3:
  reason_code: BAD_STEP_TYPE
  ingested_at: 2026-10-13T16:44:24.198913+00:00
  raw_value:   {"step": "one_hundred", "type": "PAYMENT", "amount": 250.0, "nameOrig": "C_BADSTEP_1", "nameDest": "M_DEST1", "oldbalanceOrg": 500.0, "newbalanceOrig": 250.0, "oldbalanceDest": 0.0, "newbalanceDest": 250.0, "isFraud": 0}

DLQ #4:
  reason_code: BAD_TYPE
  ingested_at: 2026-10-13T16:44:24.198913+00:00
  raw_value:   {"step": 10, "type": "TELEPORT", "amount": 250.0, "nameOrig": "C_BADTYPE_1", "nameDest": "M_DEST_TEL", "oldbalanceOrg": 500.0, "newbalanceOrig": 250.0, "oldbalanceDest": 0.0, "newbalanceDest": 0.0, "isFraud": 0}

DLQ #5:
  reason_code: BAD_FIELD_TYPE
  ingested_at: 2026-10-13T16:44:24.198913+00:00
  raw_value:   {"step": 10, "type": "PAYMENT", "amount": "abc", "nameOrig": "C_BADFIELD_1", "nameDest": "M_DEST_FIELD", "oldbalanceOrg": 500.0, "newbalanceOrig": 250.0, "oldbalanceDest": 0.0, "newbalanceDest": 0.0, "isFraud": 0}

DLQ #6:
  reason_code: NEGATIVE_AMOUNT
  ingested_at: 2026-10-13T16:44:24.198913+00:00
  raw_value:   {"step": 10, "type": "PAYMENT", "amount": -99.99, "nameOrig": "C_NEG_1", "nameDest": "M_DEST2", "oldbalanceOrg": 500.0, "newbalanceOrig": 500.0, "oldbalanceDest": 0.0, "newbalanceDest": 0.0, "isFraud": 0}

DLQ #7:
  reason_code: INVALID_STEP
  ingested_at: 2026-10-13T16:44:24.198913+00:00
  raw_value:   {"step": 9999, "type": "CASH_OUT", "amount": 300.0, "nameOrig": "C_STEP_9999", "nameDest": "M_DEST3", "oldbalanceOrg": 500.0, "newbalanceOrig": 200.0, "oldbalanceDest": 0.0, "newbalanceDest": 0.0, "isFraud": 0}

DLQ #8:
  reason_code: INVALID_LABEL
  ingested_at: 2026-10-13T16:44:24.198913+00:00
  raw_value:   {"step": 15, "type": "TRANSFER", "amount": 1000.0, "nameOrig": "C_FRAUD_2", "nameDest": "M_DEST4", "oldbalanceOrg": 1000.0, "newbalanceOrig": 0.0, "oldbalanceDest": 0.0, "newbalanceDest": 0.0, "isFraud": 2}

DLQ #9:
  reason_code: LATE_EVENT
  ingested_at: 2026-10-13T16:44:25.150732+00:00
  raw_value:   {"step": 4, "type": "PAYMENT", "amount": 50.0, "nameOrig": "C_LATE", "oldbalanceOrg": 200.0, "newbalanceOrig": 150.0, "nameDest": "M_TEST_GUARD_1R", "oldbalanceDest": 300.0, "newbalanceDest": 350.0, "isFraud": 0, "isFlaggedFraud": 0, "step_hour": 4, "txn_id": "3f60f2291e4f21d59ae9cd0117e6a076ca3c0da3b94db69cc3a2e82a44459812", "hour_sin": 0.8660254037844386, "hour_cos": 0.5000000000000001, "sim_day": 0, "history_window_complete_flag": 0, "type_transfer": 0, "type_cash_out": 0, "type_payment": 1, "type_cash_in": 0, "type_debit": 0, "split": "train", "amount_log1p": 3.9318256327243257, "amount_zero_flag": 0, "amt_to_oldbalOrg": 0.24875621890547264, "amt_to_oldbalDest": 0.16611295681063123, "orig_drain_flag": 0, "amount_exceeds_orig_balance_flag": 0, "orig_balance_zero_flag": 0, "dest_balance_zero_flag": 0}
```

### Raw Evidence: Captured Valid Output Records
```
OUT #1: step=3, nameDest=M_TEST_GUARD_1R, amount=100.0, txn_id=f8fae96d3e4fc84e5b6271af902a635143247425e82833829700790974dda78f, dest_prior_txn_count=0, dest_prior_amount_sum=0.0, dest_steps_since_last_txn=None, split=train
OUT #2: step=5, nameDest=M_TEST_GUARD_1R, amount=200.0, txn_id=ea28122ceb1b87e09dc67b0127bbc1eb9f7dfe72e693f8f013aa41d1fdf12ef3, dest_prior_txn_count=1, dest_prior_amount_sum=100.0, dest_steps_since_last_txn=2, split=train
OUT #3: step=20, nameDest=M_POST_DEST, amount=55.55, txn_id=9b7b3a5c12597c63114ee10529281e9f3d86c06324b64df0e07828275bfb0b02, dest_prior_txn_count=0, dest_prior_amount_sum=0.0, dest_steps_since_last_txn=None, split=train
OUT #4: step=5, nameDest=M_TEST_GUARD_1R, amount=75.0, txn_id=95168c38888fdd564d310119dd1ba3e772854fb0df7dd0cb76a03992a8c719d0, dest_prior_txn_count=1, dest_prior_amount_sum=100.0, dest_steps_since_last_txn=2, split=train
OUT #5: step=6, nameDest=M_TEST_GUARD_1R, amount=300.0, txn_id=c3fcce171bc35fce19e0b5a0b2fd5b038304faa65422399ef17c4bd7c65e1bd6, dest_prior_txn_count=3, dest_prior_amount_sum=375.0, dest_steps_since_last_txn=1, split=train
```

### Raw Evidence: Audit Summary
```
================================================================================
DLQ REASON CODE VERIFICATION AUDIT
================================================================================
  LATE_EVENT          : expected 1, observed 1  -> PASS
  MALFORMED_JSON      : expected 1, observed 1  -> PASS
  MISSING_FIELD       : expected 1, observed 1  -> PASS
  BAD_STEP_TYPE       : expected 1, observed 1  -> PASS
  BAD_TYPE            : expected 1, observed 1  -> PASS
  BAD_FIELD_TYPE      : expected 1, observed 1  -> PASS
  NEGATIVE_AMOUNT     : expected 1, observed 1  -> PASS
  INVALID_STEP        : expected 1, observed 1  -> PASS
  INVALID_LABEL       : expected 1, observed 1  -> PASS

================================================================================
SUMMARY: DLQ Code Check: PASS
SUMMARY: Valid Count (5 expected): PASS (actual: 5)
SUMMARY: Query Active Throughout: PASS
================================================================================
OVERALL DLQ CONSISTENCY TEST: PASSED
```

## Git Output
```
On branch member3-streaming
Changes to be committed:
	modified:   member3_streaming/MEMBER3_OUTPUT_CONTRACT.md
	modified:   member3_streaming/README.md
	modified:   member3_streaming/consumer_paysim.py
	modified:   member3_streaming/send_bad_messages.py
	new file:   member3_streaming/DECISIONS.md
	new file:   member3_streaming/PROGRESS.md
	new file:   member3_streaming/reports/C01R_report.md
	new file:   member3_streaming/test_dlq_consistency.py
```
