# Member 3 IEEE-CIS Output Contract & Specification

## 1. Topic Topology & Infrastructure
- **Input Topic:** `financial_transactions_ieee` (1 partition, key: `TransactionID`)
- **Output Topic:** `financial_features_ieee` (1 partition, key: `TransactionID`)
- **Dead Letter Queue (DLQ):** `financial_transactions_ieee_dlq` (1 partition, key: `TransactionID`)
- **Default Broker:** `127.0.0.1:9092`
- **Checkpoint Directory:** `member3_streaming/checkpoints/ieee_stateful`
- **State Store Provider:** `org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider`

## 2. Delivery Guarantees & Deduplication
- **Delivery Semantics:** At-least-once.
- **Deduplication Requirement:** Downstream consumers (e.g. Member 4 model inference) MUST deduplicate incoming stream records by `TransactionID`.
- **Duplicate Byte-Identity:** In mid-batch failure recovery events, all re-emitted duplicate messages have been empirically proven to be **100% byte-identical** across all 483 serialized JSON fields (verified on 210 replayed duplicates in Gate 1.5).

## 3. Ordering Assumptions & Tie Semantics
- **Global Ordering Assumption:** Producer emits events in strictly non-decreasing `(TransactionDT, TransactionID)` order. Stateful features are valid only under this deterministic sequence (verified with 0 inversions across all 590,540 rows of `ieee_clean.parquet`).
- **Within-Microbatch Ordering:** If out-of-order events arrive within a single Spark micro-batch, PySpark Structured Streaming stably sorts the incoming micro-batch partition by `(TransactionDT, TransactionID)` before state updates.
- **Same-Second Tie Semantics:** Transactions with identical `TransactionDT` are ordered deterministically by `TransactionID`. Rolling velocity window lookbacks use `rangeBetween(-W, -1)` (in seconds), which strictly excludes the current transaction and any other same-second tie transactions occurring at $t$. Cumulative prior features (`card_prior_*`) observe preceding transactions under `(DT, TID)` ordering.

## 4. Feature Definitions & Null Semantics
- **Total Output Columns:** Exactly 483 columns (472 stateless + 11 stateful card history features).
- **State Key:** `card_proxy_id` (SHA-256 hash of `card1`, `card2`, `card3`, `card4`, `card5`, `card6`).
- **Stateless Features (472 columns):** Includes cyclical temporal encodings (`hour_sin`, `hour_cos`, `day_sin`, `day_cos`), missingness block flags, amount decimal fingerprints, split boundaries, and masked family aggregations (M, C, D) produced identically to Member 2's `build_features.py`.
- **Stateful Card History Features (11 columns):**
  1. `card_prior_txn_count` (int32): Prior transactions for this `card_proxy_id`. Starts at 0.
  2. `card_first_seen_flag` (int8): 1 if `card_prior_txn_count == 0`, else 0.
  3. `card_prior_amt_sum` (float64): Running sum of prior transaction amounts for this card. Starts at 0.0.
  4. `card_prior_amt_mean` (float64): Running mean of prior transaction amounts. Null when `card_prior_txn_count == 0`.
  5. `card_prior_amt_std` (float64): Running sample standard deviation ($N-1$). Evaluated via Welford algorithm. Null when `card_prior_txn_count < 2`.
  6. `card_prior_amt_max` (float64): Running maximum prior transaction amount. Null when `card_prior_txn_count == 0`.
  7. `amt_zscore_vs_card` (float64): $(amt - prior\_mean) / prior\_std$. Evaluated ONLY when `card_prior_txn_count >= 3` AND `prior_std > 1e-5`; otherwise Null.
  8. `card_secs_since_last_txn` (int32): $cur\_dt - last\_dt$. Null when `card_prior_txn_count == 0`.
  9. `card_txn_count_1h` (int32): Count of card transactions in $[t - 3600, t - 1]$.
  10. `card_txn_count_24h` (int32): Count of card transactions in $[t - 86400, t - 1]$.
  11. `card_txn_count_7d` (int32): Count of card transactions in $[t - 604800, t - 1]$. History buffer pruned for timestamps older than 7 days.
- **Tier B Features:** None for IEEE-CIS. `isFraud` is the label only.
- **Dataset Split:** `train` / `valid` only (no test split). Train split boundary: `TransactionDT <= 11232000` (`split = 'train'`), else `'valid'`.

## 5. DLQ Record Schema & Reason Codes
DLQ records are published to `financial_transactions_ieee_dlq` with schema:
```json
{
  "raw_value": "<serialized JSON payload>",
  "reason_code": "<code string>",
  "ingested_at": "<ISO-8601 UTC timestamp stamped by consumer upon DLQ routing>"
}
```

| Reason Code | Trigger Condition | `raw_value` Payload Content |
|---|---|---|
| `MALFORMED_JSON` | Payload cannot be parsed as a JSON object | Raw unparsed Kafka input string |
| `MISSING_FIELD` | Required field (`TransactionID`, `TransactionDT`, `TransactionAmt`, `ProductCD`, `card1`) missing or null | Raw unparsed Kafka input string |
| `BAD_FIELD_TYPE` | Required field has invalid JSON type (e.g. non-numeric amount) | Raw unparsed Kafka input string |
| `NEGATIVE_AMOUNT` | `TransactionAmt < 0.0` | Raw unparsed Kafka input string |
| `INVALID_LABEL` | `isFraud` not in {0, 1} and not null | Raw unparsed Kafka input string |
| `INVALID_TRANSACTIONDT` | `TransactionDT <= 0` | Raw unparsed Kafka input string |
| `LATE_EVENT` | Event arrives with `TransactionDT < s_last_dt` or `(DT == s_last_dt and TID <= s_last_tid)` | **Enriched 472-column row** serialized at state-evaluation time |

## 6. Fault Tolerance & Operator Recovery Procedure
- **State Store Backend:** RocksDB state store (`RocksDBStateStoreProvider`).
- **Mid-Batch Crash Behavior:** PySpark Structured Streaming with Kafka source and RocksDB provider leaves an uncommitted offset file `offsets/N` on disk when killed mid-microbatch. Upon plain restart, `KafkaMicroBatchStream$.metrics` throws an NPE in `finishTrigger` because metric tracking for replayed batches is absent from in-memory driver structures.
- **OPERATOR WORKAROUND (Documented Rollback Procedure):**
  1. Stop consumer process (`taskkill /F /T`).
  2. Inspect checkpoint directory `member3_streaming/checkpoints/ieee_stateful/features/`: compare integer filenames in `offsets/` vs `commits/`.
  3. Identify uncommitted orphan offset index $N$ where `offsets/N` exists but `commits/N` does NOT exist.
  4. Delete ONLY `offsets/N` and `offsets/.N.crc`. **NEVER delete files from `commits/`**.
  5. Relaunch consumer stream. Spark plans batch $N$ cleanly from committed offset $N-1$, restoring complete state continuity with byte-identical duplicate output.

## 7. Output Schema Specification (483 Columns)
Verified 1:1 against `IEEE_OUTPUT_SCHEMA` and `data/features/ieee_features.parquet`:

| Column Name | PySpark Type | Nullable | Field Index |
|---|---|---|---|
| `TransactionID` | `int` | `True` | 0 |
| `isFraud` | `int` | `True` | 1 |
| `TransactionDT` | `int` | `True` | 2 |
| `TransactionAmt` | `double` | `True` | 3 |
| `ProductCD` | `string` | `True` | 4 |
| `card1` | `int` | `True` | 5 |
| `card2` | `double` | `True` | 6 |
| `card3` | `double` | `True` | 7 |
| `card4` | `string` | `True` | 8 |
| `card5` | `double` | `True` | 9 |
| `card6` | `string` | `True` | 10 |
| `addr1` | `double` | `True` | 11 |
| `addr2` | `double` | `True` | 12 |
| `dist1` | `double` | `True` | 13 |
| `dist2` | `double` | `True` | 14 |
| `P_emaildomain` | `string` | `True` | 15 |
| `R_emaildomain` | `string` | `True` | 16 |
| `C1` | `double` | `True` | 17 |
| `C2` | `double` | `True` | 18 |
| `C3` | `double` | `True` | 19 |
| `C4` | `double` | `True` | 20 |
| `C5` | `double` | `True` | 21 |
| `C6` | `double` | `True` | 22 |
| `C7` | `double` | `True` | 23 |
| `C8` | `double` | `True` | 24 |
| `C9` | `double` | `True` | 25 |
| `C10` | `double` | `True` | 26 |
| `C11` | `double` | `True` | 27 |
| `C12` | `double` | `True` | 28 |
| `C13` | `double` | `True` | 29 |
| `C14` | `double` | `True` | 30 |
| `D1` | `double` | `True` | 31 |
| `D2` | `double` | `True` | 32 |
| `D3` | `double` | `True` | 33 |
| `D4` | `double` | `True` | 34 |
| `D5` | `double` | `True` | 35 |
| `D6` | `double` | `True` | 36 |
| `D7` | `double` | `True` | 37 |
| `D8` | `double` | `True` | 38 |
| `D9` | `double` | `True` | 39 |
| `D10` | `double` | `True` | 40 |
| `D11` | `double` | `True` | 41 |
| `D12` | `double` | `True` | 42 |
| `D13` | `double` | `True` | 43 |
| `D14` | `double` | `True` | 44 |
| `D15` | `double` | `True` | 45 |
| `M1` | `string` | `True` | 46 |
| `M2` | `string` | `True` | 47 |
| `M3` | `string` | `True` | 48 |
| `M4` | `string` | `True` | 49 |
| `M5` | `string` | `True` | 50 |
| `M6` | `string` | `True` | 51 |
| `M7` | `string` | `True` | 52 |
| `M8` | `string` | `True` | 53 |
| `M9` | `string` | `True` | 54 |
| `V2` | `double` | `True` | 55 |
| `V3` | `double` | `True` | 56 |
| `V4` | `double` | `True` | 57 |
| `V5` | `double` | `True` | 58 |
| `V6` | `double` | `True` | 59 |
| `V7` | `double` | `True` | 60 |
| `V8` | `double` | `True` | 61 |
| `V9` | `double` | `True` | 62 |
| `V10` | `double` | `True` | 63 |
| `V11` | `double` | `True` | 64 |
| `V12` | `double` | `True` | 65 |
| `V13` | `double` | `True` | 66 |
| `V15` | `double` | `True` | 67 |
| `V16` | `double` | `True` | 68 |
| `V17` | `double` | `True` | 69 |
| `V18` | `double` | `True` | 70 |
| `V19` | `double` | `True` | 71 |
| `V20` | `double` | `True` | 72 |
| `V21` | `double` | `True` | 73 |
| `V22` | `double` | `True` | 74 |
| `V23` | `double` | `True` | 75 |
| `V24` | `double` | `True` | 76 |
| `V25` | `double` | `True` | 77 |
| `V26` | `double` | `True` | 78 |
| `V29` | `double` | `True` | 79 |
| `V30` | `double` | `True` | 80 |
| `V31` | `double` | `True` | 81 |
| `V32` | `double` | `True` | 82 |
| `V33` | `double` | `True` | 83 |
| `V34` | `double` | `True` | 84 |
| `V35` | `double` | `True` | 85 |
| `V36` | `double` | `True` | 86 |
| `V37` | `double` | `True` | 87 |
| `V38` | `double` | `True` | 88 |
| `V39` | `double` | `True` | 89 |
| `V40` | `double` | `True` | 90 |
| `V42` | `double` | `True` | 91 |
| `V43` | `double` | `True` | 92 |
| `V44` | `double` | `True` | 93 |
| `V45` | `double` | `True` | 94 |
| `V46` | `double` | `True` | 95 |
| `V47` | `double` | `True` | 96 |
| `V48` | `double` | `True` | 97 |
| `V49` | `double` | `True` | 98 |
| `V50` | `double` | `True` | 99 |
| `V51` | `double` | `True` | 100 |
| `V52` | `double` | `True` | 101 |
| `V53` | `double` | `True` | 102 |
| `V54` | `double` | `True` | 103 |
| `V55` | `double` | `True` | 104 |
| `V56` | `double` | `True` | 105 |
| `V57` | `double` | `True` | 106 |
| `V58` | `double` | `True` | 107 |
| `V59` | `double` | `True` | 108 |
| `V60` | `double` | `True` | 109 |
| `V61` | `double` | `True` | 110 |
| `V62` | `double` | `True` | 111 |
| `V63` | `double` | `True` | 112 |
| `V64` | `double` | `True` | 113 |
| `V66` | `double` | `True` | 114 |
| `V67` | `double` | `True` | 115 |
| `V69` | `double` | `True` | 116 |
| `V70` | `double` | `True` | 117 |
| `V71` | `double` | `True` | 118 |
| `V72` | `double` | `True` | 119 |
| `V73` | `double` | `True` | 120 |
| `V74` | `double` | `True` | 121 |
| `V75` | `double` | `True` | 122 |
| `V76` | `double` | `True` | 123 |
| `V77` | `double` | `True` | 124 |
| `V78` | `double` | `True` | 125 |
| `V79` | `double` | `True` | 126 |
| `V80` | `double` | `True` | 127 |
| `V81` | `double` | `True` | 128 |
| `V82` | `double` | `True` | 129 |
| `V83` | `double` | `True` | 130 |
| `V84` | `double` | `True` | 131 |
| `V85` | `double` | `True` | 132 |
| `V86` | `double` | `True` | 133 |
| `V87` | `double` | `True` | 134 |
| `V90` | `double` | `True` | 135 |
| `V91` | `double` | `True` | 136 |
| `V92` | `double` | `True` | 137 |
| `V93` | `double` | `True` | 138 |
| `V94` | `double` | `True` | 139 |
| `V95` | `double` | `True` | 140 |
| `V96` | `double` | `True` | 141 |
| `V97` | `double` | `True` | 142 |
| `V98` | `double` | `True` | 143 |
| `V99` | `double` | `True` | 144 |
| `V100` | `double` | `True` | 145 |
| `V101` | `double` | `True` | 146 |
| `V102` | `double` | `True` | 147 |
| `V103` | `double` | `True` | 148 |
| `V104` | `double` | `True` | 149 |
| `V105` | `double` | `True` | 150 |
| `V106` | `double` | `True` | 151 |
| `V108` | `double` | `True` | 152 |
| `V109` | `double` | `True` | 153 |
| `V110` | `double` | `True` | 154 |
| `V111` | `double` | `True` | 155 |
| `V112` | `double` | `True` | 156 |
| `V113` | `double` | `True` | 157 |
| `V114` | `double` | `True` | 158 |
| `V115` | `double` | `True` | 159 |
| `V116` | `double` | `True` | 160 |
| `V117` | `double` | `True` | 161 |
| `V118` | `double` | `True` | 162 |
| `V119` | `double` | `True` | 163 |
| `V120` | `double` | `True` | 164 |
| `V121` | `double` | `True` | 165 |
| `V122` | `double` | `True` | 166 |
| `V123` | `double` | `True` | 167 |
| `V124` | `double` | `True` | 168 |
| `V125` | `double` | `True` | 169 |
| `V126` | `double` | `True` | 170 |
| `V127` | `double` | `True` | 171 |
| `V128` | `double` | `True` | 172 |
| `V129` | `double` | `True` | 173 |
| `V130` | `double` | `True` | 174 |
| `V131` | `double` | `True` | 175 |
| `V132` | `double` | `True` | 176 |
| `V133` | `double` | `True` | 177 |
| `V134` | `double` | `True` | 178 |
| `V135` | `double` | `True` | 179 |
| `V136` | `double` | `True` | 180 |
| `V137` | `double` | `True` | 181 |
| `V138` | `double` | `True` | 182 |
| `V139` | `double` | `True` | 183 |
| `V140` | `double` | `True` | 184 |
| `V141` | `double` | `True` | 185 |
| `V142` | `double` | `True` | 186 |
| `V143` | `double` | `True` | 187 |
| `V144` | `double` | `True` | 188 |
| `V145` | `double` | `True` | 189 |
| `V146` | `double` | `True` | 190 |
| `V147` | `double` | `True` | 191 |
| `V148` | `double` | `True` | 192 |
| `V149` | `double` | `True` | 193 |
| `V150` | `double` | `True` | 194 |
| `V151` | `double` | `True` | 195 |
| `V152` | `double` | `True` | 196 |
| `V153` | `double` | `True` | 197 |
| `V154` | `double` | `True` | 198 |
| `V155` | `double` | `True` | 199 |
| `V156` | `double` | `True` | 200 |
| `V157` | `double` | `True` | 201 |
| `V158` | `double` | `True` | 202 |
| `V159` | `double` | `True` | 203 |
| `V160` | `double` | `True` | 204 |
| `V161` | `double` | `True` | 205 |
| `V162` | `double` | `True` | 206 |
| `V163` | `double` | `True` | 207 |
| `V164` | `double` | `True` | 208 |
| `V165` | `double` | `True` | 209 |
| `V166` | `double` | `True` | 210 |
| `V167` | `double` | `True` | 211 |
| `V168` | `double` | `True` | 212 |
| `V169` | `double` | `True` | 213 |
| `V170` | `double` | `True` | 214 |
| `V171` | `double` | `True` | 215 |
| `V172` | `double` | `True` | 216 |
| `V173` | `double` | `True` | 217 |
| `V174` | `double` | `True` | 218 |
| `V175` | `double` | `True` | 219 |
| `V176` | `double` | `True` | 220 |
| `V177` | `double` | `True` | 221 |
| `V178` | `double` | `True` | 222 |
| `V179` | `double` | `True` | 223 |
| `V180` | `double` | `True` | 224 |
| `V181` | `double` | `True` | 225 |
| `V182` | `double` | `True` | 226 |
| `V183` | `double` | `True` | 227 |
| `V184` | `double` | `True` | 228 |
| `V185` | `double` | `True` | 229 |
| `V186` | `double` | `True` | 230 |
| `V187` | `double` | `True` | 231 |
| `V188` | `double` | `True` | 232 |
| `V189` | `double` | `True` | 233 |
| `V190` | `double` | `True` | 234 |
| `V191` | `double` | `True` | 235 |
| `V192` | `double` | `True` | 236 |
| `V193` | `double` | `True` | 237 |
| `V194` | `double` | `True` | 238 |
| `V195` | `double` | `True` | 239 |
| `V196` | `double` | `True` | 240 |
| `V197` | `double` | `True` | 241 |
| `V198` | `double` | `True` | 242 |
| `V199` | `double` | `True` | 243 |
| `V200` | `double` | `True` | 244 |
| `V201` | `double` | `True` | 245 |
| `V202` | `double` | `True` | 246 |
| `V203` | `double` | `True` | 247 |
| `V204` | `double` | `True` | 248 |
| `V205` | `double` | `True` | 249 |
| `V206` | `double` | `True` | 250 |
| `V207` | `double` | `True` | 251 |
| `V208` | `double` | `True` | 252 |
| `V209` | `double` | `True` | 253 |
| `V210` | `double` | `True` | 254 |
| `V211` | `double` | `True` | 255 |
| `V212` | `double` | `True` | 256 |
| `V213` | `double` | `True` | 257 |
| `V214` | `double` | `True` | 258 |
| `V215` | `double` | `True` | 259 |
| `V216` | `double` | `True` | 260 |
| `V217` | `double` | `True` | 261 |
| `V218` | `double` | `True` | 262 |
| `V219` | `double` | `True` | 263 |
| `V220` | `double` | `True` | 264 |
| `V221` | `double` | `True` | 265 |
| `V222` | `double` | `True` | 266 |
| `V223` | `double` | `True` | 267 |
| `V224` | `double` | `True` | 268 |
| `V225` | `double` | `True` | 269 |
| `V226` | `double` | `True` | 270 |
| `V227` | `double` | `True` | 271 |
| `V228` | `double` | `True` | 272 |
| `V229` | `double` | `True` | 273 |
| `V230` | `double` | `True` | 274 |
| `V231` | `double` | `True` | 275 |
| `V232` | `double` | `True` | 276 |
| `V233` | `double` | `True` | 277 |
| `V234` | `double` | `True` | 278 |
| `V235` | `double` | `True` | 279 |
| `V236` | `double` | `True` | 280 |
| `V237` | `double` | `True` | 281 |
| `V238` | `double` | `True` | 282 |
| `V239` | `double` | `True` | 283 |
| `V242` | `double` | `True` | 284 |
| `V243` | `double` | `True` | 285 |
| `V244` | `double` | `True` | 286 |
| `V245` | `double` | `True` | 287 |
| `V246` | `double` | `True` | 288 |
| `V247` | `double` | `True` | 289 |
| `V248` | `double` | `True` | 290 |
| `V249` | `double` | `True` | 291 |
| `V250` | `double` | `True` | 292 |
| `V251` | `double` | `True` | 293 |
| `V252` | `double` | `True` | 294 |
| `V253` | `double` | `True` | 295 |
| `V254` | `double` | `True` | 296 |
| `V255` | `double` | `True` | 297 |
| `V256` | `double` | `True` | 298 |
| `V257` | `double` | `True` | 299 |
| `V258` | `double` | `True` | 300 |
| `V259` | `double` | `True` | 301 |
| `V260` | `double` | `True` | 302 |
| `V261` | `double` | `True` | 303 |
| `V262` | `double` | `True` | 304 |
| `V263` | `double` | `True` | 305 |
| `V264` | `double` | `True` | 306 |
| `V265` | `double` | `True` | 307 |
| `V266` | `double` | `True` | 308 |
| `V267` | `double` | `True` | 309 |
| `V268` | `double` | `True` | 310 |
| `V269` | `double` | `True` | 311 |
| `V270` | `double` | `True` | 312 |
| `V271` | `double` | `True` | 313 |
| `V272` | `double` | `True` | 314 |
| `V273` | `double` | `True` | 315 |
| `V274` | `double` | `True` | 316 |
| `V275` | `double` | `True` | 317 |
| `V276` | `double` | `True` | 318 |
| `V277` | `double` | `True` | 319 |
| `V278` | `double` | `True` | 320 |
| `V279` | `double` | `True` | 321 |
| `V280` | `double` | `True` | 322 |
| `V281` | `double` | `True` | 323 |
| `V282` | `double` | `True` | 324 |
| `V283` | `double` | `True` | 325 |
| `V284` | `double` | `True` | 326 |
| `V285` | `double` | `True` | 327 |
| `V286` | `double` | `True` | 328 |
| `V287` | `double` | `True` | 329 |
| `V288` | `double` | `True` | 330 |
| `V289` | `double` | `True` | 331 |
| `V290` | `double` | `True` | 332 |
| `V291` | `double` | `True` | 333 |
| `V292` | `double` | `True` | 334 |
| `V293` | `double` | `True` | 335 |
| `V294` | `double` | `True` | 336 |
| `V295` | `double` | `True` | 337 |
| `V296` | `double` | `True` | 338 |
| `V297` | `double` | `True` | 339 |
| `V298` | `double` | `True` | 340 |
| `V299` | `double` | `True` | 341 |
| `V300` | `double` | `True` | 342 |
| `V301` | `double` | `True` | 343 |
| `V302` | `double` | `True` | 344 |
| `V303` | `double` | `True` | 345 |
| `V304` | `double` | `True` | 346 |
| `V306` | `double` | `True` | 347 |
| `V307` | `double` | `True` | 348 |
| `V308` | `double` | `True` | 349 |
| `V309` | `double` | `True` | 350 |
| `V310` | `double` | `True` | 351 |
| `V311` | `double` | `True` | 352 |
| `V312` | `double` | `True` | 353 |
| `V313` | `double` | `True` | 354 |
| `V314` | `double` | `True` | 355 |
| `V315` | `double` | `True` | 356 |
| `V316` | `double` | `True` | 357 |
| `V317` | `double` | `True` | 358 |
| `V318` | `double` | `True` | 359 |
| `V319` | `double` | `True` | 360 |
| `V320` | `double` | `True` | 361 |
| `V321` | `double` | `True` | 362 |
| `V322` | `double` | `True` | 363 |
| `V323` | `double` | `True` | 364 |
| `V324` | `double` | `True` | 365 |
| `V325` | `double` | `True` | 366 |
| `V326` | `double` | `True` | 367 |
| `V327` | `double` | `True` | 368 |
| `V328` | `double` | `True` | 369 |
| `V329` | `double` | `True` | 370 |
| `V330` | `double` | `True` | 371 |
| `V331` | `double` | `True` | 372 |
| `V332` | `double` | `True` | 373 |
| `V333` | `double` | `True` | 374 |
| `V334` | `double` | `True` | 375 |
| `V335` | `double` | `True` | 376 |
| `V336` | `double` | `True` | 377 |
| `V337` | `double` | `True` | 378 |
| `V338` | `double` | `True` | 379 |
| `V339` | `double` | `True` | 380 |
| `id_01` | `double` | `True` | 381 |
| `id_02` | `double` | `True` | 382 |
| `id_03` | `double` | `True` | 383 |
| `id_04` | `double` | `True` | 384 |
| `id_05` | `double` | `True` | 385 |
| `id_06` | `double` | `True` | 386 |
| `id_07` | `double` | `True` | 387 |
| `id_08` | `double` | `True` | 388 |
| `id_09` | `double` | `True` | 389 |
| `id_10` | `double` | `True` | 390 |
| `id_11` | `double` | `True` | 391 |
| `id_12` | `string` | `True` | 392 |
| `id_13` | `double` | `True` | 393 |
| `id_14` | `double` | `True` | 394 |
| `id_15` | `string` | `True` | 395 |
| `id_16` | `string` | `True` | 396 |
| `id_17` | `double` | `True` | 397 |
| `id_18` | `double` | `True` | 398 |
| `id_19` | `double` | `True` | 399 |
| `id_20` | `double` | `True` | 400 |
| `id_21` | `double` | `True` | 401 |
| `id_22` | `double` | `True` | 402 |
| `id_23` | `string` | `True` | 403 |
| `id_24` | `double` | `True` | 404 |
| `id_25` | `double` | `True` | 405 |
| `id_26` | `double` | `True` | 406 |
| `id_28` | `string` | `True` | 407 |
| `id_29` | `string` | `True` | 408 |
| `id_30` | `string` | `True` | 409 |
| `id_31` | `string` | `True` | 410 |
| `id_32` | `double` | `True` | 411 |
| `id_33` | `string` | `True` | 412 |
| `id_34` | `string` | `True` | 413 |
| `id_35` | `string` | `True` | 414 |
| `id_36` | `string` | `True` | 415 |
| `id_37` | `string` | `True` | 416 |
| `id_38` | `string` | `True` | 417 |
| `DeviceType` | `string` | `True` | 418 |
| `DeviceInfo` | `string` | `True` | 419 |
| `identity_nonnull_count` | `int` | `True` | 420 |
| `has_identity_flag` | `tinyint` | `True` | 421 |
| `identity_completeness` | `double` | `True` | 422 |
| `ind_addr` | `tinyint` | `True` | 423 |
| `ind_dist1` | `tinyint` | `True` | 424 |
| `ind_dist2` | `tinyint` | `True` | 425 |
| `ind_Pemail` | `tinyint` | `True` | 426 |
| `ind_Remail` | `tinyint` | `True` | 427 |
| `ind_D2` | `tinyint` | `True` | 428 |
| `ind_D3` | `tinyint` | `True` | 429 |
| `ind_D4` | `tinyint` | `True` | 430 |
| `ind_D5` | `tinyint` | `True` | 431 |
| `ind_D6` | `tinyint` | `True` | 432 |
| `ind_D7` | `tinyint` | `True` | 433 |
| `ind_D10` | `tinyint` | `True` | 434 |
| `ind_D11` | `tinyint` | `True` | 435 |
| `ind_D12` | `tinyint` | `True` | 436 |
| `ind_D13` | `tinyint` | `True` | 437 |
| `ind_D14` | `tinyint` | `True` | 438 |
| `ind_D15` | `tinyint` | `True` | 439 |
| `ind_M1_3` | `tinyint` | `True` | 440 |
| `ind_V1_11` | `tinyint` | `True` | 441 |
| `ind_V12_34` | `tinyint` | `True` | 442 |
| `ind_V35_52` | `tinyint` | `True` | 443 |
| `ind_V53_74` | `tinyint` | `True` | 444 |
| `ind_V75_94` | `tinyint` | `True` | 445 |
| `ind_V138_166` | `tinyint` | `True` | 446 |
| `ind_V167_278` | `tinyint` | `True` | 447 |
| `ind_V322_339` | `tinyint` | `True` | 448 |
| `card_proxy_id` | `string` | `True` | 449 |
| `card_addr_proxy_id` | `string` | `True` | 450 |
| `hour_of_day` | `int` | `True` | 451 |
| `hour_sin` | `double` | `True` | 452 |
| `hour_cos` | `double` | `True` | 453 |
| `day_index` | `int` | `True` | 454 |
| `day_of_week_rel` | `int` | `True` | 455 |
| `history_window_complete_flag` | `tinyint` | `True` | 456 |
| `split` | `string` | `True` | 457 |
| `amt_log1p` | `double` | `True` | 458 |
| `amt_cents` | `int` | `True` | 459 |
| `amt_decimals_gt2_flag` | `tinyint` | `True` | 460 |
| `amt_is_whole_flag` | `tinyint` | `True` | 461 |
| `m_true_cnt` | `int` | `True` | 462 |
| `m_false_cnt` | `int` | `True` | 463 |
| `m_null_cnt` | `int` | `True` | 464 |
| `c_sum_log1p` | `double` | `True` | 465 |
| `c_nonzero_cnt` | `int` | `True` | 466 |
| `c_max` | `double` | `True` | 467 |
| `d_nonnull_cnt` | `int` | `True` | 468 |
| `d_min` | `double` | `True` | 469 |
| `d_max` | `double` | `True` | 470 |
| `card_prior_txn_count` | `int` | `True` | 471 |
| `card_first_seen_flag` | `tinyint` | `True` | 472 |
| `card_prior_amt_sum` | `double` | `True` | 473 |
| `card_prior_amt_mean` | `double` | `True` | 474 |
| `card_prior_amt_std` | `double` | `True` | 475 |
| `card_prior_amt_max` | `double` | `True` | 476 |
| `amt_zscore_vs_card` | `double` | `True` | 477 |
| `card_secs_since_last_txn` | `int` | `True` | 478 |
| `card_txn_count_1h` | `int` | `True` | 479 |
| `card_txn_count_24h` | `int` | `True` | 480 |
| `card_txn_count_7d` | `int` | `True` | 481 |
| `feature_version` | `string` | `True` | 482 |
