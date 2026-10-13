# Member 3 Architectural & Design Decisions Ledger

| Date | Chunk | Choice | Alternatives | Reason / Rationale |
|---|---|---|---|---|
| 2026-10-13 | 1R | DLQ naming: `BAD_TYPE` vs `BAD_STEP_TYPE` vs `BAD_FIELD_TYPE` | Combine all into `BAD_TYPE` or use single `BAD_FIELD_TYPE` | `BAD_TYPE` is reserved for invalid transaction type values (e.g. `TELEPORT`). `BAD_STEP_TYPE` specifically identifies type corruptions on the critical partitioning/sequencing `step` attribute. `BAD_FIELD_TYPE` is assigned to any other non-step attribute having incorrect JSON data types (e.g. `amount="abc"`). |
| 2026-10-13 | 1R | Single DLQ record schema: `raw_value`, `reason_code`, `ingested_at` | `raw_record`, `error_message`, `timestamp` | Emitted Spark schema in `consumer_paysim.py` explicitly constructs `raw_value`, `reason_code`, `ingested_at`. Standardizing across all docs, tests, and contract. |
| 2026-10-13 | 1 | `split` column retention | Exclude `split` from streaming output | Member 2's Tier A batch schema outputs `split` (`train`, `valid`, `test`) derived deterministically from `step`. Stream replicates this column verbatim for 100% parity. |
| 2026-10-13 | 2R | State store provider: `RocksDBStateStoreProvider` | `HDFSBackedStateStoreProvider` | High key cardinality (2.7M+ destinations in PaySim, millions of card proxies in IEEE) necessitates off-heap disk-backed RocksDB state management. |
| 2026-10-13 | 5 | Default sink destination: Kafka topic | Parquet files, Delta table, gRPC | Default streaming integration layer outputs feature events to Kafka topics (`financial_features_paysim`, `financial_features_ieee`) unless Member 4 explicitly requests a file/table sink. |
| 2026-10-13 | 7 | IEEE producer message key: `card1` | `TransactionID`, `card_proxy_id` | `card1` is the raw payment card network/issuer block present on all raw records. Partitioning on `card1` guarantees in-order arrival per card instrument for stateful downstream consumers. |
