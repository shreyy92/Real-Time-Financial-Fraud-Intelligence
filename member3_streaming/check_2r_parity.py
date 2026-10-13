import os
import sys
import json
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from kafka import KafkaConsumer

def main():
    print("Consuming records from financial_features_paysim...")
    c = KafkaConsumer(
        'financial_features_paysim',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        consumer_timeout_ms=5000,
    )
    records = []
    for msg in c:
        records.append((msg.key.decode('utf-8') if msg.key else None, json.loads(msg.value.decode('utf-8')), msg.value))
    c.close()

    total_recs = len(records)
    print(f"Total output records: {total_recs}")

    seen = {}
    duplicates = []
    for k, row, raw_b in records:
        tid = row['txn_id']
        if tid in seen:
            duplicates.append((tid, raw_b, seen[tid]))
        else:
            seen[tid] = raw_b

    distinct_count = len(seen)
    dup_count = len(duplicates)
    print(f"Distinct txn_id count: {distinct_count} (expected 20000)")
    print(f"Duplicate count: {dup_count}")

    byte_identical = True
    for tid, b1, b2 in duplicates:
        if b1 != b2:
            byte_identical = False
            print(f"Mismatch in duplicate bytes for {tid}!")
            break
    print(f"All duplicates byte-identical: {byte_identical}")

    # Parity check vs parquet
    table = pq.read_table('data/features/paysim_features.parquet')
    batch_df = table.slice(0, 20000).to_pandas()
    stream_rows = [r for (k, r, b) in records]
    stream_df = pd.DataFrame(stream_rows).drop_duplicates(subset=['txn_id']).sort_values('txn_id').reset_index(drop=True)
    batch_df = batch_df.sort_values('txn_id').reset_index(drop=True)

    compared_cols = [col for col in batch_df.columns if col in stream_df.columns and col != 'feature_version']
    print(f"Total compared columns: {len(compared_cols)}")

    mismatches = {}
    for col in compared_cols:
        b_col = batch_df[col]
        s_col = stream_df[col]
        if pd.api.types.is_float_dtype(b_col):
            null_ok = (b_col.isna() == s_col.isna()).all()
            if not null_ok:
                mismatches[col] = int((b_col.isna() != s_col.isna()).sum())
                continue
            valid = b_col.notna()
            diff = np.abs(b_col[valid].to_numpy() - s_col[valid].to_numpy())
            cnt = int((diff > 1e-4).sum())
            if cnt > 0:
                mismatches[col] = cnt
        else:
            diff = (b_col.to_numpy() != s_col.to_numpy())
            cnt = int(diff.sum())
            if cnt > 0:
                mismatches[col] = cnt

    print("Mismatch summary:")
    for col in compared_cols:
        c_m = mismatches.get(col, 0)
        print(f"  {col:35s}: {'PASS' if c_m == 0 else f'FAIL ({c_m})'}")

    tot_mismatches = sum(mismatches.values())
    print(f"\nTOTAL MISMATCHES ACROSS ALL 41 COLUMNS: {tot_mismatches}")
    assert tot_mismatches == 0, f"Parity check failed with {tot_mismatches} mismatches!"
    assert distinct_count == 20000, f"Distinct count {distinct_count} != 20,000!"
    assert byte_identical, "Duplicates not byte-identical!"
    print("\nCHUNK 2R 20,000 ROW RECOVERY: 100% PASSED!")

if __name__ == '__main__':
    main()
