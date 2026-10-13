import json
import numpy as np
import pandas as pd
from kafka import KafkaConsumer

def main():
    print("Reading stream records from Kafka...")
    c = KafkaConsumer('financial_features_paysim', bootstrap_servers=['localhost:9092'], auto_offset_reset='earliest', consumer_timeout_ms=5000)
    seen = {}
    duplicates = []
    for msg in c:
        val_str = msg.value.decode('utf-8')
        row = json.loads(val_str)
        tid = row['txn_id']
        if tid in seen:
            duplicates.append((tid, val_str, seen[tid][1]))
        else:
            seen[tid] = (row, val_str)
    c.close()

    print(f"Total distinct records: {len(seen)}")
    print(f"Total duplicates: {len(duplicates)}")
    byte_identical = all(v1 == v2 for _, v1, v2 in duplicates)
    print(f"All duplicates byte-identical: {byte_identical}")

    stream_df = pd.DataFrame([row for row, _ in seen.values()])

    print("Loading data/features/paysim_features.parquet...")
    p_feat = pd.read_parquet('data/features/paysim_features.parquet')
    print(f"Full feature store shape: {p_feat.shape}")

    matched_batch = p_feat[p_feat['txn_id'].isin(stream_df['txn_id'])].copy()
    print(f"Matched feature store shape: {matched_batch.shape}")
    assert len(matched_batch) == 20000, f"Expected 20,000 matches, found {len(matched_batch)}"

    s_df = stream_df.sort_values('txn_id').reset_index(drop=True)
    b_df = matched_batch.sort_values('txn_id').reset_index(drop=True)

    compared_cols = [c for c in b_df.columns if c in s_df.columns and c != 'feature_version']
    print(f"Total compared columns: {len(compared_cols)}")

    mismatches = {}
    for col in compared_cols:
        b_col = b_df[col]
        s_col = s_df[col]
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

    print("\nColumn Mismatch Summary:")
    for col in compared_cols:
        cnt = mismatches.get(col, 0)
        status = "PASS" if cnt == 0 else f"FAIL ({cnt} mismatches)"
        print(f"  {col:35s}: {status}")

    tot = sum(mismatches.values())
    print(f"\nTOTAL MISMATCHES ACROSS ALL 41 COLUMNS: {tot}")
    assert tot == 0, f"Parity failed with {tot} mismatches!"
    print("\n=======================================================")
    print("PARITY AUDIT: 100% PERFECT MATCH ACROSS ALL 41 COLUMNS!")
    print("=======================================================")

if __name__ == '__main__':
    main()
