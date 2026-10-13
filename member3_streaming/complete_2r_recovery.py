import os
import sys
import time
import json
import subprocess
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from kafka import KafkaConsumer, TopicPartition

def main():
    chk_dir = os.path.abspath('member3_streaming/checkpoints/paysim')

    # 1. Roll back orphan uncommitted offset file
    print('Applying orphan-file rollback workaround...')
    for q in ['features', 'dlq']:
        q_offsets = os.path.join(chk_dir, q, 'offsets')
        q_commits = os.path.join(chk_dir, q, 'commits')
        if os.path.exists(q_offsets) and os.path.exists(q_commits):
            for f in os.listdir(q_offsets):
                if f.isdigit():
                    commit_file = os.path.join(q_commits, f)
                    if not os.path.exists(commit_file):
                        print(f'Removing uncommitted offset file {q}/offsets/{f}...')
                        os.remove(os.path.join(q_offsets, f))
                        crc = os.path.join(q_offsets, f'.{f}.crc')
                        if os.path.exists(crc):
                            os.remove(crc)

    # 2. Restart consumer to complete processing
    restart_cmd = [
        sys.executable, '-u', '-m', 'member3_streaming.consumer_paysim',
        '--bootstrap-servers', 'localhost:9092',
        '--input-topic', 'financial_transactions_paysim',
        '--output-topic', 'financial_features_paysim',
        '--dlq-topic', 'financial_transactions_paysim_dlq',
        '--checkpoint-location', chk_dir,
        '--max-offsets-per-trigger', '1000',
    ]
    f_log = open('member3_streaming/consumer_restart_fix.log', 'w', encoding='utf-8')
    p_fix = subprocess.Popen(restart_cmd, stdout=f_log, stderr=subprocess.STDOUT)
    print(f'Restarted consumer (PID: {p_fix.pid}). Waiting for completion...')

    consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'])
    tp_out = TopicPartition('financial_features_paysim', 0)

    offsets_dir = os.path.join(chk_dir, 'features', 'offsets')
    commits_dir = os.path.join(chk_dir, 'features', 'commits')

    t0 = time.time()
    while time.time() - t0 < 300:
        time.sleep(4)
        curr_out = consumer.end_offsets([tp_out]).get(tp_out, 0)
        print(f'  Current output offset: {curr_out}...')
        if os.path.exists(offsets_dir):
            nums = sorted([int(f) for f in os.listdir(offsets_dir) if f.isdigit()])
            if nums:
                latest = nums[-1]
                off_f = os.path.join(offsets_dir, str(latest))
                comm_f = os.path.join(commits_dir, str(latest))
                if os.path.exists(comm_f):
                    with open(off_f, 'r') as f:
                        data = f.read()
                        if '"0":20000' in data:
                            print(f'Input offset 20,000 committed at batch {latest}!')
                            time.sleep(3)
                            break

    consumer.close()
    subprocess.run(['taskkill', '/F', '/T', '/PID', str(p_fix.pid)], capture_output=True)
    f_log.close()
    time.sleep(2)

    # 3. Read and audit all records
    c = KafkaConsumer('financial_features_paysim', bootstrap_servers=['localhost:9092'], auto_offset_reset='earliest', consumer_timeout_ms=5000)
    records = []
    for msg in c:
        records.append((msg.key.decode('utf-8') if msg.key else None, json.loads(msg.value.decode('utf-8')), msg.value))
    c.close()

    total_recs = len(records)
    print(f'Total output records: {total_recs}')

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
    print(f'Distinct txn_id count: {distinct_count} (expected 20000)')
    print(f'Duplicate count: {dup_count}')

    byte_identical = True
    for tid, b1, b2 in duplicates:
        if b1 != b2:
            byte_identical = False
            print(f'Mismatch in duplicate bytes for {tid}!')
            break
    print(f'All duplicates byte-identical: {byte_identical}')

    # 4. Parity check vs parquet
    table = pq.read_table('data/features/paysim_features.parquet')
    batch_df = table.slice(0, 20000).to_pandas()
    stream_rows = [r for (k, r, b) in records]
    stream_df = pd.DataFrame(stream_rows).drop_duplicates(subset=['txn_id']).sort_values('txn_id').reset_index(drop=True)
    batch_df = batch_df.sort_values('txn_id').reset_index(drop=True)

    compared_cols = [col for col in batch_df.columns if col in stream_df.columns and col != 'feature_version']
    print(f'Total compared columns: {len(compared_cols)}')

    mismatches = {}
    for col in compared_cols:
        b_col = batch_df[col]
        s_col = stream_df[col]
        if np.issubdtype(b_col.dtype, np.floating):
            null_ok = (b_col.isna() == s_col.isna()).all()
            if not null_ok:
                mismatches[col] = int((b_col.isna() != s_col.isna()).sum())
                continue
            valid = b_col.notna()
            diff = np.abs(b_col[valid] - s_col[valid])
            cnt = int((diff > 1e-4).sum())
            if cnt > 0:
                mismatches[col] = cnt
        else:
            diff = (b_col != s_col)
            cnt = int(diff.sum())
            if cnt > 0:
                mismatches[col] = cnt

    print('Mismatch summary:')
    for col in compared_cols:
        c_m = mismatches.get(col, 0)
        print(f'  {col:35s}: {"PASS" if c_m == 0 else f"FAIL ({c_m})" }')

    tot_mismatches = sum(mismatches.values())
    print(f'TOTAL MISMATCHES ACROSS ALL 41 COLUMNS: {tot_mismatches}')
    assert tot_mismatches == 0, 'Parity check failed!'
    assert distinct_count == 20000, 'Distinct count mismatch!'
    assert byte_identical, 'Duplicates not byte-identical!'
    print('CHUNK 2R 20,000 ROW RECOVERY: 100% PASSED!')

if __name__ == '__main__':
    main()
