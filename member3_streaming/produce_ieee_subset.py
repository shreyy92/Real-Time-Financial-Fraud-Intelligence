"""Deterministic Long-Span Subset Producer for Gate 1.4 and Gate 1.5.

Selects cards deterministically from data/features/ieee_features.parquet:
(a) Top 10 cards with highest max card_txn_count_7d
(b) Up to 30 cards with same-second ties (ranked by tie transaction count)
(c) Up to 30 cards spanning >= 30 days with >= 200 rows (ranked by total rows)
Interleaves candidates across (a), (b), (c) in round-robin order and caps
at 40,000 rows total, dropping lowest-ranked cards when the cap is reached.

Extracts the corresponding raw records from data/processed/ieee_clean.parquet,
stably sorts globally by (TransactionDT, TransactionID), and streams to Kafka
topic `financial_transactions_ieee` using producer_ieee serialization.
"""

import os
import sys
import time
import argparse
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from kafka import KafkaProducer

from member3_streaming.producer_ieee import sanitize_record, json_serializer


def select_subset_cards(features_path: str = "data/features/ieee_features.parquet", row_cap: int = 40000):
    cols = ["card_proxy_id", "TransactionDT", "TransactionID", "card_txn_count_7d"]
    table = pq.read_table(features_path, columns=cols)
    df = table.to_pandas()

    card_counts = df.groupby("card_proxy_id").size()
    card_max_7d = df.groupby("card_proxy_id")["card_txn_count_7d"].max()
    card_spans = df.groupby("card_proxy_id")["TransactionDT"].agg(lambda s: s.max() - s.min())

    # (a) Top 10 cards with highest max card_txn_count_7d
    a_candidates = card_max_7d.sort_values(ascending=False).head(10).index.tolist()

    # (b) Up to 30 cards with same-second ties
    tie_df = df.groupby(["card_proxy_id", "TransactionDT"]).size()
    cards_with_ties = tie_df[tie_df > 1].reset_index()["card_proxy_id"].value_counts()
    b_candidates = [c for c in cards_with_ties.index if c not in a_candidates][:30]

    # (c) Up to 30 cards spanning >= 30 days with >= 200 rows
    cond_c = (card_spans >= 30 * 86400) & (card_counts >= 200)
    c_candidates = [c for c in card_counts[cond_c].sort_values(ascending=False).index if c not in a_candidates and c not in b_candidates][:30]

    # Interleaved round-robin ranking: A[0], B[0], C[0], A[1], B[1], C[1]...
    ranked = []
    max_len = max(len(a_candidates), len(b_candidates), len(c_candidates))
    for i in range(max_len):
        if i < len(a_candidates):
            ranked.append(("A", a_candidates[i]))
        if i < len(b_candidates):
            ranked.append(("B", b_candidates[i]))
        if i < len(c_candidates):
            ranked.append(("C", c_candidates[i]))

    total_rows = 0
    selected_cards = []
    for cat, cid in ranked:
        cnt = card_counts[cid]
        if total_rows + cnt <= row_cap:
            selected_cards.append((cat, cid, cnt))
            total_rows += cnt

    return selected_cards, df


def main():
    parser = argparse.ArgumentParser(description="Deterministic IEEE Subset Producer")
    parser.add_argument("--bootstrap-servers", type=str, default="127.0.0.1:9092", help="Kafka broker")
    parser.add_argument("--topic", type=str, default="financial_transactions_ieee", help="Kafka topic")
    parser.add_argument("--delay", type=float, default=0.0, help="Delay between messages in seconds")
    parser.add_argument("--cap", type=int, default=40000, help="Row cap (default: 40000)")
    args = parser.parse_args()

    features_path = "data/features/ieee_features.parquet"
    clean_path = "data/processed/ieee_clean.parquet"

    print("=" * 80)
    print("DETERMINISTIC SUBSET SELECTION FOR LONG-SPAN AUDIT (GATE 1.4 / 1.5)")
    print(f"Features: {features_path}")
    print(f"Clean:    {clean_path}")
    print(f"Row Cap:  {args.cap}")
    print("=" * 80)

    selected_cards, feat_df = select_subset_cards(features_path, row_cap=args.cap)
    cats = [cat for cat, _, _ in selected_cards]
    print(f"Selection Summary:")
    print(f"  Total Cards: {len(selected_cards)}")
    print(f"  Category A (Top 7d):           {cats.count('A')} cards")
    print(f"  Category B (Ties):             {cats.count('B')} cards")
    print(f"  Category C (>=30d, >=200 txns):{cats.count('C')} cards")

    total_expected_rows = sum(cnt for _, _, cnt in selected_cards)
    print(f"  Total Rows:  {total_expected_rows} / {args.cap}")

    card_ids = set([cid for _, cid, _ in selected_cards])
    selected_tids = set(feat_df[feat_df["card_proxy_id"].isin(card_ids)]["TransactionID"].values)
    assert len(selected_tids) == total_expected_rows

    print(f"\nLoading raw transaction rows from {clean_path}...")
    clean_table = pq.read_table(clean_path, filters=[("TransactionID", "in", selected_tids)])
    clean_df = clean_table.to_pandas()
    assert len(clean_df) == total_expected_rows, f"Loaded {len(clean_df)} != expected {total_expected_rows}"

    print("Sorting globally by (TransactionDT, TransactionID) with stable sort...")
    sort_idx = np.lexsort((clean_df["TransactionID"].values, clean_df["TransactionDT"].values))
    clean_sorted = clean_df.iloc[sort_idx]

    print(f"Connecting to Kafka at {args.bootstrap_servers}...")
    producer = KafkaProducer(
        bootstrap_servers=[args.bootstrap_servers],
        key_serializer=lambda k: str(k).encode("utf-8") if k is not None else None,
        value_serializer=json_serializer,
    )

    print(f"Producing {len(clean_sorted)} events to topic: {args.topic}...")
    records = clean_sorted.to_dict(orient="records")
    t0 = time.time()
    for i, rec in enumerate(records, 1):
        clean_msg = sanitize_record(rec)
        key = str(clean_msg.get("card1", ""))
        producer.send(args.topic, key=key, value=clean_msg)

        if i % 10000 == 0 or i == len(records):
            print(f"  Sent {i}/{len(records)} events ({time.time() - t0:.1f}s)...")

        if args.delay > 0:
            time.sleep(args.delay)

    producer.flush()
    producer.close()
    print(f"Done. Successfully emitted {len(records)} events in {time.time() - t0:.2f}s.")


if __name__ == "__main__":
    main()
