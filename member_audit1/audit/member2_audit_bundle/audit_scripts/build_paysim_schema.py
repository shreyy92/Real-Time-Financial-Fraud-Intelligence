"""Build paysim_schema.csv from the Parquet files. Usage: python build_paysim_schema.py <paysim_clean.parquet dir> <out_dir>"""
import sys, duckdb, pandas as pd
P, OUT = sys.argv[1], sys.argv[2]
desc = {
 "step":("int32","Simulation time unit: 1 step = 1 hour (range 1-743, ~31 days). No calendar timestamp.","TIME: ordering key + window axis; not an ML feature directly (drift proxy)"),
 "type":("string","Transaction type: CASH_IN, CASH_OUT, DEBIT, PAYMENT, TRANSFER","CATEGORICAL (5 levels); fraud only in TRANSFER/CASH_OUT"),
 "amount":("double","Transaction amount in local currency","NUMERIC: heavy right-skew -> log1p; ratios to balances"),
 "nameOrig":("string","Originating (sender) customer ID (always 'C' prefix)","IDENTIFIER (entity key). NOT a model feature. 99.85% of IDs appear once"),
 "oldbalanceOrg":("double","Sender balance BEFORE the transaction","NUMERIC: pre-txn state, available at scoring time"),
 "newbalanceOrig":("double","Sender balance AFTER the transaction","POST-TXN STATE: leakage-prone (see Part 7); prefer derived deltas with explicit caveat"),
 "nameDest":("string","Recipient ID ('C' = customer, 'M' = merchant)","IDENTIFIER (entity key); source of fan-in/first-seen features. NOT a raw feature"),
 "oldbalanceDest":("double","Recipient balance BEFORE the transaction (0 for all merchants)","NUMERIC: pre-txn state; zeros are structural for merchants"),
 "newbalanceDest":("double","Recipient balance AFTER the transaction (0 for all merchants)","POST-TXN STATE: leakage-prone"),
 "isFraud":("int32","TARGET: 1 = fraudulent transaction, 0 = legitimate","TARGET LABEL"),
 "isFlaggedFraud":("int32 (NOT NULL)","Simulator's own rule-based flag; fires on only 16 rows (all TRANSFER, all fraud)","EXCLUDE from ML features (near-constant; rule-engine output). Keep only as rules-baseline reference"),
 "step_hour":("int32","Member 1 derived: step % 24 (hour-of-day proxy; 0-23)","TEMPORAL (cyclical). Semantics slightly ambiguous (see Part 2)"),
}
con = duckdb.connect(); con.execute("PRAGMA memory_limit=\'2GB\'"); con.execute("PRAGMA threads=1")
con.execute(f"CREATE VIEW p AS SELECT * FROM read_parquet(\'{P}/part-*.parquet\')")
n = con.execute("SELECT COUNT(*) FROM p").fetchone()[0]; rows=[]
for c,(dt,d,role) in desc.items():
    r = con.execute(f"SELECT COUNT(*)-COUNT({c}), COUNT(DISTINCT {c}), MIN({c}), MAX({c}) FROM p").fetchone()
    rows.append(dict(dataset="paysim",column=c,data_type=dt,null_count=r[0],null_pct=100*r[0]/n,exact_unique=r[1],min=r[2],max=r[3],description=d,potential_feature_role=role))
pd.DataFrame(rows).to_csv(f"{OUT}/paysim_schema.csv", index=False)
