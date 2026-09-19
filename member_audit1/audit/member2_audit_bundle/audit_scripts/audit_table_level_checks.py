#!/usr/bin/env python3
"""
Re-runnable table-level audit of Member 1's deliverables (PaySim + IEEE-CIS Parquet).
Uses DuckDB (no Spark needed) so it runs on a laptop.

  pip install duckdb pyarrow pandas
  python audit_table_level_checks.py --paysim path/to/paysim_clean.parquet --ieee path/to/ieee_clean.parquet --out audit_results.json
"""
import argparse, json, glob, duckdb

ap = argparse.ArgumentParser()
ap.add_argument("--paysim", required=True); ap.add_argument("--ieee", required=True)
ap.add_argument("--out", default="audit_results.json")
a = ap.parse_args()
R = {}
def con_for(path, name):
    c = duckdb.connect(); c.execute("PRAGMA memory_limit='2GB'"); c.execute("PRAGMA threads=1")
    c.execute(f"CREATE VIEW {name} AS SELECT * FROM read_parquet('{path}/part-*.parquet')")
    return c

# ---------------- PaySim ----------------
p = con_for(a.paysim, "p"); ps = {}
ps["rows"] = p.execute("SELECT COUNT(*) FROM p").fetchone()[0]
cols = [r[0] for r in p.execute("DESCRIBE p").fetchall()]
ps["columns"] = cols
ps["null_counts"] = dict(zip(cols, p.execute("SELECT " + ",".join(f"COUNT(*)-COUNT({c})" for c in cols) + " FROM p").fetchone()))
ps["full_row_duplicates"] = ps["rows"] - p.execute("SELECT COUNT(*) FROM (SELECT DISTINCT * FROM p)").fetchone()[0]
ps["negative_numeric_rows"] = p.execute("SELECT COUNT(*) FROM p WHERE amount<0 OR oldbalanceOrg<0 OR newbalanceOrig<0 OR oldbalanceDest<0 OR newbalanceDest<0").fetchone()[0]
ps["zero_amount_rows"] = p.execute("SELECT COUNT(*), SUM(isFraud) FROM p WHERE amount=0").fetchone()
ps["step_hour_mismatch_rows"] = p.execute("SELECT COUNT(*) FROM p WHERE step_hour <> step % 24").fetchone()[0]
ps["fraud"] = p.execute("SELECT SUM(isFraud), 100.0*AVG(isFraud), SUM(isFlaggedFraud) FROM p").fetchone()
ps["fraud_by_type"] = p.execute("SELECT type, COUNT(*), SUM(isFraud) FROM p GROUP BY 1 ORDER BY 2 DESC").fetchall()
ps["distinct_nameOrig_nameDest"] = p.execute("SELECT COUNT(DISTINCT nameOrig), COUNT(DISTINCT nameDest) FROM p").fetchone()
ps["nameOrig_txn_count_histogram"] = p.execute("WITH c AS (SELECT nameOrig, COUNT(*) n FROM p GROUP BY 1) SELECT n, COUNT(*) FROM c GROUP BY 1 ORDER BY 1").fetchall()
ps["drain_rule_fraud_vs_legit"] = p.execute("SELECT isFraud, COUNT(*) FROM p WHERE oldbalanceOrg>0 AND abs(amount-oldbalanceOrg)<0.005 GROUP BY 1").fetchall()
ps["fraud_by_step_volume_bucket"] = p.execute("""WITH s AS (SELECT step, COUNT(*) n, SUM(isFraud) f FROM p GROUP BY 1)
  SELECT CASE WHEN n<100 THEN '<100' WHEN n<1000 THEN '100-999' WHEN n<10000 THEN '1k-9.9k' WHEN n<30000 THEN '10k-29.9k' ELSE '30k+' END b,
         COUNT(*) steps, SUM(n) txns, SUM(f) fraud FROM s GROUP BY 1 ORDER BY MIN(n)""").fetchall()
R["paysim"] = ps

# ---------------- IEEE ----------------
i = con_for(a.ieee, "i"); ie = {}
ie["rows"], ie["distinct_TransactionID"] = i.execute("SELECT COUNT(*), COUNT(DISTINCT TransactionID) FROM i").fetchone()
ie["columns"] = len(i.execute("DESCRIBE i").fetchall())
ie["fraud"] = i.execute("SELECT SUM(isFraud), 100.0*AVG(isFraud) FROM i").fetchone()
ie["ProductCD"] = i.execute("SELECT ProductCD, COUNT(*), SUM(isFraud) FROM i GROUP BY 1 ORDER BY 2 DESC").fetchall()
ie["amount_min_max_nonpositive"] = i.execute("SELECT MIN(TransactionAmt), MAX(TransactionAmt), SUM(CASE WHEN TransactionAmt<=0 THEN 1 ELSE 0 END) FROM i").fetchone()
idc = [f"id_{k:02d}" for k in range(1, 39)] + ["DeviceType", "DeviceInfo"]
cond = " OR ".join(f"{c} IS NOT NULL" for c in idc)
ie["rows_with_any_identity_field"] = i.execute(f"SELECT SUM(CASE WHEN {cond} THEN 1 ELSE 0 END) FROM i").fetchone()[0]
ie["fraud_rate_with_vs_without_identity"] = i.execute(f"SELECT CASE WHEN {cond} THEN 'has_identity' ELSE 'no_identity' END, COUNT(*), 100.0*AVG(isFraud) FROM i GROUP BY 1").fetchall()
ie["near_duplicates_excl_TransactionID"] = i.execute("WITH x AS (SELECT * EXCLUDE (TransactionID) FROM i) SELECT COUNT(*) - COUNT(DISTINCT x) FROM x").fetchone()[0]
ie["null_pct_key_cols"] = i.execute("SELECT 100.0*(COUNT(*)-COUNT(TransactionID))/COUNT(*), 100.0*(COUNT(*)-COUNT(isFraud))/COUNT(*), 100.0*(COUNT(*)-COUNT(TransactionAmt))/COUNT(*), 100.0*(COUNT(*)-COUNT(ProductCD))/COUNT(*), 100.0*(COUNT(*)-COUNT(card1))/COUNT(*) FROM i").fetchone()
ie["DT_span_days"] = i.execute("SELECT MIN(TransactionDT)/86400.0, MAX(TransactionDT)/86400.0 FROM i").fetchone()
R["ieee"] = ie
json.dump(R, open(a.out, "w"), indent=2, default=str)
print(json.dumps(R, indent=2, default=str)[:3000])
