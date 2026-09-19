"""Step 2: add family / missingness-lift columns and compute minority-value fraud lift for near-constant columns.
Usage: python derive_ieee_tables.py <ieee_clean.parquet dir> <out_dir>   (out_dir must contain ieee_column_profile.csv from step 1)"""
import sys, re, duckdb, pandas as pd
IEEE_DIR, OUT = sys.argv[1], sys.argv[2]
df = pd.read_csv(f"{OUT}/ieee_column_profile.csv")
def fam(c):
    if re.fullmatch(r"V\d+",c): return "V"
    if re.fullmatch(r"C\d+",c): return "C"
    if re.fullmatch(r"D\d+",c): return "D"
    if re.fullmatch(r"M\d",c): return "M"
    if re.fullmatch(r"id_\d+",c): return "id"
    if c.startswith("card"): return "card"
    if c.startswith("addr") or c.startswith("dist"): return "addr/dist"
    if "email" in c: return "email"
    if c.startswith("Device"): return "device"
    return "core"
df["family"] = df.column.map(fam)
df["auc_dev"] = (df.auc_nonnull-0.5).abs()
df["miss_lift"] = df.fraud_rate_if_null - df.fraud_rate_if_present
df["miss_abs"] = df.miss_lift.abs()
df.to_csv(f"{OUT}/ieee_column_profile3.csv", index=False)
con = duckdb.connect(); con.execute("PRAGMA memory_limit=\'2GB\'"); con.execute("PRAGMA threads=1")
con.execute(f"CREATE VIEW i AS SELECT * FROM read_parquet(\'{IEEE_DIR}/part-*.parquet\')")
base = 3.499
nc = df[(((df.top1_share_of_nonnull>=99)|(df.top1_share_of_all>=95)) & (df.column!="isFraud") & (df.family!="id")) | (df.column=="id_27")]
rows=[]
for _,r in nc.iterrows():
    c=r["column"]; top=r["top1_value"]
    cond = f"{c} = {top}" if (r["arrow_type"].startswith("double") or r["arrow_type"].startswith("int")) else f"{c} = \'{top}\'"
    q = con.execute(f"SELECT COUNT(*) FILTER (WHERE {c} IS NOT NULL AND NOT ({cond})), AVG(isFraud) FILTER (WHERE {c} IS NOT NULL AND NOT ({cond}))*100, AVG(isFraud) FILTER (WHERE {c} IS NOT NULL AND ({cond}))*100 FROM i").fetchone()
    rows.append((c, r["null_pct"], top, r["top1_share_of_nonnull"], q[0], q[1], q[2]))
m = pd.DataFrame(rows, columns=["column","null_pct","top1","top1_share_nonnull","minority_n","minority_fraud_pct","majority_fraud_pct"])
m["lift"] = m.minority_fraud_pct/base
m.to_csv(f"{OUT}/near_constant_minority.csv", index=False)
print("wrote ieee_column_profile3.csv and near_constant_minority.csv")
