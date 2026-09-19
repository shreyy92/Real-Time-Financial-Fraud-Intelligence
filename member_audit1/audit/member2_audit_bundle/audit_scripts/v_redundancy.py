#!/usr/bin/env python3
"""Within each identical-null-mask block of V columns, greedily map columns to a representative
when |Pearson r| >= 0.98 on a 25% seeded sample. Advisory only: RECOMPUTE ON THE TRAINING WINDOW before dropping anything.
Needs ieee_column_profile3.csv (from profile_ieee.py) for mask_hash / null_pct."""
import pyarrow.parquet as pq, pandas as pd, numpy as np, glob, sys
ieee_dir, prof_csv, out_csv = sys.argv[1], sys.argv[2], sys.argv[3]
vcols=[f"V{k}" for k in range(1,340)]
S=pd.concat([pq.read_table(f,columns=vcols).to_pandas().sample(frac=0.25,random_state=42).astype('float32') for f in sorted(glob.glob(f"{ieee_dir}/part-*.parquet"))])
prof=pd.read_csv(prof_csv).set_index('column'); blocks={}
for c in vcols: blocks.setdefault(prof.loc[c,'mask_hash'],[]).append(c)
rows=[]
for h,cols in blocks.items():
    sub=S[cols].dropna(); const=[c for c in cols if sub[c].std()==0]; sub=sub.loc[:,sub.std()>0]; C=sub.corr().abs().fillna(0); kept=[]
    for c in sub.columns:
        best=next((k for k in kept if C.loc[c,k]>=0.98),None)
        if best is None: kept.append(c); rows.append((c,c,1.0,h,prof.loc[c,'null_pct']))
        else: rows.append((c,best,float(C.loc[c,best]),h,prof.loc[c,'null_pct']))
    rows += [(c,'CONSTANT_IN_SAMPLE',np.nan,h,prof.loc[c,'null_pct']) for c in const]
pd.DataFrame(rows,columns=['column','representative_at_r>=0.98','abs_corr','mask_hash','null_pct']).to_csv(out_csv,index=False)
