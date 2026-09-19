"""Step 1: exact per-column profile of ieee_clean.parquet.
Usage: python profile_ieee.py <ieee_clean.parquet dir> <out_dir>"""
import pyarrow.parquet as pq, pyarrow.compute as pc, pyarrow as pa, numpy as np, pandas as pd, glob, hashlib, json, time, sys, os
IEEE_DIR, OUT = sys.argv[1], sys.argv[2]; os.makedirs(OUT, exist_ok=True)
files = sorted(glob.glob(f'{IEEE_DIR}/part-*.parquet'))
def col(name):
    t = pa.concat_tables([pq.read_table(f, columns=[name]) for f in files])
    return t.column(0).combine_chunks()
schema = pq.ParquetFile(files[0]).schema_arrow
names = schema.names
y = col('isFraud').to_numpy(zero_copy_only=False).astype(np.int8)
n = len(y); pos = y.sum()
def auc(x, mask):
    # AUC on rows where mask (non-null); x numeric
    xv = x[mask]; yv = y[mask]
    p = yv.sum(); q = len(yv)-p
    if p==0 or q==0 or len(np.unique(xv))<2: return np.nan
    order = np.argsort(xv, kind='mergesort')
    ranks = np.empty(len(xv)); 
    # average ranks for ties
    s = xv[order]; r = np.arange(1,len(xv)+1, dtype=float)
    # tie handling
    uniq, idx, cnt = np.unique(s, return_index=True, return_counts=True)
    avg = idx + (cnt+1)/2.0
    r = np.repeat(avg, cnt)
    ranks[order] = r
    return (ranks[yv==1].sum() - p*(p+1)/2) / (p*q)
rows=[]; maskhash={}; valhash={}; topvals={}
t0=time.time()
for k,name in enumerate(names):
    a = col(name)
    typ = str(a.type)
    nn = a.null_count
    d = {'column':name,'arrow_type':typ,'null_count':nn,'null_pct':100*nn/n}
    m_null = a.is_null().to_numpy(zero_copy_only=False)
    maskhash.setdefault(hashlib.md5(np.packbits(m_null).tobytes()).hexdigest(), []).append(name)
    d['mask_hash']=hashlib.md5(np.packbits(m_null).tobytes()).hexdigest()[:10]
    # missingness signal
    if 0<nn<n:
        d['fraud_rate_if_null']=100*y[m_null].mean(); d['fraud_rate_if_present']=100*y[~m_null].mean()
    d['distinct_nonnull']=int(pc.count_distinct(a, mode='only_valid').as_py())
    vc = pc.value_counts(a.drop_null()) if nn<n else None
    if vc is not None and len(vc)>0:
        cnts = vc.field('counts').to_numpy(); vals = vc.field('values').to_pylist()
        o = np.argsort(-cnts)[:5]
        d['top1_value']=str(vals[o[0]]); d['top1_share_of_nonnull']=100*cnts[o[0]]/(n-nn); d['top1_share_of_all']=100*cnts[o[0]]/n
        topvals[name]=[(str(vals[i]), int(cnts[i])) for i in o]
    if pa.types.is_integer(a.type) or pa.types.is_floating(a.type):
        x = a.to_numpy(zero_copy_only=False).astype(np.float64)
        ok = ~np.isnan(x)
        valhash.setdefault(hashlib.md5(np.nan_to_num(x, nan=-9.87654321e9).tobytes()).hexdigest(), []).append(name)
        xv = x[ok]
        d.update(min=float(xv.min()), max=float(xv.max()), mean=float(xv.mean()), std=float(xv.std()), median=float(np.median(xv)))
        d['pct_zero_of_nonnull']=100*float((xv==0).mean())
        d['pct_negative']=100*float((xv<0).mean())
        d['all_integer_valued']=bool(np.all(xv==np.round(xv)))
        d['auc_nonnull']=auc(x, ok)
        if xv.std()>0: d['pearson_isFraud_nonnull']=float(np.corrcoef(xv, y[ok])[0,1])
    else:
        valhash.setdefault(hashlib.md5(pc.fill_null(a,'<NA>').to_pandas().astype(str).str.cat(sep='|').encode()).hexdigest(), []).append(name)
        # fraud rate by category (top 3)
        s = pd.Series(a.to_pandas()); 
        g = pd.DataFrame({'v':s,'y':y}).groupby('v',observed=True)['y'].agg(['mean','count'])
        d['max_cat_fraud_rate_min1000']=100*float(g[g['count']>=1000]['mean'].max()) if (g['count']>=1000).any() else np.nan
        d['min_cat_fraud_rate_min1000']=100*float(g[g['count']>=1000]['mean'].min()) if (g['count']>=1000).any() else np.nan
    rows.append(d)
    if k%40==0: print(k, name, round(time.time()-t0,1), flush=True)
df = pd.DataFrame(rows)
df.to_csv(f'{OUT}/ieee_column_profile.csv', index=False)
json.dump({'mask_groups':[v for v in maskhash.values()], 'dup_columns':[v for v in valhash.values() if len(v)>1], 'topvals':topvals}, open(f'{OUT}/ieee_groups.json','w'))
print('DONE', round(time.time()-t0,1), flush=True)
