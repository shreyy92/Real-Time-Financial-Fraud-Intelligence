"""Step 4: build ieee_schema.csv + ieee_missingness_groups.csv.
Run from inside <out_dir> (needs ieee_column_profile3.csv, near_constant_minority.csv, v_redundancy_r098.csv there)."""
import pandas as pd, numpy as np, re, json
prof = pd.read_csv('ieee_column_profile3.csv')
nc   = pd.read_csv('near_constant_minority.csv').set_index('column')
vred = pd.read_csv('v_redundancy_r098.csv').set_index('column')
N = 590540
def fam(c):
    if re.fullmatch(r'V\d+',c): return 'V'
    if re.fullmatch(r'C\d+',c): return 'C'
    if re.fullmatch(r'D\d+',c): return 'D'
    if re.fullmatch(r'M\d',c): return 'M'
    if re.fullmatch(r'id_\d+',c): return 'id'
    if re.fullmatch(r'card\d',c): return 'card'
    if c in('addr1','addr2'): return 'addr'
    if c in('dist1','dist2'): return 'dist'
    if 'email' in c: return 'email'
    if c.startswith('Device'): return 'device'
    return 'core'
prof['family']=prof.column.map(fam)
# ---- mask groups (exact identical null pattern) ----
grp = {}
for h,g in prof.groupby('mask_hash'): grp[h]=g.column.tolist()
def rng(cols):
    vs=sorted(int(c[1:]) for c in cols if re.fullmatch(r'V\d+',c)); out=[]
    for v in vs:
        if out and v==out[-1][1]+1: out[-1][1]=v
        else: out.append([v,v])
    s=",".join(f"V{a}-V{b}" if a!=b else f"V{a}" for a,b in out)
    others=[c for c in cols if not re.fullmatch(r'V\d+',c)]
    return ",".join(([s] if s else [])+others)
gid={}; mrows=[]
for k,(h,cols) in enumerate(sorted(grp.items(), key=lambda kv: (prof.set_index('column').loc[kv[1][0],'null_pct'], kv[1][0]))):
    nullp = prof.set_index('column').loc[cols[0],'null_pct']
    gid[h]=f"MG{k:02d}"
    mrows.append(dict(mask_group=f"MG{k:02d}", null_pct=round(nullp,3), n_columns=len(cols), members=rng(cols),
        fraud_rate_if_null=prof.set_index('column').loc[cols[0],'fraud_rate_if_null'], fraud_rate_if_present=prof.set_index('column').loc[cols[0],'fraud_rate_if_present']))
mg = pd.DataFrame(mrows)
mg['indicator_recommended'] = np.where((mg.null_pct>=5)&(mg.null_pct<99.5)&(mg.n_columns>=1),'YES','NO')
mg.to_csv('ieee_missingness_groups.csv', index=False)
prof['mask_group']=prof.mask_hash.map(lambda h: gid[next(k for k in gid if k[:10]==h)])

CAT_DECLARED = set(['ProductCD','card1','card2','card3','card4','card5','card6','addr1','addr2','P_emaildomain','R_emaildomain']+[f'M{i}' for i in range(1,10)]+['DeviceType','DeviceInfo']+[f'id_{i}' for i in range(12,39)])
HIGHCARD = {'card1','card2','DeviceInfo','id_31','id_33','id_30','P_emaildomain','R_emaildomain','addr1','id_02'}
INVESTIGATE = {'id_02':"ID-like: 115,655 distinct in 140,872 non-null; behaves like a hashed identifier, not a magnitude",
               'D8':"Non-integer; null mask identical to D9/id_09/id_10 (74,926 rows, all inside identity rows) - coupling to identity table unexplained",
               'D9':"Range 0-0.958 (fraction-like); mask identical to D8/id_09/id_10",
               'id_07':"99.1% null; AUC 0.40 on ~5.1k rows - possible real signal, tiny coverage",
               'id_08':"99.1% null; AUC 0.44 on ~5.1k rows",
               'id_22':"99.1% null; AUC 0.56 on ~5.1k rows",
               'id_26':"99.1% null; AUC 0.60 on ~5.1k rows"}
def treat(r):
    c=r['column']; nullp=r['null_pct']; f=r['family']
    if c=='TransactionID': return ('EXCLUDE_IDENTIFIER','Primary key; join key only. Never a model feature.')
    if c=='isFraud': return ('TARGET','Label.')
    if c=='TransactionDT': return ('DERIVE_TIME_FEATURES','Seconds since an undisclosed reference; use for ordering/splits and derive hour/day features; do not feed raw to ML (monotone drift proxy).')
    if c in INVESTIGATE: return ('5_NEEDS_INVESTIGATION',INVESTIGATE[c])
    lift = abs(r['fraud_rate_if_null']-r['fraud_rate_if_present']) if pd.notna(r.get('fraud_rate_if_null')) else 0
    if c in nc.index and c not in CAT_DECLARED:
        mn=nc.loc[c,'minority_n']; lf=nc.loc[c,'lift']
        if mn>=500: return ('1_KEEP_IMPUTE',f"Near-constant ({nc.loc[c,'top1_share_nonnull']:.2f}% one value) BUT minority rows n={int(mn)} have fraud rate {nc.loc[c,'minority_fraud_pct']:.2f}% ({lf:.2f}x base) - keep; consider binary 'is_minority' flag.")
        tail = "Its missingness is captured by the block indicator, not by this column." if nullp>=5 else "Null rate <1% so no missingness signal either."
        return ('4_DROP_LOW_INFO',f"Near-constant ({nc.loc[c,'top1_share_nonnull']:.3f}% one value); only {int(mn)} minority rows. {tail}")
    if c=='id_27': return ('4_DROP_LOW_INFO','99.1% null and 99.7% one value (14 minority rows).')
    if c in CAT_DECLARED:
        if nullp>=99: return ('3_KEEP_CATEGORICAL','Categorical; 99% null -> missing level dominates; keep only if cheap.')
        return ('3_KEEP_CATEGORICAL', ('High-cardinality: frequency encode / train-only target-encode; never one-hot' if c in HIGHCARD else 'Low-cardinality: label/one-hot; encode NaN as explicit MISSING level'))
    # numeric
    if nullp<1: return ('1_KEEP_IMPUTE','Negligible sparsity; median impute (or leave NaN for tree models).')
    if nullp>=99: 
        if pd.notna(r['auc_nonnull']) and abs(r['auc_nonnull']-0.5)>=0.05: return ('5_NEEDS_INVESTIGATION',f"99% null but AUC {r['auc_nonnull']:.2f} on non-null rows")
        return ('4_DROP_LOW_INFO','>=99% null and AUC within 0.05 of 0.5; rely on identity-block indicator.')
    if nullp>=5 and lift>=2: return ('2_KEEP_MISSING_INDICATOR',f"{nullp:.1f}% null; fraud rate {r['fraud_rate_if_null']:.2f}% if null vs {r['fraud_rate_if_present']:.2f}% if present ({lift:.1f}pp gap). Use block-level indicator ({r['mask_group']}).")
    return ('1_KEEP_IMPUTE',f"{nullp:.1f}% null; missingness weakly informative ({lift:.1f}pp).")
t = prof.apply(treat, axis=1, result_type='expand'); prof['treatment']=t[0]; prof['treatment_reason']=t[1]

# ----- descriptions & roles -----
def describe(c,f):
    d={
    'TransactionID':("Unique transaction key (join key to identity table)","identifier"),
    'isFraud':("Target label (1 = fraud)","target"),
    'TransactionDT':("Timedelta in seconds from an undisclosed reference datetime (not a real timestamp)","time"),
    'TransactionAmt':("Transaction payment amount (USD per competition description)","numeric-continuous; log1p; amount decimals"),
    'ProductCD':("Product code of the transaction (C/H/R/S/W)","categorical-low-card"),
    'card1':("Payment-card attribute (masked; competition lists as categorical)","categorical-high-card / entity proxy"),
    'card2':("Payment-card attribute (masked; categorical)","categorical-high-card"),
    'card3':("Payment-card attribute (masked; categorical)","categorical-mid-card"),
    'card4':("Card network (visa / mastercard / american express / discover)","categorical-low-card"),
    'card5':("Payment-card attribute (masked; categorical)","categorical-mid-card"),
    'card6':("Card type (debit / credit / charge card / debit or credit)","categorical-low-card"),
    'addr1':("Address attribute (masked, region-like; categorical)","categorical-mid-card"),
    'addr2':("Address attribute (masked, country-like; categorical); 87 = 99.2% of non-null","categorical (dominant level)"),
    'dist1':("Distance-type feature (masked)","numeric-continuous"),
    'dist2':("Distance-type feature (masked)","numeric-continuous"),
    'P_emaildomain':("Purchaser email domain","categorical-high-card (domain / provider-group / TLD splits)"),
    'R_emaildomain':("Recipient email domain","categorical-high-card"),
    'DeviceType':("Device type (desktop/mobile)","categorical-low-card"),
    'DeviceInfo':("Device string (e.g. OS/build/model)","categorical-high-card (normalise: prefix/brand)"),
    }
    if c in d: return d[c]
    if f=='C': return ("Counting feature (masked; Vesta: e.g. number of addresses linked to the card)","numeric-count; heavy tail -> log1p")
    if f=='D': return ("Timedelta-type feature (masked; Vesta: e.g. days since previous transaction)","numeric-timedelta")
    if f=='M': return ("Match flag (masked; Vesta: e.g. name/address match)","categorical-binary (T/F) or M0/M1/M2")
    if f=='V': return ("Vesta-engineered feature (masked: ranking/counting/entity relations)","numeric (count/flag-like)")
    if f=='id':
        n=int(c.split('_')[1])
        if c in ('id_12','id_15','id_16','id_23','id_27','id_28','id_29','id_34','id_35','id_36','id_37','id_38'): return ("Identity/device-network flag (string level)","categorical-low-card")
        if c in ('id_30','id_31','id_33'): return ("Identity string: OS / browser / screen resolution","categorical-high-card")
        if n in(1,3,4,5,6,7,8,9,10,11): return ("Identity numeric score/delta (masked)","numeric")
        return ("Identity attribute (masked; competition lists id_12-id_38 as categorical)","categorical (numeric-coded)")
    return ("","")
dr = prof.apply(lambda r: describe(r['column'], r['family']), axis=1, result_type='expand'); prof['description']=dr[0]; prof['potential_feature_role']=dr[1]
# V redundancy
prof['v_redundant_with_at_r098']=prof.column.map(lambda c: vred.loc[c,'representative_at_r>=0.98'] if c in vred.index and vred.loc[c,'representative_at_r>=0.98']!=c else '')
out = prof[['column','family','arrow_type','null_count','null_pct','distinct_nonnull','min','max','mean','median','std','top1_value','top1_share_of_nonnull','mask_group','fraud_rate_if_null','fraud_rate_if_present','auc_nonnull','treatment','treatment_reason','description','potential_feature_role','v_redundant_with_at_r098']].copy()
out.insert(0,'dataset','ieee')
out=out.rename(columns={'column':'column','arrow_type':'data_type','distinct_nonnull':'exact_unique_nonnull'})
out.to_csv('ieee_schema.csv', index=False)
print(out.treatment.value_counts())
print(out.groupby(['family','treatment']).size().unstack(fill_value=0))
print(mg.to_string())
