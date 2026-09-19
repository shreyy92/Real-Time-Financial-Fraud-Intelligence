# Member 1 audit bundle (for Member 2)

- `MEMBER1_AUDIT_AND_HANDOFF_REPORT.md` - the full 12-part audit and handoff report (start here).
- `data_profiles/` - machine-readable outputs behind the report:
  - `paysim_schema.csv`, `ieee_schema.csv` - every column (the IEEE file includes null %, exact uniques, min/max/mean,
    fraud rate when null/present, single-feature AUC, default treatment + reason, V-redundancy mapping)
  - `ieee_missingness_groups.csv` - the 70 identical-null-mask column groups
  - `near_constant_minority.csv` - minority-value fraud lift for near-constant columns
  - `v_redundancy_r098.csv` - advisory V-column correlation clusters (recompute on the training window before dropping anything)
- `audit_scripts/` - re-runnable, no Spark needed (`pip install duckdb pyarrow pandas numpy`):

      ./audit_scripts/run_all.sh <paysim_clean.parquet dir> <ieee_clean.parquet dir> <out_dir>

  Tested end-to-end: a fresh run regenerated all CSVs identical to those in `data_profiles/`.
