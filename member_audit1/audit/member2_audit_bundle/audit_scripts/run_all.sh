#!/usr/bin/env bash
# Re-runs the whole audit chain (no Spark needed). Requires: pip install duckdb pyarrow pandas numpy
# Usage: ./run_all.sh <paysim_clean.parquet dir> <ieee_clean.parquet dir> <out_dir>
set -euo pipefail
PAYSIM="$1"; IEEE="$2"; OUT="$3"; HERE="$(cd "$(dirname "$0")" && pwd)"; mkdir -p "$OUT"
python "$HERE/audit_table_level_checks.py" --paysim "$PAYSIM" --ieee "$IEEE" --out "$OUT/audit_results.json" > /dev/null
python "$HERE/profile_ieee.py" "$IEEE" "$OUT"                          # step 1: per-column profile   (~30 s)
python "$HERE/derive_ieee_tables.py" "$IEEE" "$OUT"                    # step 2: family/lift/near-constant
python "$HERE/v_redundancy.py" "$IEEE" "$OUT/ieee_column_profile3.csv" "$OUT/v_redundancy_r098.csv"   # step 3
(cd "$OUT" && python "$HERE/build_ieee_schema.py" > /dev/null)         # step 4: ieee_schema.csv + missingness groups
python "$HERE/build_paysim_schema.py" "$PAYSIM" "$OUT"
echo "done -> $OUT"
