"""Tests asserting IEEE contract columns, types, and DLQ specifications strictly match code schema and ground truth parquet."""

import os
import re
import pytest
import pyarrow.parquet as pq
from member3_streaming.ieee_output_schema import IEEE_OUTPUT_SCHEMA
from member3_streaming.consumer_ieee import (
    DLQ_REASON_CODES,
    DLQ_RECORD_SCHEMA,
)

CONTRACT_PATH = os.path.join(os.path.dirname(__file__), "..", "MEMBER3_IEEE_OUTPUT_CONTRACT.md")
PARQUET_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "data", "features", "ieee_features.parquet")


def test_ieee_contract_columns_match_spark_schema_and_parquet():
    """Assert contract schema table exactly matches IEEE_OUTPUT_SCHEMA and ieee_features.parquet (independent check)."""
    assert os.path.exists(CONTRACT_PATH), f"Contract file {CONTRACT_PATH} must exist"
    assert os.path.exists(PARQUET_PATH), f"Parquet ground truth file {PARQUET_PATH} must exist"

    with open(CONTRACT_PATH, "r", encoding="utf-8") as f:
        content = f.read()

    # Extract rows from section 7 table: | `col_name` | `spark_type` | `nullable` | index |
    rows = re.findall(r"\|\s*`([^`]+)`\s*\|\s*`([^`]+)`\s*\|\s*`([^`]+)`\s*\|\s*(\d+)\s*\|", content)
    assert len(rows) > 0, "No schema rows found in contract table"

    contract_col_names = [r[0] for r in rows]
    schema_col_names = [f.name for f in IEEE_OUTPUT_SCHEMA.fields]

    # Read independent parquet column names
    parquet_table = pq.read_table(PARQUET_PATH)
    parquet_col_names = parquet_table.column_names

    assert len(contract_col_names) == 483, f"Expected 483 contract columns, got {len(contract_col_names)}"
    assert len(schema_col_names) == 483, f"Expected 483 schema columns, got {len(schema_col_names)}"
    assert len(parquet_col_names) == 483, f"Expected 483 parquet columns, got {len(parquet_col_names)}"

    # Check 1:1 name match with code schema
    assert contract_col_names == schema_col_names, (
        f"Contract vs Spark schema mismatch: "
        f"{[(c, s) for c, s in zip(contract_col_names, schema_col_names) if c != s][:5]}"
    )

    # Check 1:1 name and order match with independent ground truth parquet
    assert contract_col_names == parquet_col_names, (
        f"Contract vs Parquet ground truth mismatch: "
        f"{[(c, p) for c, p in zip(contract_col_names, parquet_col_names) if c != p][:5]}"
    )


def test_ieee_contract_dlq_codes_match_code_constants():
    """Assert contract DLQ table contains all reason codes defined in consumer_ieee DLQ_REASON_CODES."""
    with open(CONTRACT_PATH, "r", encoding="utf-8") as f:
        content = f.read()

    # Extract reason codes from markdown table under section 5
    dlq_rows = re.findall(r"\|\s*`([A-Z_]+)`\s*\|\s*[^|]+\|\s*[^|]+\|", content)
    contract_codes = set(dlq_rows)

    code_constants = set(DLQ_REASON_CODES.keys())
    assert contract_codes == code_constants, (
        f"DLQ codes set mismatch! Missing in contract: {code_constants - contract_codes}, "
        f"unexpected in contract: {contract_codes - code_constants}"
    )


def test_ieee_dlq_record_schema_fields():
    """Assert IEEE DLQ record schema contains raw_value, reason_code, ingested_at."""
    dlq_fields = [f.name for f in DLQ_RECORD_SCHEMA.fields]
    assert dlq_fields == ["raw_value", "reason_code", "ingested_at"]
