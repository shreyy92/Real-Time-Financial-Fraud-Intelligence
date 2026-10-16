"""Tests asserting contract columns and DLQ specifications strictly match code schema and constants."""

import os
import re
import pytest
from member3_streaming.consumer_paysim import (
    PAYSIM_OUTPUT_SCHEMA,
    DLQ_REASON_CODES,
    DLQ_RECORD_SCHEMA,
)

CONTRACT_PATH = os.path.join(os.path.dirname(__file__), "..", "MEMBER3_OUTPUT_CONTRACT.md")


def test_contract_columns_match_paysim_output_schema():
    """Assert contract schema table exactly matches PAYSIM_OUTPUT_SCHEMA in column names and count."""
    assert os.path.exists(CONTRACT_PATH), f"Contract file {CONTRACT_PATH} must exist"
    with open(CONTRACT_PATH, "r", encoding="utf-8") as f:
        content = f.read()

    # Find the table rows under ## 2. Full Schema
    rows = re.findall(r"\|\s*(\d+)\s*\|\s*`([^`]+)`\s*\|\s*`([^`]+)`\s*\|\s*(\w+)\s*\|\s*([^|]+)\|", content)
    assert len(rows) > 0, "No schema rows found in contract table"

    contract_col_names = [r[1] for r in rows]
    schema_col_names = [f.name for f in PAYSIM_OUTPUT_SCHEMA.fields]

    assert len(contract_col_names) == 42, f"Expected 42 columns, got {len(contract_col_names)}"
    assert len(schema_col_names) == 42, f"Expected 42 schema columns, got {len(schema_col_names)}"
    assert contract_col_names == schema_col_names, (
        f"Contract column mismatch. First difference: "
        f"{[(c, s) for c, s in zip(contract_col_names, schema_col_names) if c != s]}"
    )


def test_contract_dlq_codes_match_code_constants():
    """Assert contract DLQ table contains all reason codes defined in DLQ_REASON_CODES."""
    with open(CONTRACT_PATH, "r", encoding="utf-8") as f:
        content = f.read()

    # Extract reason codes from markdown table
    dlq_rows = re.findall(r"\|\s*`([A-Z_]+)`\s*\|\s*([^|]+)\|", content)
    contract_codes = set(r[0] for r in dlq_rows)

    code_constants = set(DLQ_REASON_CODES.keys())
    assert contract_codes == code_constants, (
        f"DLQ codes set mismatch! Missing in contract: {code_constants - contract_codes}, "
        f"unexpected in contract: {contract_codes - code_constants}"
    )


def test_contract_tier_b_rationale():
    """Assert contract documents Tier B as outcome balance leakage, not graph embeddings."""
    with open(CONTRACT_PATH, "r", encoding="utf-8") as f:
        content = f.read()

    assert "newbalanceOrig" in content
    assert "newbalanceDest" in content
    assert "leak" in content.lower()
    assert "graph" not in content.lower()


def test_dlq_record_schema_fields():
    """Assert DLQ record schema has raw_value, reason_code, ingested_at."""
    dlq_fields = [f.name for f in DLQ_RECORD_SCHEMA.fields]
    assert dlq_fields == ["raw_value", "reason_code", "ingested_at"]
