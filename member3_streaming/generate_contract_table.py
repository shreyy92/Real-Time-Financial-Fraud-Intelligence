"""Programmatically generate or verify the Output Contract and DLQ tables from consumer code definitions."""

import re
from member3_streaming.consumer_paysim import PAYSIM_OUTPUT_SCHEMA, DLQ_REASON_CODES

DESCRIPTIONS = {
    "step": "Simulation timestep in hours (1..743)",
    "type": "Raw transaction type (TRANSFER, CASH_OUT, PAYMENT, CASH_IN, DEBIT)",
    "amount": "Transaction amount in simulated currency",
    "nameOrig": "Originating customer account identifier",
    "oldbalanceOrg": "Originator pre-transaction balance",
    "nameDest": "Destination recipient account identifier",
    "oldbalanceDest": "Recipient pre-transaction balance",
    "isFraud": "Ground-truth fraud label (0 or 1)",
    "step_hour": "Hour of day derived as step % 24 (0..23)",
    "txn_id": "Unique SHA-256 transaction identifier",
    "hour_sin": "Cyclical hour feature: sin(2 * pi * step_hour / 24)",
    "hour_cos": "Cyclical hour feature: cos(2 * pi * step_hour / 24)",
    "sim_day": "Simulation day derived as floor((step - 1) / 24)",
    "history_window_complete_flag": "1 if step >= 169 (full 7-day 168h history available), else 0",
    "type_transfer": "One-hot binary indicator: 1 if type == 'TRANSFER', else 0",
    "type_cash_out": "One-hot binary indicator: 1 if type == 'CASH_OUT', else 0",
    "type_payment": "One-hot binary indicator: 1 if type == 'PAYMENT', else 0",
    "type_cash_in": "One-hot binary indicator: 1 if type == 'CASH_IN', else 0",
    "type_debit": "One-hot binary indicator: 1 if type == 'DEBIT', else 0",
    "split": "Dataset split partition: 'train' for step <= 500, 'valid' for 501 <= step <= 600, 'test' for 601 <= step <= 743",
    "amount_log1p": "log1p(max(0, amount))",
    "amount_zero_flag": "1 if amount == 0.0, else 0",
    "amt_to_oldbalOrg": "Ratio: amount / (oldbalanceOrg + 1.0)",
    "amt_to_oldbalDest": "Ratio: amount / (oldbalanceDest + 1.0)",
    "orig_drain_flag": "1 if oldbalanceOrg > 0 and abs(amount - oldbalanceOrg) < 0.005, else 0",
    "amount_exceeds_orig_balance_flag": "1 if amount > oldbalanceOrg, else 0",
    "orig_balance_zero_flag": "1 if oldbalanceOrg == 0.0, else 0",
    "dest_balance_zero_flag": "1 if oldbalanceDest == 0.0 and type != 'PAYMENT', else 0",
    "dest_prior_txn_count": "Cumulative prior transactions seen for this destination account",
    "dest_first_seen_flag": "1 if destination is appearing for the very first time (dest_prior_txn_count == 0), else 0",
    "dest_prior_amount_sum": "Cumulative sum of amounts previously received by this destination",
    "dest_prior_amount_mean": "Prior average amount received; null when dest_prior_txn_count == 0",
    "dest_prior_amount_std": "Sample standard deviation of prior amounts; null when dest_prior_txn_count < 2",
    "dest_prior_amount_max": "Maximum prior amount received; null when dest_prior_txn_count == 0",
    "dest_steps_since_last_txn": "step - last_seen_step; null when destination is appearing for the first time",
    "dest_amount_zscore": "Z-score of current amount vs destination prior history; null when dest_prior_txn_count < 2 or std == 0.0",
    "dest_txn_count_prev_24h": "Prior transaction count received by destination in trailing 24 hours (step - 24 <= prev_step < step)",
    "dest_txn_count_prev_168h": "Prior transaction count received by destination in trailing 168 hours (step - 168 <= prev_step < step)",
    "dest_amount_sum_prev_24h": "Prior transaction amount sum received by destination in trailing 24 hours",
    "dest_amount_sum_prev_168h": "Prior transaction amount sum received by destination in trailing 168 hours",
    "dest_prior_cashout_share": "Fraction of prior destination transactions that were CASH_OUT; null when dest_prior_txn_count == 0",
    "feature_version": "Contract feature store version string (v1.0)",
}

def generate_schema_markdown_table():
    lines = [
        "| # | Column Name | PySpark DataType | Nullable | Description & Domain Semantics |",
        "|---|---|---|---|---|",
    ]
    for idx, field in enumerate(PAYSIM_OUTPUT_SCHEMA.fields, start=1):
        dtype_str = field.dataType.simpleString()
        desc = DESCRIPTIONS.get(field.name, "Enriched feature attribute")
        lines.append(f"| {idx} | `{field.name}` | `{dtype_str}` | {field.nullable} | {desc} |")
    return "\n".join(lines)


def generate_dlq_markdown_table():
    lines = [
        "| Reason Code | Description |",
        "|---|---|",
    ]
    for code, desc in DLQ_REASON_CODES.items():
        lines.append(f"| `{code}` | {desc} |")
    return "\n".join(lines)


if __name__ == "__main__":
    print(f"Generated schema table with {len(PAYSIM_OUTPUT_SCHEMA.fields)} columns:")
    print(generate_schema_markdown_table()[:300] + "...")
    print("\nGenerated DLQ codes table:")
    print(generate_dlq_markdown_table())
