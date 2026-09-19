"""
IEEE-CIS Structured Missingness and Identity Completeness Feature Engineering.

Leverages the Member 1 audit discovery that IEEE-CIS nulls are non-random and fall
into 70 identical-null-mask groups. Replaces hundreds of collinear per-column flags
with block-level indicators and identity completeness metrics.

CRITICAL: Never drops rows (no dropna).
"""

from typing import List
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# 40 columns belonging to the IEEE-CIS Identity table
IDENTITY_COLUMNS: List[str] = [f"id_{str(i).zfill(2)}" for i in range(1, 39)] + ["DeviceType", "DeviceInfo"]


def add_ieee_missingness_features(df: DataFrame) -> DataFrame:
    """
    Derive block-level missingness indicators and identity coverage metrics without dropping rows.

    Features derived:
        - has_identity_flag: 1 if any identity field is present, else 0 (7.85% vs 2.09% fraud rate).
        - identity_nonnull_count: Count of populated fields across identity columns (0 to 40).
        - identity_completeness: Ratio of populated identity fields (nonnull_count / 40.0).
        - ind_addr: 1 if addr1 is populated (MG09: addr null has 11.78% fraud rate vs 2.46% when present).
        - ind_dist1, ind_dist2: Distance block indicators.
        - ind_Pemail, ind_Remail: Purchaser and recipient email domain presence indicators.
        - ind_D2, ind_D3, ind_D4, ind_D5, ind_D6, ind_D7, ind_D10..ind_D15: Timedelta block indicators.
        - Block indicators for representative Vesta clusters (V1_11, V12_34, V35_52, V53_74, V75_94,
          V138_166, V167_278, V322_339).

    Args:
        df: Input IEEE-CIS DataFrame.

    Returns:
        DataFrame: Augmented DataFrame with missingness and identity features.
    """
    existing_cols = set(df.columns)

    # 1. Identity table coverage and completeness
    present_id_cols = [col for col in IDENTITY_COLUMNS if col in existing_cols]
    if present_id_cols:
        # Sum of (col is not null) across all present identity columns
        nonnull_expr = sum(
            [F.when(F.col(c).isNotNull(), 1).otherwise(0) for c in present_id_cols]
        )
        total_possible = len(IDENTITY_COLUMNS)

        df = (
            df.withColumn("identity_nonnull_count", nonnull_expr.cast("integer"))
            .withColumn(
                "has_identity_flag",
                F.when(F.col("identity_nonnull_count") > 0, 1).otherwise(0).cast("tinyint"),
            )
            .withColumn(
                "identity_completeness",
                (F.col("identity_nonnull_count") / float(total_possible)).cast("double"),
            )
        )
    else:
        # Fallback if no identity columns exist in test fixture
        df = (
            df.withColumn("identity_nonnull_count", F.lit(0).cast("integer"))
            .withColumn("has_identity_flag", F.lit(0).cast("tinyint"))
            .withColumn("identity_completeness", F.lit(0.0).cast("double"))
        )

    # 2. Block-level presence indicators (only compute if column exists in DataFrame)
    block_indicators = [
        ("ind_addr", "addr1"),
        ("ind_dist1", "dist1"),
        ("ind_dist2", "dist2"),
        ("ind_Pemail", "P_emaildomain"),
        ("ind_Remail", "R_emaildomain"),
        ("ind_D2", "D2"),
        ("ind_D3", "D3"),
        ("ind_D4", "D4"),
        ("ind_D5", "D5"),
        ("ind_D6", "D6"),
        ("ind_D7", "D7"),
        ("ind_D10", "D10"),
        ("ind_D11", "D11"),
        ("ind_D12", "D12"),
        ("ind_D13", "D13"),
        ("ind_D14", "D14"),
        ("ind_D15", "D15"),
        ("ind_M1_3", "M1"),
        ("ind_V1_11", "V1"),
        ("ind_V12_34", "V12"),
        ("ind_V35_52", "V35"),
        ("ind_V53_74", "V53"),
        ("ind_V75_94", "V75"),
        ("ind_V138_166", "V138"),
        ("ind_V167_278", "V167"),
        ("ind_V322_339", "V322"),
    ]

    for ind_name, src_col in block_indicators:
        if src_col in existing_cols:
            df = df.withColumn(
                ind_name,
                F.when(F.col(src_col).isNotNull(), 1).otherwise(0).cast("tinyint"),
            )

    return df
