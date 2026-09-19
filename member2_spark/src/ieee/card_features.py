"""
IEEE-CIS Card-Based Entity Proxy Generation.

Constructs deterministic proxy entity keys from available payment card attributes
(card1 through card6, and addr1).

CRITICAL ARCHITECTURAL RULE:
The IEEE-CIS dataset does NOT provide a direct customer identifier. This proxy
must be explicitly designated as 'card_proxy_id' (or 'card_addr_proxy_id') and
never represented as a true 'customer_id'.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_ieee_card_features(df: DataFrame) -> DataFrame:
    """
    Generate deterministic card and card-address proxy identifiers.

    Source columns:
        - Primary Card Proxy: card1, card2, card3, card4, card5, card6
          (Coalesces missing values with 'NA' to ensure deterministic grouping).
        - Card + Address Proxy: card_proxy_id + addr1

    Features derived:
        - card_proxy_id: SHA-256 hash representing the payment card instrument.
        - card_addr_proxy_id: SHA-256 hash representing payment card bound to billing region (addr1).

    Args:
        df: Input IEEE-CIS DataFrame containing card1-card6 and optionally addr1.

    Returns:
        DataFrame: Augmented DataFrame with card proxy keys.
    """
    existing_cols = set(df.columns)

    def _col_or_na(c: str):
        if c in existing_cols:
            return F.coalesce(F.col(c).cast("string"), F.lit("NA"))
        return F.lit("NA")

    # 1. Card Proxy Key (Instrument level)
    df = df.withColumn(
        "card_proxy_id",
        F.sha2(
            F.concat_ws(
                "_",
                _col_or_na("card1"),
                _col_or_na("card2"),
                _col_or_na("card3"),
                _col_or_na("card4"),
                _col_or_na("card5"),
                _col_or_na("card6"),
            ),
            256,
        ),
    )

    # 2. Card + Address Proxy Key (Instrument + billing address region)
    df = df.withColumn(
        "card_addr_proxy_id",
        F.sha2(
            F.concat_ws(
                "_",
                F.col("card_proxy_id"),
                _col_or_na("addr1"),
            ),
            256,
        ),
    )

    return df
