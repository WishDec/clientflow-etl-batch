"""
ClientFlow ETL (Batch) - Transform

Transforms RawOffer records into a cleaned staging representation:
- derives business date (dt)
- validates prices
- normalizes fields

This mirrors the logic in sql/transformations.sql (RAW -> STG).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Iterable, List, Optional, Tuple

from etl.extract import RawOffer


@dataclass(frozen=True)
class StgOfferClean:
    dt: date
    source: str
    product_sku: str
    merchant_id: str
    price_kzt: float
    rating: Optional[float]
    reviews_qty: Optional[int]
    purchase_count: Optional[int]
    is_valid: bool
    invalid_reason: Optional[str]


def validate_price(price_kzt: float) -> Tuple[bool, Optional[str]]:
    if price_kzt is None:
        return False, "price_is_null"
    if price_kzt <= 0:
        return False, "price_non_positive"
    if price_kzt > 100000000:
        return False, "price_outlier"
    return True, None


def to_staging(offers: Iterable[RawOffer]) -> List[StgOfferClean]:
    out: List[StgOfferClean] = []
    for o in offers:
        is_valid, reason = validate_price(o.price_kzt)
        out.append(
            StgOfferClean(
                dt=o.ingested_at.date(),
                source=o.source.strip(),
                product_sku=o.product_sku.strip(),
                merchant_id=o.merchant_id.strip(),
                price_kzt=o.price_kzt,
                rating=o.rating,
                reviews_qty=o.reviews_qty,
                purchase_count=o.purchase_count,
                is_valid=is_valid,
                invalid_reason=reason,
            )
        )
    return out


if __name__ == "__main__":
    # Demo flow:
    from pathlib import Path
    from etl.extract import read_offers_csv

    raw = read_offers_csv(Path("data/sample_offers.csv"))
    stg = to_staging(raw)

    # Print preview
    for row in stg[:5]:
        print(row)
