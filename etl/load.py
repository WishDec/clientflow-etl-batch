"""
ClientFlow ETL (Batch) - Load (demo)

In a real pipeline, this step would load STG into a database and
build MART tables via SQL transformations.

Here we demonstrate the MART logic in-memory:
- best price per product/day
- avg price per product/day
- offers count
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Dict, Iterable, List, Optional, Tuple

from etl.transform import StgOfferClean


@dataclass(frozen=True)
class MartProductPriceDaily:
    dt: date
    source: str
    product_sku: str
    best_price_kzt: Optional[float]
    best_merchant_id: Optional[str]
    avg_price_kzt: Optional[float]
    offers_count: int


def build_mart_price_daily(rows: Iterable[StgOfferClean]) -> List[MartProductPriceDaily]:
    # Group valid offers by (dt, source, product_sku)
    groups: Dict[Tuple[date, str, str], List[StgOfferClean]] = {}
    for r in rows:
        if not r.is_valid:
            continue
        key = (r.dt, r.source, r.product_sku)
        groups.setdefault(key, []).append(r)

    out: List[MartProductPriceDaily] = []

    for (dt, source, sku), offers in groups.items():
        offers_count = len(offers)
        prices = [o.price_kzt for o in offers]
        avg_price = sum(prices) / offers_count if offers_count else None

        # Best price + deterministic merchant selection by merchant_id
        best_price = min(prices) if prices else None
        best_merchants = sorted([o.merchant_id for o in offers if o.price_kzt == best_price])
        best_merchant = best_merchants[0] if best_merchants else None

        out.append(
            MartProductPriceDaily(
                dt=dt,
                source=source,
                product_sku=sku,
                best_price_kzt=best_price,
                best_merchant_id=best_merchant,
                avg_price_kzt=round(avg_price, 2) if avg_price is not None else None,
                offers_count=offers_count,
            )
        )

    # sort for stable output
    out.sort(key=lambda x: (x.dt, x.source, x.product_sku))
    return out


if __name__ == "__main__":
    from pathlib import Path
    from etl.extract import read_offers_csv
    from etl.transform import to_staging

    raw = read_offers_csv(Path("data/sample_offers.csv"))
    stg = to_staging(raw)
    mart = build_mart_price_daily(stg)

    for row in mart:
        print(row)
