"""
ClientFlow ETL (Batch) - Extract

Reads demo marketplace offers from CSV and prepares "raw" records
similar to what would be inserted into raw.marketplace_offers.

This module is designed to be readable and production-like,
even if you don't run it locally.
"""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


@dataclass(frozen=True)
class RawOffer:
    ingested_at: datetime
    source: str
    product_sku: str
    merchant_id: str
    merchant_name: Optional[str]
    offer_title: Optional[str]
    price_kzt: float
    currency: str = "KZT"
    rating: Optional[float] = None
    reviews_qty: Optional[int] = None
    purchase_count: Optional[int] = None
    delivery_ts: Optional[datetime] = None
    raw_payload: Optional[Dict[str, Any]] = None


def _parse_dt(value: str) -> Optional[datetime]:
    value = (value or "").strip()
    if not value:
        return None
    # CSV uses format: "YYYY-MM-DD HH:MM:SS"
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")


def read_offers_csv(csv_path: str | Path) -> List[RawOffer]:
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"CSV file not found: {path}")

    offers: List[RawOffer] = []
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            offer = RawOffer(
                ingested_at=_parse_dt(row["ingested_at"]) or datetime.utcnow(),
                source=row["source"].strip(),
                product_sku=row["product_sku"].strip(),
                merchant_id=row["merchant_id"].strip(),
                merchant_name=(row.get("merchant_name") or "").strip() or None,
                offer_title=(row.get("offer_title") or "").strip() or None,
                price_kzt=float(row["price_kzt"]),
                currency=(row.get("currency") or "KZT").strip(),
                rating=float(row["rating"]) if (row.get("rating") or "").strip() else None,
                reviews_qty=int(row["reviews_qty"]) if (row.get("reviews_qty") or "").strip() else None,
                purchase_count=int(row["purchase_count"]) if (row.get("purchase_count") or "").strip() else None,
                delivery_ts=_parse_dt(row.get("delivery_ts") or ""),
                raw_payload={"source_row": row},
            )
            offers.append(offer)

    return offers


def to_raw_insert_payloads(offers: Iterable[RawOffer]) -> List[Dict[str, Any]]:
    """
    Prepares dictionaries representing the DB insert payload for raw.marketplace_offers.
    """
    payloads: List[Dict[str, Any]] = []
    for o in offers:
        payloads.append(
            {
                "ingested_at": o.ingested_at.isoformat(sep=" "),
                "source": o.source,
                "product_sku": o.product_sku,
                "merchant_id": o.merchant_id,
                "merchant_name": o.merchant_name,
                "offer_title": o.offer_title,
                "price_kzt": o.price_kzt,
                "currency": o.currency,
                "rating": o.rating,
                "reviews_qty": o.reviews_qty,
                "purchase_count": o.purchase_count,
                "delivery_ts": o.delivery_ts.isoformat(sep=" ") if o.delivery_ts else None,
                "raw_payload": json.dumps(o.raw_payload) if o.raw_payload else None,
            }
        )
    return payloads


if __name__ == "__main__":
    offers = read_offers_csv(Path("data/sample_offers.csv"))
    payloads = to_raw_insert_payloads(offers)

    # Print a small preview (first 2 rows)
    for p in payloads[:2]:
        print(p)
