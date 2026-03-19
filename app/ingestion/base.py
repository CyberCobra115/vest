"""
Shared row dataclasses used as the common contract between every parser
and the ingestion service.

Every trade parser returns list[TradeRow].
Every position parser returns list[PositionRow].

The ingestion service only imports these types — never individual parser
modules — which is what allows it to stay format-count-agnostic.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal


@dataclass
class TradeRow:
    """Normalised representation of one trade fill from any source format."""
    trade_date: date
    account_id: str
    ticker: str
    # Signed: positive = BUY, negative = SELL.
    # SUM(quantity) across all rows for an account+ticker yields net position.
    quantity: int
    price: Decimal
    trade_type: str          # "BUY" or "SELL"
    settlement_date: date
    source_ref: str | None   # custodian/system reference, if the format provides one
    _line_number: int = field(default=0, repr=False)


@dataclass
class PositionRow:
    """Normalised representation of one end-of-day position from any source format."""
    report_date: date
    account_id: str
    ticker: str
    # Signed shares: negative = short position.
    shares: int
    market_value: Decimal
    custodian_ref: str
