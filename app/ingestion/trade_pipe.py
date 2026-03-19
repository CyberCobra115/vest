"""
Parser for pipe-delimited daily trade fills (formerly "Format 2").

Expected header:
  REPORT_DATE|ACCOUNT_ID|SECURITY_TICKER|SHARES|MARKET_VALUE|SOURCE_SYSTEM

SHARES is the day's fill quantity (signed: negative = SELL).
MARKET_VALUE is the fill notional for that day's trades.
Price per share is derived: abs(MARKET_VALUE) / abs(SHARES).

Dates are compact YYYYMMDD.

Contract: exposes parse(content) -> tuple[list[TradeRow], list[dict]]
"""

import csv
import io
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation

from app.ingestion.utils import parse_date, ParseError
from app.ingestion.base import TradeRow


def parse(content: str) -> tuple[list[TradeRow], list[dict]]:
    rows: list[TradeRow] = []
    errors: list[dict] = []

    reader = csv.DictReader(io.StringIO(content), delimiter="|")
    for line_num, raw in enumerate(reader, start=2):
        row_errors = _validate(raw, line_num)
        if row_errors:
            errors.extend(row_errors)
            continue

        try:
            shares = int(raw["SHARES"])
            market_value = Decimal(raw["MARKET_VALUE"])

            if shares == 0:
                errors.append({"line": line_num, "field": "SHARES",
                               "value": raw["SHARES"],
                               "reason": "SHARES must be non-zero"})
                continue

            # Derive per-share price from fill notional; always positive.
            price = abs(market_value) / abs(Decimal(shares))
            trade_type = "BUY" if shares > 0 else "SELL"

            rows.append(TradeRow(
                trade_date=parse_date(raw["REPORT_DATE"]),
                account_id=raw["ACCOUNT_ID"].strip(),
                ticker=raw["SECURITY_TICKER"].strip().upper(),
                quantity=shares,        # already signed from source
                price=price,
                trade_type=trade_type,
                settlement_date=parse_date(raw["REPORT_DATE"]),  # no settlement col; use trade date
                source_ref=raw["SOURCE_SYSTEM"].strip(),
                _line_number=line_num,
            ))
        except (ValueError, InvalidOperation, ParseError) as exc:
            errors.append({"line": line_num, "field": "parse", "reason": str(exc)})

    return rows, errors


def _validate(raw: dict, line_num: int) -> list[dict]:
    errors = []
    required = ["REPORT_DATE", "ACCOUNT_ID", "SECURITY_TICKER",
                "SHARES", "MARKET_VALUE", "SOURCE_SYSTEM"]

    for field in required:
        if not raw.get(field, "").strip():
            errors.append({"line": line_num, "field": field, "reason": "missing or empty"})

    shares = raw.get("SHARES", "").strip()
    if shares:
        try:
            int(shares)
        except ValueError:
            errors.append({"line": line_num, "field": "SHARES",
                           "value": shares, "reason": "must be integer"})

    mv = raw.get("MARKET_VALUE", "").strip()
    if mv:
        try:
            Decimal(mv)
        except InvalidOperation:
            errors.append({"line": line_num, "field": "MARKET_VALUE",
                           "value": mv, "reason": "must be numeric"})

    return errors
