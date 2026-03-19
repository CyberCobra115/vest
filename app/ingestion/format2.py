"""
Parser for Trade File Format 2 — pipe-delimited.

Expected header:
  REPORT_DATE|ACCOUNT_ID|SECURITY_TICKER|SHARES|MARKET_VALUE|SOURCE_SYSTEM

This is a second trade file format (per the spec: "ingest files of two
different formats for daily trades into a single relational database table").
SHARES maps to quantity (signed: negative = SELL fill).
MARKET_VALUE is used to derive the per-share price = MARKET_VALUE / SHARES.
SOURCE_SYSTEM identifies the originating custodian.

Dates are compact YYYYMMDD.
"""

import csv
import io
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation

from app.ingestion.utils import parse_date, ParseError


@dataclass
class Format2Row:
    trade_date: date
    account_id: str
    ticker: str
    # Signed quantity: negative shares = SELL fill.
    quantity: int
    price: Decimal       # derived: abs(market_value) / abs(shares)
    trade_type: str      # "BUY" if shares > 0, "SELL" if shares < 0
    source_system: str
    _line_number: int = 0


def parse(content: str) -> tuple[list[Format2Row], list[dict]]:
    rows: list[Format2Row] = []
    errors: list[dict] = []

    reader = csv.DictReader(io.StringIO(content), delimiter="|")

    for line_num, raw in enumerate(reader, start=2):
        row_errors = _validate_row(raw, line_num)
        if row_errors:
            errors.extend(row_errors)
            continue

        try:
            shares = int(raw["SHARES"])
            market_value = Decimal(raw["MARKET_VALUE"])

            if shares == 0:
                errors.append({
                    "line": line_num,
                    "field": "SHARES",
                    "value": raw["SHARES"],
                    "reason": "SHARES must be non-zero",
                })
                continue

            # Derive per-share price from fill notional; always positive.
            price = abs(market_value) / abs(Decimal(shares))
            trade_type = "BUY" if shares > 0 else "SELL"
            # Stored signed so SUM(quantity) yields correct net position.
            signed_qty = shares  # already signed from source

            rows.append(Format2Row(
                trade_date=parse_date(raw["REPORT_DATE"]),
                account_id=raw["ACCOUNT_ID"].strip(),
                ticker=raw["SECURITY_TICKER"].strip().upper(),
                quantity=signed_qty,
                price=price,
                trade_type=trade_type,
                source_system=raw["SOURCE_SYSTEM"].strip(),
                _line_number=line_num,
            ))
        except (ValueError, InvalidOperation, ParseError) as exc:
            errors.append({"line": line_num, "field": "parse", "reason": str(exc)})

    return rows, errors


def _validate_row(raw: dict, line_num: int) -> list[dict]:
    errors = []
    required = ["REPORT_DATE", "ACCOUNT_ID", "SECURITY_TICKER", "SHARES", "MARKET_VALUE", "SOURCE_SYSTEM"]

    for field in required:
        if not raw.get(field, "").strip():
            errors.append({"line": line_num, "field": field, "reason": "missing or empty"})

    shares = raw.get("SHARES", "").strip()
    if shares:
        try:
            int(shares)
        except ValueError:
            errors.append({"line": line_num, "field": "SHARES", "value": shares, "reason": "must be integer"})

    mv = raw.get("MARKET_VALUE", "").strip()
    if mv:
        try:
            Decimal(mv)
        except InvalidOperation:
            errors.append({"line": line_num, "field": "MARKET_VALUE", "value": mv, "reason": "must be numeric"})

    return errors
