"""
Parser for Trade File Format 1 — comma-delimited CSV.

Expected header:
  TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate
"""

import csv
import io
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Generator

from app.ingestion.utils import parse_date, ParseError


@dataclass
class Format1Row:
    trade_date: date
    account_id: str
    ticker: str
    quantity: int
    price: Decimal
    trade_type: str
    settlement_date: date
    _line_number: int = 0


def parse(content: str) -> tuple[list[Format1Row], list[dict]]:
    """
    Returns (valid_rows, errors).
    Errors are dicts with {line, field, value, reason}.
    """
    rows: list[Format1Row] = []
    errors: list[dict] = []

    reader = csv.DictReader(io.StringIO(content))

    for line_num, raw in enumerate(reader, start=2):  # line 1 = header
        row_errors = _validate_row(raw, line_num)
        if row_errors:
            errors.extend(row_errors)
            continue

        try:
            trade_type = raw["TradeType"].strip().upper()
            raw_qty = int(raw["Quantity"])
            # Quantities are stored signed: positive=BUY, negative=SELL.
            # This lets SUM(quantity) yield the correct net position directly.
            signed_qty = raw_qty if trade_type == "BUY" else -raw_qty
            rows.append(Format1Row(
                trade_date=parse_date(raw["TradeDate"]),
                account_id=raw["AccountID"].strip(),
                ticker=raw["Ticker"].strip().upper(),
                quantity=signed_qty,
                price=Decimal(raw["Price"]),
                trade_type=trade_type,
                settlement_date=parse_date(raw["SettlementDate"]),
                _line_number=line_num,
            ))
        except (ValueError, InvalidOperation, ParseError) as exc:
            errors.append({"line": line_num, "field": "parse", "reason": str(exc)})

    return rows, errors


def _validate_row(raw: dict, line_num: int) -> list[dict]:
    errors = []
    required = ["TradeDate", "AccountID", "Ticker", "Quantity", "Price", "TradeType", "SettlementDate"]

    for field in required:
        if not raw.get(field, "").strip():
            errors.append({"line": line_num, "field": field, "reason": "missing or empty"})

    if raw.get("TradeType", "").strip().upper() not in ("BUY", "SELL"):
        errors.append({"line": line_num, "field": "TradeType", "value": raw.get("TradeType"), "reason": "must be BUY or SELL"})

    qty = raw.get("Quantity", "").strip()
    if qty:
        try:
            int(qty)
        except ValueError:
            errors.append({"line": line_num, "field": "Quantity", "value": qty, "reason": "must be integer"})

    price = raw.get("Price", "").strip()
    if price:
        try:
            if Decimal(price) <= 0:
                errors.append({"line": line_num, "field": "Price", "value": price, "reason": "must be positive"})
        except InvalidOperation:
            errors.append({"line": line_num, "field": "Price", "value": price, "reason": "must be numeric"})

    return errors
