"""
Parser for YAML end-of-day position snapshots (formerly "Format 3").

Expected shape:
  report_date: "20250115"
  positions:
    - account_id: "ACC001"
      ticker: "AAPL"
      shares: 100
      market_value: 18550.00
      custodian_ref: "CUST_A_12345"

Each row represents the total shares held in a given account+ticker as of
report_date — not a daily delta. This file goes into the positions table.

Contract: exposes parse(content) -> tuple[list[PositionRow], list[dict]]
"""

from decimal import Decimal, InvalidOperation

import yaml

from app.ingestion.utils import parse_date, ParseError
from app.ingestion.base import PositionRow


def parse(content: str) -> tuple[list[PositionRow], list[dict]]:
    rows: list[PositionRow] = []
    errors: list[dict] = []

    try:
        doc = yaml.safe_load(content)
    except yaml.YAMLError as exc:
        return [], [{"line": None, "field": "file", "reason": f"YAML parse error: {exc}"}]

    if not isinstance(doc, dict):
        return [], [{"line": None, "field": "file",
                     "reason": "expected a YAML mapping at root"}]

    try:
        report_date = parse_date(str(doc.get("report_date", "")))
    except ParseError as exc:
        return [], [{"line": None, "field": "report_date", "reason": str(exc)}]

    positions = doc.get("positions", [])
    if not isinstance(positions, list):
        return [], [{"line": None, "field": "positions",
                     "reason": "expected a list under 'positions'"}]

    for idx, entry in enumerate(positions):
        entry_errors = _validate(entry, idx)
        if entry_errors:
            errors.extend(entry_errors)
            continue

        try:
            rows.append(PositionRow(
                report_date=report_date,
                account_id=str(entry["account_id"]).strip(),
                ticker=str(entry["ticker"]).strip().upper(),
                shares=int(entry["shares"]),
                market_value=Decimal(str(entry["market_value"])),
                custodian_ref=str(entry.get("custodian_ref", "")).strip(),
            ))
        except (ValueError, InvalidOperation, ParseError) as exc:
            errors.append({"line": f"positions[{idx}]",
                           "field": "parse", "reason": str(exc)})

    return rows, errors


def _validate(entry: dict, idx: int) -> list[dict]:
    errors = []
    location = f"positions[{idx}]"

    if not isinstance(entry, dict):
        return [{"line": location, "field": "entry", "reason": "expected a mapping"}]

    for field in ("account_id", "ticker", "shares", "market_value"):
        if field not in entry or entry[field] is None:
            errors.append({"line": location, "field": field, "reason": "missing or null"})

    if "shares" in entry:
        try:
            int(entry["shares"])
        except (ValueError, TypeError):
            errors.append({"line": location, "field": "shares",
                           "reason": "must be integer"})

    if "market_value" in entry:
        try:
            Decimal(str(entry["market_value"]))
        except InvalidOperation:
            errors.append({"line": location, "field": "market_value",
                           "reason": "must be numeric"})

    return errors
