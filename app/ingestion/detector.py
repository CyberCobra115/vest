"""
Detect which ingestion format a file/content belongs to.

Format 1: CSV comma-delimited trades
  header: TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate

Format 2: pipe-delimited trades
  header: REPORT_DATE|ACCOUNT_ID|SECURITY_TICKER|SHARES|MARKET_VALUE|SOURCE_SYSTEM

Format 3: YAML/JSON-like bank position format
  starts with: report_date: "..."
"""

from enum import Enum


class FileFormat(str, Enum):
    FORMAT_1 = "FORMAT_1"
    FORMAT_2 = "FORMAT_2"
    FORMAT_3 = "FORMAT_3"
    UNKNOWN = "UNKNOWN"


_FORMAT_1_HEADER = {"tradedate", "accountid", "ticker", "quantity", "price", "tradetype", "settlementdate"}
_FORMAT_2_HEADER = {"report_date", "account_id", "security_ticker", "shares", "market_value", "source_system"}


def detect_format(content: str) -> FileFormat:
    """Inspect the first non-empty line to determine file format."""
    first_line = _first_nonempty_line(content)

    if not first_line:
        return FileFormat.UNKNOWN

    if first_line.startswith("report_date:"):
        return FileFormat.FORMAT_3

    if "|" in first_line:
        cols = {c.strip().lower() for c in first_line.split("|")}
        if cols == _FORMAT_2_HEADER:
            return FileFormat.FORMAT_2

    if "," in first_line:
        cols = {c.strip().lower() for c in first_line.split(",")}
        if cols == _FORMAT_1_HEADER:
            return FileFormat.FORMAT_1

    return FileFormat.UNKNOWN


def _first_nonempty_line(content: str) -> str:
    for line in content.splitlines():
        stripped = line.strip()
        if stripped:
            return stripped
    return ""
