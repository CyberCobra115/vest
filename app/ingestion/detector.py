"""
Detect which ingestion format a raw file belongs to.

FileFormat values are descriptive of the file's nature, not arbitrary numbers,
so adding a 14th format means adding one new enum member — not FORMAT_14.

Detection strategy: read only as far as needed.
  - For delimited files: inspect the header line only.
  - For YAML: check whether the first non-whitespace content starts with
    the known root key.

_header() is O(first-line) — it stops reading as soon as it finds the first
non-blank line, using the file's own iterator rather than loading the whole
content into memory first.
"""

from __future__ import annotations

import io
from enum import Enum


class FileFormat(str, Enum):
    TRADE_CSV  = "TRADE_CSV"       # comma-delimited daily trade fills
    TRADE_PIPE = "TRADE_PIPE"      # pipe-delimited daily trade fills
    POSITION_YAML = "POSITION_YAML"  # YAML end-of-day position snapshot
    UNKNOWN    = "UNKNOWN"


# Expected header column sets (lowercased for case-insensitive matching).
_TRADE_CSV_COLS  = {"tradedate", "accountid", "ticker", "quantity", "price",
                    "tradetype", "settlementdate"}
_TRADE_PIPE_COLS = {"report_date", "account_id", "security_ticker", "shares",
                    "market_value", "source_system"}


def detect(content: str) -> FileFormat:
    """
    Return the FileFormat for *content*, or FileFormat.UNKNOWN.

    Reads only the minimum required: the first non-blank line.
    """
    first = _header(content)
    if not first:
        return FileFormat.UNKNOWN

    # YAML position file — root key is always 'report_date:'
    if first.startswith("report_date:"):
        return FileFormat.POSITION_YAML

    # Pipe-delimited trade file
    if "|" in first:
        if {c.strip().lower() for c in first.split("|")} == _TRADE_PIPE_COLS:
            return FileFormat.TRADE_PIPE

    # Comma-delimited trade file
    if "," in first:
        if {c.strip().lower() for c in first.split(",")} == _TRADE_CSV_COLS:
            return FileFormat.TRADE_CSV

    return FileFormat.UNKNOWN


def _header(content: str) -> str:
    """
    Return the first non-blank line of *content* without reading further.

    Uses an iterator over the lines so we stop as soon as we have what we need,
    rather than splitting the entire file into a list first.
    """
    for line in io.StringIO(content):
        stripped = line.strip()
        if stripped:
            return stripped
    return ""
