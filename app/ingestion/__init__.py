"""
Parser registry.

Maps each FileFormat to the module that can parse it.
Every module exposes exactly one function:  parse(content: str) -> (rows, errors)

To add a new format:
  1. Create app/ingestion/<descriptive_name>.py implementing parse()
  2. Add one line to TRADE_PARSERS or POSITION_PARSERS below.
  Nothing else in the codebase needs to change.
"""

from __future__ import annotations

from typing import Callable

from app.ingestion.detector import FileFormat
from app.ingestion import trade_csv, trade_pipe, position_yaml

# Maps FileFormat → parser module.
# The ingestion service queries these dicts; it never imports parsers directly.
TRADE_PARSERS: dict[FileFormat, object] = {
    FileFormat.TRADE_CSV:  trade_csv,
    FileFormat.TRADE_PIPE: trade_pipe,
}

POSITION_PARSERS: dict[FileFormat, object] = {
    FileFormat.POSITION_YAML: position_yaml,
}
