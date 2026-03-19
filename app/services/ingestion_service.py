"""
Ingestion service: detect → parse → quality-check → persist.

Format routing is data-driven via the parser registry in app/ingestion/__init__.py:
  TRADE_PARSERS   → all formats that produce TradeRow  → trades table
  POSITION_PARSERS → all formats that produce PositionRow → positions table

Adding a 13th trade format means adding one parser module and one registry
entry. Nothing in this file changes.

Quality checks applied to all trade formats:
  - Field-level validation (delegated to parser)
  - SettlementDate >= TradeDate
  - Intra-file duplicate detection
  - Cross-file duplicate detection (DB lookup before insert)
  - DB-level UniqueConstraint as final concurrency guard

Quality checks applied to all position formats:
  - Field-level validation (delegated to parser)
  - Intra-file duplicate detection
  - Upsert: update shares/market_value if the record already exists
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

from sqlalchemy import and_
from sqlalchemy.exc import IntegrityError

from app import db
from app.ingestion import TRADE_PARSERS, POSITION_PARSERS
from app.ingestion.base import TradeRow, PositionRow
from app.ingestion.detector import FileFormat, detect
from app.models import Position, SourceFormat, Trade, TradeType


@dataclass
class QualityReport:
    format_detected: str
    total_rows: int
    rows_accepted: int
    rows_rejected: int
    rows_skipped_duplicate: int = 0
    rows_upserted: int = 0
    errors: list[dict] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "format_detected": self.format_detected,
            "total_rows": self.total_rows,
            "rows_accepted": self.rows_accepted,
            "rows_rejected": self.rows_rejected,
            "rows_skipped_duplicate": self.rows_skipped_duplicate,
            "rows_upserted": self.rows_upserted,
            "errors": self.errors,
            "warnings": self.warnings,
        }


# ── Public entry point ────────────────────────────────────────────────────────

def ingest(content: str) -> QualityReport:
    fmt = detect(content)

    if fmt in TRADE_PARSERS:
        return _ingest_trades(fmt, content)
    if fmt in POSITION_PARSERS:
        return _ingest_positions(fmt, content)

    return QualityReport(
        format_detected=FileFormat.UNKNOWN,
        total_rows=0,
        rows_accepted=0,
        rows_rejected=0,
        errors=[{"field": "file",
                 "reason": "Could not detect file format. Check header row."}],
    )


# ── Trade ingestion (all trade formats share this path) ───────────────────────

def _ingest_trades(fmt: FileFormat, content: str) -> QualityReport:
    parser = TRADE_PARSERS[fmt]
    rows, parse_errors = parser.parse(content)
    warnings: list[str] = []
    errors: list[dict] = list(parse_errors)

    # Count data lines (excluding header) before any filtering.
    raw_row_count = sum(1 for ln in content.splitlines() if ln.strip()) - 1

    # Quality check: SettlementDate must be >= TradeDate.
    clean: list[TradeRow] = []
    for row in rows:
        if row.settlement_date < row.trade_date:
            errors.append({
                "line": row._line_number,
                "field": "SettlementDate",
                "value": str(row.settlement_date),
                "reason": (f"SettlementDate {row.settlement_date} is before "
                           f"TradeDate {row.trade_date}"),
            })
        else:
            clean.append(row)

    # Quality check: intra-file duplicates.
    seen: set[tuple] = set()
    deduped: list[TradeRow] = []
    for row in clean:
        key = (row.trade_date, row.account_id, row.ticker,
               row.quantity, row.price, row.trade_type)
        if key in seen:
            warnings.append(
                f"Line {row._line_number}: duplicate trade skipped "
                f"({row.account_id} {row.ticker} {row.trade_type} "
                f"{abs(row.quantity)}@{row.price} on {row.trade_date})"
            )
        else:
            seen.add(key)
            deduped.append(row)

    # Persist — DB UniqueConstraint is the final guard against races.
    source_fmt = SourceFormat(fmt.value)   # enum value names match 1-to-1
    accepted = skipped = 0

    for row in deduped:
        exists = db.session.query(Trade).filter(
            and_(
                Trade.trade_date == row.trade_date,
                Trade.account_id == row.account_id,
                Trade.ticker == row.ticker,
                Trade.quantity == row.quantity,
                Trade.price == row.price,
                Trade.trade_type == TradeType(row.trade_type),
            )
        ).first()

        if exists:
            warnings.append(
                f"Trade already in DB, skipped: {row.account_id} {row.ticker} "
                f"{row.trade_type} {abs(row.quantity)}@{row.price} on {row.trade_date}"
            )
            skipped += 1
            continue

        db.session.add(Trade(
            trade_date=row.trade_date,
            account_id=row.account_id,
            ticker=row.ticker,
            quantity=row.quantity,
            price=row.price,
            trade_type=TradeType(row.trade_type),
            settlement_date=row.settlement_date,
            source_format=source_fmt,
        ))
        accepted += 1

    try:
        db.session.commit()
    except IntegrityError:
        db.session.rollback()
        raise

    # rejected = distinct bad lines (one bad row may produce multiple error dicts)
    rejected_lines = {e["line"] for e in errors
                      if "line" in e and e["line"] is not None}

    return QualityReport(
        format_detected=fmt.value,
        total_rows=raw_row_count,
        rows_accepted=accepted,
        rows_rejected=len(rejected_lines),
        rows_skipped_duplicate=skipped,
        warnings=warnings,
        errors=errors,
    )


# ── Position ingestion (all position formats share this path) ─────────────────

def _ingest_positions(fmt: FileFormat, content: str) -> QualityReport:
    parser = POSITION_PARSERS[fmt]
    rows, parse_errors = parser.parse(content)
    warnings: list[str] = []
    errors: list[dict] = list(parse_errors)

    raw_row_count = len(rows) + len(errors)

    # Quality check: intra-file duplicates.
    seen: set[tuple] = set()
    deduped: list[PositionRow] = []
    for idx, row in enumerate(rows):
        key = (row.report_date, row.account_id, row.ticker, row.custodian_ref)
        if key in seen:
            warnings.append(
                f"positions[{idx}]: duplicate entry skipped "
                f"({row.account_id} {row.ticker} on {row.report_date})"
            )
        else:
            seen.add(key)
            deduped.append(row)

    accepted = upserted = 0

    for row in deduped:
        existing = db.session.query(Position).filter(
            and_(
                Position.report_date == row.report_date,
                Position.account_id == row.account_id,
                Position.ticker == row.ticker,
                Position.custodian_ref == row.custodian_ref,
            )
        ).first()

        if existing:
            if (existing.shares != row.shares
                    or existing.market_value != row.market_value):
                warnings.append(
                    f"Position updated (upsert): {row.account_id} {row.ticker} "
                    f"on {row.report_date} — "
                    f"shares {existing.shares}→{row.shares}, "
                    f"market_value {existing.market_value}→{row.market_value}"
                )
                existing.shares = row.shares
                existing.market_value = row.market_value
                upserted += 1
            else:
                warnings.append(
                    f"Position unchanged, skipped: "
                    f"{row.account_id} {row.ticker} on {row.report_date}"
                )
            continue

        db.session.add(Position(
            report_date=row.report_date,
            account_id=row.account_id,
            ticker=row.ticker,
            shares=row.shares,
            market_value=row.market_value,
            custodian_ref=row.custodian_ref,
            source_format=SourceFormat(fmt.value),
        ))
        accepted += 1

    db.session.commit()

    return QualityReport(
        format_detected=fmt.value,
        total_rows=raw_row_count,
        rows_accepted=accepted,
        rows_rejected=len(errors),
        rows_upserted=upserted,
        warnings=warnings,
        errors=errors,
    )
