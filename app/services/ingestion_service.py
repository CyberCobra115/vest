"""
Ingestion service: detect format → parse → quality check → persist.
Returns a structured quality report regardless of outcome.

Quality checks performed:
  - Field-level validation (done in parsers)
  - Settlement date must be >= trade date (Format 1)
  - Intra-file duplicate detection (same key appearing twice in one file)
  - Cross-file duplicate detection (record already exists in DB)
  - Position upsert: update market_value/shares if same date+account+ticker+custodian
"""

from dataclasses import dataclass, field
from datetime import date

from sqlalchemy import and_

from app import db
from app.ingestion.detector import FileFormat, detect_format
from app.ingestion import format1, format2, format3
from app.ingestion.format1 import Format1Row
from app.ingestion.format2 import Format2Row
from app.ingestion.format3 import Format3Row
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
    fmt = detect_format(content)

    if fmt == FileFormat.FORMAT_1:
        return _ingest_format1(content)
    elif fmt == FileFormat.FORMAT_2:
        return _ingest_format2(content)
    elif fmt == FileFormat.FORMAT_3:
        return _ingest_format3(content)
    else:
        return QualityReport(
            format_detected="UNKNOWN",
            total_rows=0,
            rows_accepted=0,
            rows_rejected=0,
            errors=[{"field": "file", "reason": "Could not detect file format. Check header row."}],
        )


# ── Format 1 ──────────────────────────────────────────────────────────────────

def _ingest_format1(content: str) -> QualityReport:
    rows, errors = format1.parse(content)
    warnings: list[str] = []

    # Quality check: settlement_date must be >= trade_date
    clean: list[Format1Row] = []
    for row in rows:
        if row.settlement_date < row.trade_date:
            errors.append({
                "line": row._line_number,
                "field": "SettlementDate",
                "value": str(row.settlement_date),
                "reason": f"SettlementDate {row.settlement_date} is before TradeDate {row.trade_date}",
            })
            continue
        clean.append(row)

    # Quality check: intra-file duplicates
    # A trade is uniquely identified by (trade_date, account_id, ticker, quantity, price, trade_type)
    seen: set[tuple] = set()
    deduped: list[Format1Row] = []
    for row in clean:
        key = (row.trade_date, row.account_id, row.ticker, row.quantity, row.price, row.trade_type)
        if key in seen:
            warnings.append(
                f"Line {row._line_number}: duplicate trade skipped "
                f"({row.account_id} {row.ticker} {row.trade_type} {row.quantity}@{row.price} on {row.trade_date})"
            )
            continue
        seen.add(key)
        deduped.append(row)

    # Quality check: cross-file duplicates (already in DB)
    accepted = 0
    skipped = 0
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
                f"Trade already exists in DB, skipped: "
                f"{row.account_id} {row.ticker} {row.trade_type} {row.quantity}@{row.price} on {row.trade_date}"
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
            source_format=SourceFormat.FORMAT_1,
        ))
        accepted += 1

    db.session.commit()

    return QualityReport(
        format_detected=FileFormat.FORMAT_1,
        total_rows=len(rows) + len([e for e in errors]),
        rows_accepted=accepted,
        rows_rejected=len(errors),
        rows_skipped_duplicate=skipped,
        warnings=warnings,
        errors=errors,
    )


# ── Format 2 ──────────────────────────────────────────────────────────────────

def _ingest_format2(content: str) -> QualityReport:
    rows, errors = format2.parse(content)
    warnings: list[str] = []

    # Quality check: intra-file duplicates
    seen: set[tuple] = set()
    deduped: list[Format2Row] = []
    for row in rows:
        key = (row.report_date, row.account_id, row.ticker, row.source_system)
        if key in seen:
            warnings.append(
                f"Line {row._line_number}: duplicate position skipped "
                f"({row.account_id} {row.ticker} on {row.report_date} from {row.source_system})"
            )
            continue
        seen.add(key)
        deduped.append(row)

    accepted = 0
    upserted = 0
    for row in deduped:
        existing = db.session.query(Position).filter(
            and_(
                Position.report_date == row.report_date,
                Position.account_id == row.account_id,
                Position.ticker == row.ticker,
                Position.custodian_ref == row.source_system,
            )
        ).first()

        if existing:
            # Upsert: update shares and market_value in place
            if existing.shares != row.shares or existing.market_value != row.market_value:
                warnings.append(
                    f"Position updated (upsert): {row.account_id} {row.ticker} on {row.report_date} — "
                    f"shares {existing.shares}→{row.shares}, "
                    f"market_value {existing.market_value}→{row.market_value}"
                )
                existing.shares = row.shares
                existing.market_value = row.market_value
                upserted += 1
            else:
                warnings.append(
                    f"Position unchanged, skipped: {row.account_id} {row.ticker} on {row.report_date}"
                )
            continue

        db.session.add(Position(
            report_date=row.report_date,
            account_id=row.account_id,
            ticker=row.ticker,
            shares=row.shares,
            market_value=row.market_value,
            custodian_ref=row.source_system,
            source_format="FORMAT_2",
        ))
        accepted += 1

    db.session.commit()

    return QualityReport(
        format_detected=FileFormat.FORMAT_2,
        total_rows=len(rows) + len(errors),
        rows_accepted=accepted,
        rows_rejected=len(errors),
        rows_upserted=upserted,
        warnings=warnings,
        errors=errors,
    )


# ── Format 3 ──────────────────────────────────────────────────────────────────

def _ingest_format3(content: str) -> QualityReport:
    rows, errors = format3.parse(content)
    warnings: list[str] = []

    # Quality check: intra-file duplicates
    seen: set[tuple] = set()
    deduped: list[Format3Row] = []
    for idx, row in enumerate(rows):
        key = (row.report_date, row.account_id, row.ticker, row.custodian_ref)
        if key in seen:
            warnings.append(
                f"positions[{idx}]: duplicate entry skipped "
                f"({row.account_id} {row.ticker} on {row.report_date})"
            )
            continue
        seen.add(key)
        deduped.append(row)

    accepted = 0
    upserted = 0
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
            if existing.shares != row.shares or existing.market_value != row.market_value:
                warnings.append(
                    f"Position updated (upsert): {row.account_id} {row.ticker} on {row.report_date} — "
                    f"shares {existing.shares}→{row.shares}, "
                    f"market_value {existing.market_value}→{row.market_value}"
                )
                existing.shares = row.shares
                existing.market_value = row.market_value
                upserted += 1
            else:
                warnings.append(
                    f"Position unchanged, skipped: {row.account_id} {row.ticker} on {row.report_date}"
                )
            continue

        db.session.add(Position(
            report_date=row.report_date,
            account_id=row.account_id,
            ticker=row.ticker,
            shares=row.shares,
            market_value=row.market_value,
            custodian_ref=row.custodian_ref,
            source_format="FORMAT_3",
        ))
        accepted += 1

    db.session.commit()

    return QualityReport(
        format_detected=FileFormat.FORMAT_3,
        total_rows=len(rows) + len(errors),
        rows_accepted=accepted,
        rows_rejected=len(errors),
        rows_upserted=upserted,
        warnings=warnings,
        errors=errors,
    )
