"""
Compliance service: detect concentration violations.

A violation occurs when any single equity position's market value
exceeds 20% of the total account market value.
"""

from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from sqlalchemy import func

from app import db
from app.models import Position

CONCENTRATION_THRESHOLD = Decimal("0.20")


@dataclass
class ConcentrationBreach:
    account_id: str
    ticker: str
    position_value: Decimal
    total_account_value: Decimal
    concentration_pct: Decimal
    threshold_pct: Decimal = CONCENTRATION_THRESHOLD


def get_concentration_breaches(as_of: date) -> list[ConcentrationBreach]:
    """
    For each account on the given date, compute concentration per ticker.
    Return all positions exceeding the 20% threshold.
    """
    # Aggregate market value per account+ticker on exact date
    rows = (
        db.session.query(
            Position.account_id,
            Position.ticker,
            func.sum(Position.market_value).label("position_value"),
        )
        .filter(Position.report_date == as_of)
        .group_by(Position.account_id, Position.ticker)
        .all()
    )

    if not rows:
        return []

    # Compute total per account
    account_totals: dict[str, Decimal] = {}
    for row in rows:
        account_totals[row.account_id] = (
            account_totals.get(row.account_id, Decimal("0")) + row.position_value
        )

    breaches = []
    for row in rows:
        total = account_totals[row.account_id]
        if total == 0:
            continue
        pct = Decimal(str(row.position_value)) / total
        if pct > CONCENTRATION_THRESHOLD:
            breaches.append(ConcentrationBreach(
                account_id=row.account_id,
                ticker=row.ticker,
                position_value=Decimal(str(row.position_value)),
                total_account_value=total,
                concentration_pct=pct,
            ))

    # Sort by worst offenders first
    breaches.sort(key=lambda b: b.concentration_pct, reverse=True)
    return breaches
