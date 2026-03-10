"""
Positions service: compute cost basis from trade history,
join with latest market value from positions table.

Cost basis method: average cost (sum(qty * price) / sum(qty)).
"""

from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from sqlalchemy import func, text

from app import db
from app.models import Position, Trade, TradeType


@dataclass
class PositionSummary:
    account_id: str
    ticker: str
    net_quantity: int
    avg_cost_basis: Decimal | None
    total_cost_basis: Decimal | None
    market_value: Decimal | None
    unrealized_pnl: Decimal | None


def get_positions(account_id: str, as_of: date) -> list[PositionSummary]:
    """
    Return all positions for an account as of a given date.
    Cost basis from trades table; market value from positions table.
    """
    trade_rows = (
        db.session.query(
            Trade.ticker,
            func.sum(Trade.quantity).label("net_qty"),
            func.sum(Trade.quantity * Trade.price).label("total_cost"),
        )
        .filter(
            Trade.account_id == account_id,
            Trade.trade_date <= as_of,
        )
        .group_by(Trade.ticker)
        .all()
    )

    # Pull latest market values from positions table on or before as_of
    market_values = _get_latest_market_values(account_id, as_of)

    results = []
    for row in trade_rows:
        net_qty = row.net_qty or 0
        if net_qty == 0:
            continue  # flat — position closed

        total_cost = row.total_cost or Decimal("0")
        avg_cost = (total_cost / net_qty) if net_qty != 0 else None
        mv = market_values.get(row.ticker)
        pnl = (mv - total_cost) if mv is not None else None

        results.append(PositionSummary(
            account_id=account_id,
            ticker=row.ticker,
            net_quantity=net_qty,
            avg_cost_basis=avg_cost,
            total_cost_basis=total_cost,
            market_value=mv,
            unrealized_pnl=pnl,
        ))

    return results


def _get_latest_market_values(account_id: str, as_of: date) -> dict[str, Decimal]:
    """
    For each ticker held by the account, return the most recent market_value
    from the positions table where report_date <= as_of.
    """
    subq = (
        db.session.query(
            Position.ticker,
            func.max(Position.report_date).label("latest_date"),
        )
        .filter(
            Position.account_id == account_id,
            Position.report_date <= as_of,
        )
        .group_by(Position.ticker)
        .subquery()
    )

    rows = (
        db.session.query(Position.ticker, Position.market_value)
        .join(subq, (Position.ticker == subq.c.ticker) & (Position.report_date == subq.c.latest_date))
        .filter(Position.account_id == account_id)
        .all()
    )

    return {r.ticker: r.market_value for r in rows}
