"""
Reconciliation service: compare expected position (derived from trade history)
against reported position (from bank/custodian position files).

A break is flagged when the net quantity from trades does not match
the shares reported in the position file for the same account + ticker + date.
"""

from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from sqlalchemy import func

from app import db
from app.models import Position, Trade


@dataclass
class ReconciliationBreak:
    account_id: str
    ticker: str
    trade_derived_qty: int
    reported_shares: int
    delta: int                  # reported - derived; nonzero = break
    reported_market_value: Decimal | None


def get_reconciliation_breaks(as_of: date) -> list[ReconciliationBreak]:
    """
    For each account+ticker pair visible in either trades or positions on as_of,
    surface discrepancies between trade-derived quantity and reported shares.
    """
    trade_positions = _get_trade_derived_positions(as_of)
    reported_positions = _get_reported_positions(as_of)

    all_keys = set(trade_positions) | set(reported_positions)
    breaks = []

    for key in sorted(all_keys):
        derived_qty = trade_positions.get(key, 0)
        reported = reported_positions.get(key)
        reported_shares = reported["shares"] if reported else 0
        mv = reported["market_value"] if reported else None

        delta = reported_shares - derived_qty
        if delta != 0:
            account_id, ticker = key
            breaks.append(ReconciliationBreak(
                account_id=account_id,
                ticker=ticker,
                trade_derived_qty=derived_qty,
                reported_shares=reported_shares,
                delta=delta,
                reported_market_value=mv,
            ))

    return breaks


def _get_trade_derived_positions(as_of: date) -> dict[tuple[str, str], int]:
    """Net quantity per account+ticker from all trades up to and including as_of."""
    rows = (
        db.session.query(
            Trade.account_id,
            Trade.ticker,
            func.sum(Trade.quantity).label("net_qty"),
        )
        .filter(Trade.trade_date <= as_of)
        .group_by(Trade.account_id, Trade.ticker)
        .all()
    )
    return {(r.account_id, r.ticker): r.net_qty or 0 for r in rows}


def _get_reported_positions(as_of: date) -> dict[tuple[str, str], dict]:
    """Reported shares + market value per account+ticker on the given date."""
    rows = (
        db.session.query(
            Position.account_id,
            Position.ticker,
            func.sum(Position.shares).label("shares"),
            func.sum(Position.market_value).label("market_value"),
        )
        .filter(Position.report_date == as_of)
        .group_by(Position.account_id, Position.ticker)
        .all()
    )
    return {
        (r.account_id, r.ticker): {
            "shares": r.shares or 0,
            "market_value": r.market_value,
        }
        for r in rows
    }
