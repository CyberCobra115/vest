from datetime import date

from flask import Blueprint, jsonify, request

from app.services.reconciliation_service import get_reconciliation_breaks

bp = Blueprint("reconciliation", __name__)


@bp.get("/reconciliation")
def reconciliation():
    """
    GET /reconciliation?date=2026-01-15

    Compares trade-derived positions against bank-reported positions
    for the given date. Returns all discrepancies (breaks).
    """
    date_str = request.args.get("date", "").strip()

    if not date_str:
        return jsonify({"error": "Missing required query param: date"}), 400

    try:
        as_of = date.fromisoformat(date_str)
    except ValueError:
        return jsonify({"error": f"Invalid date format: {date_str!r}. Use YYYY-MM-DD."}), 400

    breaks = get_reconciliation_breaks(as_of)

    return jsonify({
        "as_of": as_of.isoformat(),
        "break_count": len(breaks),
        "breaks": [
            {
                "account_id": b.account_id,
                "ticker": b.ticker,
                "trade_derived_qty": b.trade_derived_qty,
                "reported_shares": b.reported_shares,
                "delta": b.delta,
                "reported_market_value": str(b.reported_market_value) if b.reported_market_value is not None else None,
            }
            for b in breaks
        ],
    })
