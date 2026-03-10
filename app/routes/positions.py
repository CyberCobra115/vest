from datetime import date

from flask import Blueprint, jsonify, request

from app.services.positions_service import get_positions

bp = Blueprint("positions", __name__)


@bp.get("/positions")
def positions():
    """
    GET /positions?account=ACC001&date=2026-01-15

    Returns cost basis and market value for all open positions
    in the given account as of the given date.
    """
    account_id = request.args.get("account", "").strip()
    date_str = request.args.get("date", "").strip()

    if not account_id:
        return jsonify({"error": "Missing required query param: account"}), 400
    if not date_str:
        return jsonify({"error": "Missing required query param: date"}), 400

    try:
        as_of = date.fromisoformat(date_str)
    except ValueError:
        return jsonify({"error": f"Invalid date format: {date_str!r}. Use YYYY-MM-DD."}), 400

    summaries = get_positions(account_id, as_of)

    return jsonify({
        "account_id": account_id,
        "as_of": as_of.isoformat(),
        "positions": [
            {
                "ticker": s.ticker,
                "net_quantity": s.net_quantity,
                "avg_cost_basis": str(s.avg_cost_basis) if s.avg_cost_basis is not None else None,
                "total_cost_basis": str(s.total_cost_basis) if s.total_cost_basis is not None else None,
                "market_value": str(s.market_value) if s.market_value is not None else None,
                "unrealized_pnl": str(s.unrealized_pnl) if s.unrealized_pnl is not None else None,
            }
            for s in summaries
        ],
    })
