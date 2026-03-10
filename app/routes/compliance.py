from datetime import date

from flask import Blueprint, jsonify, request

from app.services.compliance_service import get_concentration_breaches

bp = Blueprint("compliance", __name__)


@bp.get("/compliance/concentration")
def concentration():
    """
    GET /compliance/concentration?date=2026-01-15

    Returns all accounts with a single equity position exceeding
    20% of total account market value on the given date.
    """
    date_str = request.args.get("date", "").strip()

    if not date_str:
        return jsonify({"error": "Missing required query param: date"}), 400

    try:
        as_of = date.fromisoformat(date_str)
    except ValueError:
        return jsonify({"error": f"Invalid date format: {date_str!r}. Use YYYY-MM-DD."}), 400

    breaches = get_concentration_breaches(as_of)

    return jsonify({
        "as_of": as_of.isoformat(),
        "threshold": "20%",
        "breach_count": len(breaches),
        "breaches": [
            {
                "account_id": b.account_id,
                "ticker": b.ticker,
                "position_value": str(b.position_value),
                "total_account_value": str(b.total_account_value),
                "concentration_pct": f"{b.concentration_pct * 100:.2f}%",
            }
            for b in breaches
        ],
    })
