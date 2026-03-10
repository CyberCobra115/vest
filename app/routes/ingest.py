from flask import Blueprint, jsonify, request

from app.services.ingestion_service import ingest

bp = Blueprint("ingest", __name__)


@bp.post("/ingest")
def ingest_file():
    """
    Accept a file upload or raw body and return a quality report.

    Supports multipart/form-data (field name: 'file') or raw text body.
    """
    content = _extract_content()
    if content is None:
        return jsonify({"error": "No file or body provided"}), 400

    report = ingest(content)
    status = 200 if (report.rows_accepted > 0 and report.rows_rejected == 0) else 207
    if report.rows_accepted == 0 and report.rows_rejected == 0 and report.format_detected == "UNKNOWN":
        status = 400
    return jsonify(report.to_dict()), status


def _extract_content() -> str | None:
    if "file" in request.files:
        f = request.files["file"]
        return f.read().decode("utf-8")

    if request.data:
        return request.data.decode("utf-8")

    if request.form.get("content"):
        return request.form["content"]

    return None
