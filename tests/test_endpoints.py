import json
from datetime import date

import pytest

from app import db
from app.models import Position, Trade, TradeType, SourceFormat
from tests.conftest import FORMAT_1_CONTENT, FORMAT_2_CONTENT, FORMAT_3_CONTENT


class TestIngestEndpoint:
    def test_ingest_format1_returns_quality_report(self, client):
        resp = client.post("/ingest", data=FORMAT_1_CONTENT, content_type="text/plain")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["format_detected"] == "FORMAT_1"
        assert body["rows_accepted"] == 4
        assert body["rows_rejected"] == 0

    def test_ingest_format2(self, client):
        resp = client.post("/ingest", data=FORMAT_2_CONTENT, content_type="text/plain")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["format_detected"] == "FORMAT_2"
        assert body["rows_accepted"] == 4

    def test_ingest_format3(self, client):
        resp = client.post("/ingest", data=FORMAT_3_CONTENT, content_type="text/plain")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["format_detected"] == "FORMAT_3"
        assert body["rows_accepted"] == 3

    def test_ingest_partial_failure_returns_207(self, client):
        bad_row = "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n2025-01-15,ACC001,AAPL,100,185.50,BUY,2025-01-17\n2025-01-15,,MSFT,50,420.25,BUY,2025-01-17\n"
        resp = client.post("/ingest", data=bad_row, content_type="text/plain")
        assert resp.status_code == 207
        body = resp.get_json()
        assert body["rows_accepted"] == 1
        assert body["rows_rejected"] == 1

    def test_ingest_no_body_returns_400(self, client):
        resp = client.post("/ingest")
        assert resp.status_code == 400

    def test_ingest_unknown_format_returns_400(self, client):
        resp = client.post("/ingest", data="garbage data\n1,2,3", content_type="text/plain")
        assert resp.status_code == 400


class TestPositionsEndpoint:
    def _seed_trades(self, app):
        with app.app_context():
            db.session.add_all([
                Trade(trade_date=date(2025, 1, 15), account_id="ACC001", ticker="AAPL",
                      quantity=100, price="185.50", trade_type=TradeType.BUY,
                      settlement_date=date(2025, 1, 17), source_format=SourceFormat.FORMAT_1),
                Trade(trade_date=date(2025, 1, 15), account_id="ACC001", ticker="MSFT",
                      quantity=50, price="420.25", trade_type=TradeType.BUY,
                      settlement_date=date(2025, 1, 17), source_format=SourceFormat.FORMAT_1),
            ])
            db.session.add(
                Position(report_date=date(2025, 1, 15), account_id="ACC001", ticker="AAPL",
                         shares=100, market_value="18550.00", custodian_ref="CUST_A", source_format="FORMAT_3")
            )
            db.session.commit()

    def test_returns_positions_with_cost_basis(self, client, app):
        self._seed_trades(app)
        resp = client.get("/positions?account=ACC001&date=2025-01-15")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["account_id"] == "ACC001"
        tickers = [p["ticker"] for p in body["positions"]]
        assert "AAPL" in tickers
        assert "MSFT" in tickers

    def test_aapl_cost_basis_correct(self, client, app):
        self._seed_trades(app)
        resp = client.get("/positions?account=ACC001&date=2025-01-15")
        positions = {p["ticker"]: p for p in resp.get_json()["positions"]}
        assert float(positions["AAPL"]["total_cost_basis"]) == 18550.00

    def test_missing_account_returns_400(self, client):
        resp = client.get("/positions?date=2025-01-15")
        assert resp.status_code == 400

    def test_invalid_date_returns_400(self, client):
        resp = client.get("/positions?account=ACC001&date=not-a-date")
        assert resp.status_code == 400


class TestComplianceEndpoint:
    def _seed_positions(self, app):
        with app.app_context():
            # ACC001: AAPL = 90% of portfolio — clear breach
            db.session.add_all([
                Position(report_date=date(2025, 1, 15), account_id="ACC001", ticker="AAPL",
                         shares=500, market_value="90000.00", custodian_ref="X", source_format="FORMAT_3"),
                Position(report_date=date(2025, 1, 15), account_id="ACC001", ticker="MSFT",
                         shares=50, market_value="10000.00", custodian_ref="X", source_format="FORMAT_3"),
            ])
            db.session.commit()

    def test_detects_concentration_breach(self, client, app):
        self._seed_positions(app)
        resp = client.get("/compliance/concentration?date=2025-01-15")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["breach_count"] == 1
        assert body["breaches"][0]["ticker"] == "AAPL"
        assert body["breaches"][0]["account_id"] == "ACC001"

    def test_no_breaches_when_balanced(self, client, app):
        with app.app_context():
            for ticker, mv in [("AAPL", "10000"), ("MSFT", "10000"), ("GOOGL", "10000"), ("TSLA", "10000"), ("NVDA", "10000")]:
                db.session.add(Position(report_date=date(2025, 1, 15), account_id="ACC002",
                                        ticker=ticker, shares=10, market_value=mv,
                                        custodian_ref="X", source_format="FORMAT_3"))
            db.session.commit()
        resp = client.get("/compliance/concentration?date=2025-01-15")
        body = resp.get_json()
        assert body["breach_count"] == 0

    def test_missing_date_returns_400(self, client):
        resp = client.get("/compliance/concentration")
        assert resp.status_code == 400


class TestReconciliationEndpoint:
    def _seed(self, app):
        with app.app_context():
            # Trade says 100 AAPL; position says 90 — 10 share break
            db.session.add(
                Trade(trade_date=date(2025, 1, 15), account_id="ACC001", ticker="AAPL",
                      quantity=100, price="185.50", trade_type=TradeType.BUY,
                      settlement_date=date(2025, 1, 17), source_format=SourceFormat.FORMAT_1)
            )
            db.session.add(
                Position(report_date=date(2025, 1, 15), account_id="ACC001", ticker="AAPL",
                         shares=90, market_value="16695.00", custodian_ref="X", source_format="FORMAT_3")
            )
            db.session.commit()

    def test_detects_break(self, client, app):
        self._seed(app)
        resp = client.get("/reconciliation?date=2025-01-15")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["break_count"] == 1
        b = body["breaks"][0]
        assert b["ticker"] == "AAPL"
        assert b["delta"] == -10   # 90 reported - 100 derived

    def test_no_breaks_when_matched(self, client, app):
        with app.app_context():
            db.session.add(
                Trade(trade_date=date(2025, 1, 15), account_id="ACC001", ticker="MSFT",
                      quantity=50, price="420.00", trade_type=TradeType.BUY,
                      settlement_date=date(2025, 1, 17), source_format=SourceFormat.FORMAT_1)
            )
            db.session.add(
                Position(report_date=date(2025, 1, 15), account_id="ACC001", ticker="MSFT",
                         shares=50, market_value="21000.00", custodian_ref="X", source_format="FORMAT_3")
            )
            db.session.commit()
        resp = client.get("/reconciliation?date=2025-01-15")
        body = resp.get_json()
        assert body["break_count"] == 0

    def test_missing_date_returns_400(self, client):
        resp = client.get("/reconciliation")
        assert resp.status_code == 400
