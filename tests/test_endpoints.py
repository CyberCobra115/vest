import pytest
from datetime import date

from app import db
from app.models import Position, Trade, TradeType, SourceFormat
from tests.conftest import TRADE_CSV_CONTENT, TRADE_PIPE_CONTENT, POSITION_YAML_CONTENT


class TestIngestEndpoint:
    def test_ingest_trade_csv_returns_quality_report(self, client):
        resp = client.post("/ingest", data=TRADE_CSV_CONTENT, content_type="text/plain")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["format_detected"] == "TRADE_CSV"
        assert body["rows_accepted"] + body["rows_rejected"] == body["total_rows"]
        assert body["rows_accepted"] == 4
        assert body["rows_rejected"] == 0

    def test_ingest_trade_pipe_stores_in_trades_table(self, client, app):
        resp = client.post("/ingest", data=TRADE_PIPE_CONTENT, content_type="text/plain")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["format_detected"] == "TRADE_PIPE"
        assert body["rows_accepted"] == 4

        with app.app_context():
            count = db.session.query(Trade).filter(
                Trade.source_format == SourceFormat.TRADE_PIPE
            ).count()
            assert count == 4

    def test_ingest_trade_pipe_sell_stored_negative(self, client, app):
        client.post("/ingest", data=TRADE_PIPE_CONTENT, content_type="text/plain")
        with app.app_context():
            tsla = db.session.query(Trade).filter(
                Trade.ticker == "TSLA",
                Trade.source_format == SourceFormat.TRADE_PIPE,
            ).first()
            assert tsla is not None
            assert tsla.quantity < 0
            assert tsla.trade_type == TradeType.SELL

    def test_ingest_position_yaml_stores_in_positions_table(self, client, app):
        resp = client.post("/ingest", data=POSITION_YAML_CONTENT, content_type="text/plain")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["format_detected"] == "POSITION_YAML"
        assert body["rows_accepted"] + body["rows_rejected"] == body["total_rows"]
        assert body["rows_accepted"] == 3

        with app.app_context():
            count = db.session.query(Position).filter(
                Position.source_format == SourceFormat.POSITION_YAML
            ).count()
            assert count == 3

    def test_partial_failure_returns_207(self, client):
        bad_row = (
            "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"
            "2025-01-15,ACC001,AAPL,100,185.50,BUY,2025-01-17\n"
            "2025-01-15,,MSFT,50,420.25,BUY,2025-01-17\n"
        )
        resp = client.post("/ingest", data=bad_row, content_type="text/plain")
        assert resp.status_code == 207
        body = resp.get_json()
        assert body["rows_accepted"] == 1
        assert body["rows_rejected"] == 1

    def test_all_rows_rejected_returns_400(self, client):
        all_bad = (
            "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"
            "2025-01-15,,AAPL,100,185.50,BUY,2025-01-17\n"
            "2025-01-15,,MSFT,50,420.25,BUY,2025-01-17\n"
        )
        resp = client.post("/ingest", data=all_bad, content_type="text/plain")
        assert resp.status_code == 400

    def test_no_body_returns_400(self, client):
        resp = client.post("/ingest")
        assert resp.status_code == 400

    def test_unknown_format_returns_400(self, client):
        resp = client.post("/ingest", data="garbage data\n1,2,3",
                           content_type="text/plain")
        assert resp.status_code == 400


class TestPositionsEndpoint:
    def _seed(self, app):
        with app.app_context():
            db.session.add_all([
                Trade(trade_date=date(2025, 1, 15), account_id="ACC001",
                      ticker="AAPL", quantity=100, price="185.50",
                      trade_type=TradeType.BUY, settlement_date=date(2025, 1, 17),
                      source_format=SourceFormat.TRADE_CSV),
                Trade(trade_date=date(2025, 1, 15), account_id="ACC001",
                      ticker="MSFT", quantity=50, price="420.25",
                      trade_type=TradeType.BUY, settlement_date=date(2025, 1, 17),
                      source_format=SourceFormat.TRADE_CSV),
            ])
            db.session.add(
                Position(report_date=date(2025, 1, 15), account_id="ACC001",
                         ticker="AAPL", shares=100, market_value="18550.00",
                         custodian_ref="CUST_A", source_format=SourceFormat.POSITION_YAML)
            )
            db.session.commit()

    def test_returns_positions_with_cost_basis(self, client, app):
        self._seed(app)
        resp = client.get("/positions?account=ACC001&date=2025-01-15")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["account_id"] == "ACC001"
        tickers = [p["ticker"] for p in body["positions"]]
        assert "AAPL" in tickers
        assert "MSFT" in tickers

    def test_cost_basis_correct(self, client, app):
        self._seed(app)
        resp = client.get("/positions?account=ACC001&date=2025-01-15")
        positions = {p["ticker"]: p for p in resp.get_json()["positions"]}
        assert float(positions["AAPL"]["total_cost_basis"]) == pytest.approx(18550.00)

    def test_no_position_snapshot_returns_null_market_value(self, client, app):
        with app.app_context():
            db.session.add(
                Trade(trade_date=date(2025, 1, 15), account_id="ACC099",
                      ticker="AAPL", quantity=10, price="185.50",
                      trade_type=TradeType.BUY, settlement_date=date(2025, 1, 17),
                      source_format=SourceFormat.TRADE_CSV)
            )
            db.session.commit()
        resp = client.get("/positions?account=ACC099&date=2025-01-15")
        pos = resp.get_json()["positions"][0]
        assert pos["market_value"] is None
        assert pos["unrealized_pnl"] is None

    def test_sell_reduces_net_quantity(self, client, app):
        with app.app_context():
            db.session.add_all([
                Trade(trade_date=date(2025, 1, 10), account_id="ACC001",
                      ticker="AAPL", quantity=100, price="180.00",
                      trade_type=TradeType.BUY, settlement_date=date(2025, 1, 12),
                      source_format=SourceFormat.TRADE_CSV),
                Trade(trade_date=date(2025, 1, 15), account_id="ACC001",
                      ticker="AAPL", quantity=-40, price="190.00",
                      trade_type=TradeType.SELL, settlement_date=date(2025, 1, 17),
                      source_format=SourceFormat.TRADE_CSV),
            ])
            db.session.commit()
        resp = client.get("/positions?account=ACC001&date=2025-01-15")
        positions = {p["ticker"]: p for p in resp.get_json()["positions"]}
        assert positions["AAPL"]["net_quantity"] == 60

    def test_fully_closed_position_not_returned(self, client, app):
        with app.app_context():
            db.session.add_all([
                Trade(trade_date=date(2025, 1, 10), account_id="ACC001",
                      ticker="AAPL", quantity=100, price="180.00",
                      trade_type=TradeType.BUY, settlement_date=date(2025, 1, 12),
                      source_format=SourceFormat.TRADE_CSV),
                Trade(trade_date=date(2025, 1, 15), account_id="ACC001",
                      ticker="AAPL", quantity=-100, price="190.00",
                      trade_type=TradeType.SELL, settlement_date=date(2025, 1, 17),
                      source_format=SourceFormat.TRADE_CSV),
            ])
            db.session.commit()
        resp = client.get("/positions?account=ACC001&date=2025-01-15")
        tickers = [p["ticker"] for p in resp.get_json()["positions"]]
        assert "AAPL" not in tickers

    def test_missing_account_returns_400(self, client):
        assert client.get("/positions?date=2025-01-15").status_code == 400

    def test_invalid_date_returns_400(self, client):
        assert client.get("/positions?account=ACC001&date=bad").status_code == 400


class TestComplianceEndpoint:
    def _seed(self, app):
        with app.app_context():
            db.session.add_all([
                Position(report_date=date(2025, 1, 15), account_id="ACC001",
                         ticker="AAPL", shares=500, market_value="90000.00",
                         custodian_ref="X", source_format=SourceFormat.POSITION_YAML),
                Position(report_date=date(2025, 1, 15), account_id="ACC001",
                         ticker="MSFT", shares=50, market_value="10000.00",
                         custodian_ref="X", source_format=SourceFormat.POSITION_YAML),
            ])
            db.session.commit()

    def test_detects_concentration_breach(self, client, app):
        self._seed(app)
        resp = client.get("/compliance/concentration?date=2025-01-15")
        body = resp.get_json()
        assert body["breach_count"] == 1
        assert body["breaches"][0]["ticker"] == "AAPL"

    def test_no_breaches_when_balanced(self, client, app):
        with app.app_context():
            for ticker in ["AAPL", "MSFT", "GOOGL", "TSLA", "NVDA"]:
                db.session.add(Position(
                    report_date=date(2025, 1, 15), account_id="ACC002",
                    ticker=ticker, shares=10, market_value="10000",
                    custodian_ref="X", source_format=SourceFormat.POSITION_YAML))
            db.session.commit()
        resp = client.get("/compliance/concentration?date=2025-01-15")
        assert resp.get_json()["breach_count"] == 0

    def test_exactly_20_pct_is_not_a_breach(self, client, app):
        with app.app_context():
            for ticker in ["AAPL", "MSFT", "GOOGL", "TSLA", "NVDA"]:
                db.session.add(Position(
                    report_date=date(2025, 1, 15), account_id="ACC003",
                    ticker=ticker, shares=10, market_value="10000",
                    custodian_ref="X", source_format=SourceFormat.POSITION_YAML))
            db.session.commit()
        assert client.get("/compliance/concentration?date=2025-01-15").get_json()[
            "breach_count"] == 0

    def test_missing_date_returns_400(self, client):
        assert client.get("/compliance/concentration").status_code == 400


class TestReconciliationEndpoint:
    def _seed(self, app):
        with app.app_context():
            db.session.add(Trade(
                trade_date=date(2025, 1, 15), account_id="ACC001", ticker="AAPL",
                quantity=100, price="185.50", trade_type=TradeType.BUY,
                settlement_date=date(2025, 1, 17),
                source_format=SourceFormat.TRADE_CSV))
            db.session.add(Position(
                report_date=date(2025, 1, 15), account_id="ACC001", ticker="AAPL",
                shares=90, market_value="16695.00", custodian_ref="X",
                source_format=SourceFormat.POSITION_YAML))
            db.session.commit()

    def test_detects_break(self, client, app):
        self._seed(app)
        resp = client.get("/reconciliation?date=2025-01-15")
        body = resp.get_json()
        assert body["break_count"] == 1
        assert body["breaks"][0]["delta"] == -10

    def test_sell_reduces_derived_quantity(self, client, app):
        with app.app_context():
            db.session.add_all([
                Trade(trade_date=date(2025, 1, 15), account_id="ACC001",
                      ticker="MSFT", quantity=100, price="420.00",
                      trade_type=TradeType.BUY, settlement_date=date(2025, 1, 17),
                      source_format=SourceFormat.TRADE_CSV),
                Trade(trade_date=date(2025, 1, 15), account_id="ACC001",
                      ticker="MSFT", quantity=-30, price="425.00",
                      trade_type=TradeType.SELL, settlement_date=date(2025, 1, 17),
                      source_format=SourceFormat.TRADE_CSV),
            ])
            db.session.add(Position(
                report_date=date(2025, 1, 15), account_id="ACC001", ticker="MSFT",
                shares=70, market_value="29750.00", custodian_ref="X",
                source_format=SourceFormat.POSITION_YAML))
            db.session.commit()
        assert client.get("/reconciliation?date=2025-01-15").get_json()["break_count"] == 0

    def test_no_breaks_when_matched(self, client, app):
        with app.app_context():
            db.session.add(Trade(
                trade_date=date(2025, 1, 15), account_id="ACC001", ticker="MSFT",
                quantity=50, price="420.00", trade_type=TradeType.BUY,
                settlement_date=date(2025, 1, 17), source_format=SourceFormat.TRADE_CSV))
            db.session.add(Position(
                report_date=date(2025, 1, 15), account_id="ACC001", ticker="MSFT",
                shares=50, market_value="21000.00", custodian_ref="X",
                source_format=SourceFormat.POSITION_YAML))
            db.session.commit()
        assert client.get("/reconciliation?date=2025-01-15").get_json()["break_count"] == 0

    def test_missing_date_returns_400(self, client):
        assert client.get("/reconciliation").status_code == 400
