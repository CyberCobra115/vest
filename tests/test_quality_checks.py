"""
Tests for ingestion quality checks across all formats.
"""

from datetime import date
from decimal import Decimal

import pytest

from app import db
from app.models import Position, Trade, TradeType, SourceFormat
from app.services.ingestion_service import ingest


class TestTradeCsvQualityChecks:
    def test_settlement_before_trade_date_rejected(self, app):
        content = (
            "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"
            "2025-01-15,ACC001,AAPL,100,185.50,BUY,2025-01-13\n"
        )
        with app.app_context():
            report = ingest(content)
        assert report.rows_accepted == 0
        assert report.rows_rejected == 1
        assert any("SettlementDate" in e["field"] for e in report.errors)

    def test_settlement_same_day_accepted(self, app):
        content = (
            "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"
            "2025-01-15,ACC001,AAPL,100,185.50,BUY,2025-01-15\n"
        )
        with app.app_context():
            report = ingest(content)
        assert report.rows_accepted == 1

    def test_sell_stored_with_negative_quantity(self, app):
        content = (
            "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"
            "2025-01-15,ACC001,TSLA,150,238.45,SELL,2025-01-17\n"
        )
        with app.app_context():
            ingest(content)
            trade = db.session.query(Trade).filter_by(ticker="TSLA").first()
        assert trade.quantity == -150

    def test_total_rows_counts_lines_not_error_dicts(self, app):
        # One row producing 3 errors must still count as total_rows=1, rejected=1.
        content = (
            "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"
            "2025-01-15,,AAPL,100,INVALID,HOLD,2025-01-17\n"
        )
        with app.app_context():
            report = ingest(content)
        assert report.total_rows == 1
        assert report.rows_rejected == 1

    def test_intra_file_duplicate_caught(self, app):
        content = (
            "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"
            "2025-01-15,ACC001,AAPL,100,185.50,BUY,2025-01-17\n"
            "2025-01-15,ACC001,AAPL,100,185.50,BUY,2025-01-17\n"
        )
        with app.app_context():
            report = ingest(content)
        assert report.rows_accepted == 1
        assert "duplicate" in report.warnings[0].lower()

    def test_cross_file_duplicate_skipped(self, app):
        content = (
            "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"
            "2025-01-15,ACC001,MSFT,50,420.25,BUY,2025-01-17\n"
        )
        with app.app_context():
            assert ingest(content).rows_accepted == 1
            second = ingest(content)
            assert second.rows_accepted == 0
            assert second.rows_skipped_duplicate == 1

    def test_mixed_valid_and_invalid(self, app):
        content = (
            "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"
            "2025-01-15,ACC001,AAPL,100,185.50,BUY,2025-01-17\n"
            "2025-01-15,,MSFT,50,420.25,BUY,2025-01-17\n"
            "2025-01-15,ACC002,GOOGL,75,142.80,HOLD,2025-01-17\n"
            "2025-01-15,ACC003,NVDA,80,505.30,BUY,2025-01-17\n"
        )
        with app.app_context():
            report = ingest(content)
        assert report.rows_accepted == 2
        assert report.rows_rejected == 2


class TestTradePipeQualityChecks:
    def test_sell_stored_with_negative_quantity(self, app):
        content = (
            "REPORT_DATE|ACCOUNT_ID|SECURITY_TICKER|SHARES|MARKET_VALUE|SOURCE_SYSTEM\n"
            "20250115|ACC001|TSLA|-150|35767.50|CUSTODIAN_A\n"
        )
        with app.app_context():
            ingest(content)
            trade = db.session.query(Trade).filter_by(ticker="TSLA").first()
        assert trade.quantity == -150
        assert trade.trade_type == TradeType.SELL

    def test_intra_file_duplicate_caught(self, app):
        content = (
            "REPORT_DATE|ACCOUNT_ID|SECURITY_TICKER|SHARES|MARKET_VALUE|SOURCE_SYSTEM\n"
            "20250115|ACC001|AAPL|100|18550.00|CUSTODIAN_A\n"
            "20250115|ACC001|AAPL|100|18550.00|CUSTODIAN_A\n"
        )
        with app.app_context():
            report = ingest(content)
        assert report.rows_accepted == 1

    def test_cross_file_duplicate_skipped(self, app):
        content = (
            "REPORT_DATE|ACCOUNT_ID|SECURITY_TICKER|SHARES|MARKET_VALUE|SOURCE_SYSTEM\n"
            "20250115|ACC001|MSFT|50|21012.50|CUSTODIAN_A\n"
        )
        with app.app_context():
            assert ingest(content).rows_accepted == 1
            second = ingest(content)
            assert second.rows_skipped_duplicate == 1


class TestPositionYamlQualityChecks:
    def test_intra_file_duplicate_caught(self, app):
        content = (
            'report_date: "20250115"\n'
            'positions:\n'
            '  - account_id: "ACC001"\n'
            '    ticker: "AAPL"\n'
            '    shares: 100\n'
            '    market_value: 18550.00\n'
            '    custodian_ref: "CUST_A_001"\n'
            '  - account_id: "ACC001"\n'
            '    ticker: "AAPL"\n'
            '    shares: 100\n'
            '    market_value: 18550.00\n'
            '    custodian_ref: "CUST_A_001"\n'
        )
        with app.app_context():
            report = ingest(content)
        assert report.rows_accepted == 1

    def test_upsert_when_values_change(self, app):
        original = (
            'report_date: "20250115"\n'
            'positions:\n'
            '  - account_id: "ACC002"\n'
            '    ticker: "NVDA"\n'
            '    shares: 80\n'
            '    market_value: 40424.00\n'
            '    custodian_ref: "CUST_B_001"\n'
        )
        updated = original.replace("shares: 80", "shares: 95").replace(
            "market_value: 40424.00", "market_value: 47753.50"
        )
        with app.app_context():
            ingest(original)
            report = ingest(updated)
        assert report.rows_upserted == 1

        with app.app_context():
            pos = db.session.query(Position).filter_by(
                account_id="ACC002", ticker="NVDA"
            ).first()
            assert pos.shares == 95

    def test_unchanged_position_skipped_with_warning(self, app):
        content = (
            'report_date: "20250115"\n'
            'positions:\n'
            '  - account_id: "ACC003"\n'
            '    ticker: "TSLA"\n'
            '    shares: -150\n'
            '    market_value: -35767.50\n'
            '    custodian_ref: "CUST_A_999"\n'
        )
        with app.app_context():
            ingest(content)
            report = ingest(content)
        assert report.rows_upserted == 0
        assert any("unchanged" in w.lower() for w in report.warnings)

    def test_reingest_same_file_all_skipped(self, app):
        content = (
            'report_date: "20250115"\n'
            'positions:\n'
            '  - account_id: "ACC004"\n'
            '    ticker: "AAPL"\n'
            '    shares: 500\n'
            '    market_value: 92750.00\n'
            '    custodian_ref: "CUST_C_001"\n'
            '  - account_id: "ACC004"\n'
            '    ticker: "MSFT"\n'
            '    shares: 300\n'
            '    market_value: 126075.00\n'
            '    custodian_ref: "CUST_C_002"\n'
        )
        with app.app_context():
            assert ingest(content).rows_accepted == 2
            second = ingest(content)
            assert second.rows_accepted == 0
            assert second.rows_upserted == 0
