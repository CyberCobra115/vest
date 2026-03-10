"""
Tests specifically for Requirement 3: data ingestion quality checks.

Covers:
  - Settlement date before trade date (Format 1)
  - Intra-file duplicate detection (all formats)
  - Cross-file duplicate detection / skip on reingest (Format 1)
  - Position upsert: changed values update in place (Format 2 & 3)
  - Position upsert: unchanged values skipped with warning (Format 2 & 3)
"""

from datetime import date
from decimal import Decimal

import pytest

from app import db
from app.models import Position, Trade, TradeType, SourceFormat
from app.services.ingestion_service import ingest


class TestFormat1QualityChecks:
    def test_settlement_before_trade_date_rejected(self, app):
        content = (
            "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"
            "2025-01-15,ACC001,AAPL,100,185.50,BUY,2025-01-13\n"  # settlement 2 days BEFORE trade
        )
        with app.app_context():
            report = ingest(content)
        assert report.rows_accepted == 0
        assert report.rows_rejected == 1
        assert any("SettlementDate" in e["field"] for e in report.errors)

    def test_settlement_same_as_trade_date_accepted(self, app):
        content = (
            "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"
            "2025-01-15,ACC001,AAPL,100,185.50,BUY,2025-01-15\n"
        )
        with app.app_context():
            report = ingest(content)
        assert report.rows_accepted == 1
        assert report.rows_rejected == 0

    def test_intra_file_duplicate_caught(self, app):
        content = (
            "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"
            "2025-01-15,ACC001,AAPL,100,185.50,BUY,2025-01-17\n"
            "2025-01-15,ACC001,AAPL,100,185.50,BUY,2025-01-17\n"  # exact duplicate
        )
        with app.app_context():
            report = ingest(content)
        assert report.rows_accepted == 1
        assert len(report.warnings) == 1
        assert "duplicate" in report.warnings[0].lower()

    def test_cross_file_duplicate_skipped_on_reingest(self, app):
        content = (
            "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"
            "2025-01-15,ACC001,MSFT,50,420.25,BUY,2025-01-17\n"
        )
        with app.app_context():
            first = ingest(content)
            assert first.rows_accepted == 1

            second = ingest(content)
            assert second.rows_accepted == 0
            assert second.rows_skipped_duplicate == 1
            assert any("already exists" in w for w in second.warnings)

    def test_different_trades_not_flagged_as_duplicates(self, app):
        content = (
            "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"
            "2025-01-15,ACC001,AAPL,100,185.50,BUY,2025-01-17\n"
            "2025-01-15,ACC001,AAPL,200,185.50,BUY,2025-01-17\n"  # different quantity
        )
        with app.app_context():
            report = ingest(content)
        assert report.rows_accepted == 2
        assert report.rows_skipped_duplicate == 0

    def test_mixed_valid_and_invalid_rows(self, app):
        content = (
            "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"
            "2025-01-15,ACC001,AAPL,100,185.50,BUY,2025-01-17\n"   # valid
            "2025-01-15,,MSFT,50,420.25,BUY,2025-01-17\n"          # missing AccountID
            "2025-01-15,ACC002,GOOGL,75,142.80,HOLD,2025-01-17\n"  # invalid TradeType
            "2025-01-15,ACC003,NVDA,80,505.30,BUY,2025-01-17\n"    # valid
        )
        with app.app_context():
            report = ingest(content)
        assert report.rows_accepted == 2
        assert report.rows_rejected == 2


class TestFormat2QualityChecks:
    def test_intra_file_duplicate_caught(self, app):
        content = (
            "REPORT_DATE|ACCOUNT_ID|SECURITY_TICKER|SHARES|MARKET_VALUE|SOURCE_SYSTEM\n"
            "20250115|ACC001|AAPL|100|18550.00|CUSTODIAN_A\n"
            "20250115|ACC001|AAPL|100|18550.00|CUSTODIAN_A\n"  # exact duplicate
        )
        with app.app_context():
            report = ingest(content)
        assert report.rows_accepted == 1
        assert len(report.warnings) == 1

    def test_position_upsert_when_values_change(self, app):
        original = (
            "REPORT_DATE|ACCOUNT_ID|SECURITY_TICKER|SHARES|MARKET_VALUE|SOURCE_SYSTEM\n"
            "20250115|ACC001|AAPL|100|18550.00|CUSTODIAN_A\n"
        )
        updated = (
            "REPORT_DATE|ACCOUNT_ID|SECURITY_TICKER|SHARES|MARKET_VALUE|SOURCE_SYSTEM\n"
            "20250115|ACC001|AAPL|110|20405.00|CUSTODIAN_A\n"  # corrected shares+value
        )
        with app.app_context():
            ingest(original)
            report = ingest(updated)

        assert report.rows_upserted == 1
        assert any("upsert" in w.lower() for w in report.warnings)

        with app.app_context():
            pos = db.session.query(Position).filter_by(
                account_id="ACC001", ticker="AAPL", custodian_ref="CUSTODIAN_A"
            ).first()
            assert pos.shares == 110
            assert pos.market_value == Decimal("20405.00")

    def test_position_unchanged_skipped_with_warning(self, app):
        content = (
            "REPORT_DATE|ACCOUNT_ID|SECURITY_TICKER|SHARES|MARKET_VALUE|SOURCE_SYSTEM\n"
            "20250115|ACC001|MSFT|50|21012.50|CUSTODIAN_A\n"
        )
        with app.app_context():
            ingest(content)
            report = ingest(content)  # reingest same data

        assert report.rows_accepted == 0
        assert report.rows_upserted == 0
        assert any("unchanged" in w.lower() for w in report.warnings)


class TestFormat3QualityChecks:
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
            '    custodian_ref: "CUST_A_001"\n'  # exact duplicate
        )
        with app.app_context():
            report = ingest(content)
        assert report.rows_accepted == 1
        assert len(report.warnings) == 1

    def test_position_upsert_when_values_change(self, app):
        original = (
            'report_date: "20250115"\n'
            'positions:\n'
            '  - account_id: "ACC002"\n'
            '    ticker: "NVDA"\n'
            '    shares: 80\n'
            '    market_value: 40424.00\n'
            '    custodian_ref: "CUST_B_001"\n'
        )
        updated = (
            'report_date: "20250115"\n'
            'positions:\n'
            '  - account_id: "ACC002"\n'
            '    ticker: "NVDA"\n'
            '    shares: 95\n'
            '    market_value: 47753.50\n'
            '    custodian_ref: "CUST_B_001"\n'
        )
        with app.app_context():
            ingest(original)
            report = ingest(updated)

        assert report.rows_upserted == 1

        with app.app_context():
            pos = db.session.query(Position).filter_by(
                account_id="ACC002", ticker="NVDA", custodian_ref="CUST_B_001"
            ).first()
            assert pos.shares == 95

    def test_position_unchanged_skipped_with_warning(self, app):
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

    def test_reingest_clean_file_all_skipped(self, app):
        """Simulates re-running a daily load with the same file."""
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
            first = ingest(content)
            assert first.rows_accepted == 2

            second = ingest(content)
            assert second.rows_accepted == 0
            assert second.rows_upserted == 0
            assert len(second.warnings) == 2
