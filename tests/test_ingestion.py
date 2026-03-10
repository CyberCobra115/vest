import pytest
from decimal import Decimal
from datetime import date

from app.ingestion.detector import detect_format, FileFormat
from app.ingestion import format1, format2, format3
from tests.conftest import FORMAT_1_CONTENT, FORMAT_2_CONTENT, FORMAT_3_CONTENT


class TestFormatDetection:
    def test_detects_format_1(self):
        assert detect_format(FORMAT_1_CONTENT) == FileFormat.FORMAT_1

    def test_detects_format_2(self):
        assert detect_format(FORMAT_2_CONTENT) == FileFormat.FORMAT_2

    def test_detects_format_3(self):
        assert detect_format(FORMAT_3_CONTENT) == FileFormat.FORMAT_3

    def test_unknown_format(self):
        assert detect_format("garbage,data\n1,2,3") == FileFormat.UNKNOWN

    def test_empty_content(self):
        assert detect_format("") == FileFormat.UNKNOWN


class TestFormat1Parser:
    def test_parses_valid_rows(self):
        rows, errors = format1.parse(FORMAT_1_CONTENT)
        assert len(rows) == 4
        assert errors == []

    def test_first_row_values(self):
        rows, _ = format1.parse(FORMAT_1_CONTENT)
        r = rows[0]
        assert r.account_id == "ACC001"
        assert r.ticker == "AAPL"
        assert r.quantity == 100
        assert r.price == Decimal("185.50")
        assert r.trade_type == "BUY"
        assert r.trade_date == date(2025, 1, 15)

    def test_sell_row(self):
        rows, _ = format1.parse(FORMAT_1_CONTENT)
        sell = next(r for r in rows if r.trade_type == "SELL")
        assert sell.ticker == "TSLA"
        assert sell.quantity == 150

    def test_missing_field_produces_error(self):
        bad = "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n2025-01-15,,AAPL,100,185.50,BUY,2025-01-17\n"
        rows, errors = format1.parse(bad)
        assert len(rows) == 0
        assert any(e["field"] == "AccountID" for e in errors)

    def test_invalid_trade_type(self):
        bad = "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n2025-01-15,ACC001,AAPL,100,185.50,HOLD,2025-01-17\n"
        _, errors = format1.parse(bad)
        assert any(e["field"] == "TradeType" for e in errors)

    def test_negative_price_rejected(self):
        bad = "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n2025-01-15,ACC001,AAPL,100,-10.00,BUY,2025-01-17\n"
        _, errors = format1.parse(bad)
        assert any(e["field"] == "Price" for e in errors)


class TestFormat2Parser:
    def test_parses_valid_rows(self):
        rows, errors = format2.parse(FORMAT_2_CONTENT)
        assert len(rows) == 4
        assert errors == []

    def test_short_position_negative_shares(self):
        rows, _ = format2.parse(FORMAT_2_CONTENT)
        tsla = next(r for r in rows if r.ticker == "TSLA")
        assert tsla.shares == -150

    def test_date_parsing_compact_format(self):
        rows, _ = format2.parse(FORMAT_2_CONTENT)
        assert rows[0].report_date == date(2025, 1, 15)

    def test_missing_field_rejected(self):
        bad = "REPORT_DATE|ACCOUNT_ID|SECURITY_TICKER|SHARES|MARKET_VALUE|SOURCE_SYSTEM\n20250115||AAPL|100|18550.00|CUSTODIAN_A\n"
        rows, errors = format2.parse(bad)
        assert len(rows) == 0
        assert any(e["field"] == "ACCOUNT_ID" for e in errors)


class TestFormat3Parser:
    def test_parses_valid_yaml(self):
        rows, errors = format3.parse(FORMAT_3_CONTENT)
        assert len(rows) == 3
        assert errors == []

    def test_first_row_values(self):
        rows, _ = format3.parse(FORMAT_3_CONTENT)
        r = rows[0]
        assert r.account_id == "ACC001"
        assert r.ticker == "AAPL"
        assert r.shares == 100
        assert r.market_value == Decimal("18550.00")
        assert r.custodian_ref == "CUST_A_12345"

    def test_report_date_applied_to_all_rows(self):
        rows, _ = format3.parse(FORMAT_3_CONTENT)
        for row in rows:
            assert row.report_date == date(2025, 1, 15)

    def test_malformed_yaml_returns_error(self):
        rows, errors = format3.parse("not: valid: yaml: {{{{")
        assert rows == []
        assert len(errors) > 0

    def test_missing_required_field(self):
        bad = 'report_date: "20250115"\npositions:\n  - account_id: "ACC001"\n    shares: 100\n    market_value: 100.00\n'
        rows, errors = format3.parse(bad)
        assert len(rows) == 0
        assert any(e["field"] == "ticker" for e in errors)
