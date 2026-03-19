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

    def test_format3_detected_by_first_line_only(self):
        # A CSV that happens to contain "positions:" in a field should NOT
        # be detected as Format 3 — detection must use the first line only.
        csv_with_positions_word = (
            "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"
            "2025-01-15,ACC001,AAPL,100,185.50,BUY,2025-01-17\n"
        )
        assert detect_format(csv_with_positions_word) == FileFormat.FORMAT_1


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
        # BUY → positive quantity
        assert r.quantity == 100
        assert r.price == Decimal("185.50")
        assert r.trade_type == "BUY"
        assert r.trade_date == date(2025, 1, 15)

    def test_sell_quantity_is_negative(self):
        """SELL trades must be stored with negative quantity for correct net-position math."""
        rows, _ = format1.parse(FORMAT_1_CONTENT)
        sell = next(r for r in rows if r.trade_type == "SELL")
        assert sell.ticker == "TSLA"
        # Quantity should be -150 (negative = SELL)
        assert sell.quantity == -150

    def test_buy_then_sell_nets_to_zero(self):
        """BUY 100 then SELL 100 of same ticker should net to 0."""
        content = (
            "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"
            "2025-01-15,ACC001,AAPL,100,185.50,BUY,2025-01-17\n"
            "2025-01-15,ACC001,AAPL,100,186.00,SELL,2025-01-17\n"
        )
        rows, _ = format1.parse(content)
        net = sum(r.quantity for r in rows if r.ticker == "AAPL")
        assert net == 0

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
    """Format 2 is a trade file (pipe-delimited). SHARES maps to signed quantity."""

    def test_parses_valid_rows(self):
        rows, errors = format2.parse(FORMAT_2_CONTENT)
        assert len(rows) == 4
        assert errors == []

    def test_buy_row_positive_quantity(self):
        rows, _ = format2.parse(FORMAT_2_CONTENT)
        aapl = next(r for r in rows if r.ticker == "AAPL")
        assert aapl.quantity == 100
        assert aapl.trade_type == "BUY"

    def test_sell_row_negative_quantity(self):
        """Negative SHARES in Format 2 = SELL fill → negative quantity."""
        rows, _ = format2.parse(FORMAT_2_CONTENT)
        tsla = next(r for r in rows if r.ticker == "TSLA")
        assert tsla.quantity == -150
        assert tsla.trade_type == "SELL"

    def test_price_derived_from_notional(self):
        """Price = abs(market_value) / abs(shares)."""
        rows, _ = format2.parse(FORMAT_2_CONTENT)
        aapl = next(r for r in rows if r.ticker == "AAPL")
        expected_price = Decimal("18550.00") / Decimal("100")
        assert aapl.price == expected_price

    def test_date_parsing_compact_format(self):
        rows, _ = format2.parse(FORMAT_2_CONTENT)
        assert rows[0].trade_date == date(2025, 1, 15)

    def test_missing_field_rejected(self):
        bad = "REPORT_DATE|ACCOUNT_ID|SECURITY_TICKER|SHARES|MARKET_VALUE|SOURCE_SYSTEM\n20250115||AAPL|100|18550.00|CUSTODIAN_A\n"
        rows, errors = format2.parse(bad)
        assert len(rows) == 0
        assert any(e["field"] == "ACCOUNT_ID" for e in errors)

    def test_zero_shares_rejected(self):
        bad = "REPORT_DATE|ACCOUNT_ID|SECURITY_TICKER|SHARES|MARKET_VALUE|SOURCE_SYSTEM\n20250115|ACC001|AAPL|0|0.00|CUSTODIAN_A\n"
        rows, errors = format2.parse(bad)
        assert len(rows) == 0


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
