"""
Unit tests for format detection and all three parsers.
"""

from decimal import Decimal
from datetime import date

import pytest

from app.ingestion.detector import detect, FileFormat
from app.ingestion import trade_csv, trade_pipe, position_yaml
from tests.conftest import TRADE_CSV_CONTENT, TRADE_PIPE_CONTENT, POSITION_YAML_CONTENT


class TestFormatDetection:
    def test_detects_trade_csv(self):
        assert detect(TRADE_CSV_CONTENT) == FileFormat.TRADE_CSV

    def test_detects_trade_pipe(self):
        assert detect(TRADE_PIPE_CONTENT) == FileFormat.TRADE_PIPE

    def test_detects_position_yaml(self):
        assert detect(POSITION_YAML_CONTENT) == FileFormat.POSITION_YAML

    def test_unknown_format(self):
        assert detect("garbage,data\n1,2,3") == FileFormat.UNKNOWN

    def test_empty_content(self):
        assert detect("") == FileFormat.UNKNOWN

    def test_detection_uses_first_line_only(self):
        # A CSV whose body contains 'report_date:' must still be TRADE_CSV.
        csv_with_yaml_keyword = (
            "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"
            "2025-01-15,ACC001,AAPL,100,185.50,BUY,2025-01-17\n"
        )
        assert detect(csv_with_yaml_keyword) == FileFormat.TRADE_CSV

    def test_leading_blank_lines_ignored(self):
        # Detection must work even when the file starts with blank lines.
        content = "\n\n" + TRADE_CSV_CONTENT
        assert detect(content) == FileFormat.TRADE_CSV


class TestTradeCsvParser:
    def test_parses_valid_rows(self):
        rows, errors = trade_csv.parse(TRADE_CSV_CONTENT)
        assert len(rows) == 4
        assert errors == []

    def test_buy_quantity_is_positive(self):
        rows, _ = trade_csv.parse(TRADE_CSV_CONTENT)
        aapl = next(r for r in rows if r.ticker == "AAPL")
        assert aapl.quantity == 100
        assert aapl.trade_type == "BUY"

    def test_sell_quantity_is_negative(self):
        rows, _ = trade_csv.parse(TRADE_CSV_CONTENT)
        tsla = next(r for r in rows if r.ticker == "TSLA")
        assert tsla.quantity == -150
        assert tsla.trade_type == "SELL"

    def test_buy_then_sell_nets_to_zero(self):
        content = (
            "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"
            "2025-01-15,ACC001,AAPL,100,185.50,BUY,2025-01-17\n"
            "2025-01-15,ACC001,AAPL,100,186.00,SELL,2025-01-17\n"
        )
        rows, _ = trade_csv.parse(content)
        assert sum(r.quantity for r in rows) == 0

    def test_missing_account_id_produces_error(self):
        bad = (
            "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"
            "2025-01-15,,AAPL,100,185.50,BUY,2025-01-17\n"
        )
        rows, errors = trade_csv.parse(bad)
        assert len(rows) == 0
        assert any(e["field"] == "AccountID" for e in errors)

    def test_invalid_trade_type_rejected(self):
        bad = (
            "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"
            "2025-01-15,ACC001,AAPL,100,185.50,HOLD,2025-01-17\n"
        )
        _, errors = trade_csv.parse(bad)
        assert any(e["field"] == "TradeType" for e in errors)

    def test_negative_price_rejected(self):
        bad = (
            "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"
            "2025-01-15,ACC001,AAPL,100,-10.00,BUY,2025-01-17\n"
        )
        _, errors = trade_csv.parse(bad)
        assert any(e["field"] == "Price" for e in errors)


class TestTradePipeParser:
    """Pipe-delimited trade fills. SHARES is the signed day quantity."""

    def test_parses_valid_rows(self):
        rows, errors = trade_pipe.parse(TRADE_PIPE_CONTENT)
        assert len(rows) == 4
        assert errors == []

    def test_buy_positive_quantity(self):
        rows, _ = trade_pipe.parse(TRADE_PIPE_CONTENT)
        aapl = next(r for r in rows if r.ticker == "AAPL")
        assert aapl.quantity == 100
        assert aapl.trade_type == "BUY"

    def test_sell_negative_quantity(self):
        rows, _ = trade_pipe.parse(TRADE_PIPE_CONTENT)
        tsla = next(r for r in rows if r.ticker == "TSLA")
        assert tsla.quantity == -150
        assert tsla.trade_type == "SELL"

    def test_price_derived_from_notional(self):
        rows, _ = trade_pipe.parse(TRADE_PIPE_CONTENT)
        aapl = next(r for r in rows if r.ticker == "AAPL")
        assert aapl.price == Decimal("18550.00") / Decimal("100")

    def test_date_parsing_compact_format(self):
        rows, _ = trade_pipe.parse(TRADE_PIPE_CONTENT)
        assert rows[0].trade_date == date(2025, 1, 15)

    def test_zero_shares_rejected(self):
        bad = (
            "REPORT_DATE|ACCOUNT_ID|SECURITY_TICKER|SHARES|MARKET_VALUE|SOURCE_SYSTEM\n"
            "20250115|ACC001|AAPL|0|0.00|CUSTODIAN_A\n"
        )
        rows, errors = trade_pipe.parse(bad)
        assert len(rows) == 0

    def test_missing_account_id_rejected(self):
        bad = (
            "REPORT_DATE|ACCOUNT_ID|SECURITY_TICKER|SHARES|MARKET_VALUE|SOURCE_SYSTEM\n"
            "20250115||AAPL|100|18550.00|CUSTODIAN_A\n"
        )
        rows, errors = trade_pipe.parse(bad)
        assert len(rows) == 0
        assert any(e["field"] == "ACCOUNT_ID" for e in errors)


class TestPositionYamlParser:
    """YAML position snapshots. Each row is total shares held, not a daily delta."""

    def test_parses_valid_yaml(self):
        rows, errors = position_yaml.parse(POSITION_YAML_CONTENT)
        assert len(rows) == 3
        assert errors == []

    def test_first_row_values(self):
        rows, _ = position_yaml.parse(POSITION_YAML_CONTENT)
        r = rows[0]
        assert r.account_id == "ACC001"
        assert r.ticker == "AAPL"
        assert r.shares == 100
        assert r.market_value == Decimal("18550.00")
        assert r.custodian_ref == "CUST_A_12345"

    def test_report_date_applied_to_all_rows(self):
        rows, _ = position_yaml.parse(POSITION_YAML_CONTENT)
        for row in rows:
            assert row.report_date == date(2025, 1, 15)

    def test_malformed_yaml_returns_error(self):
        rows, errors = position_yaml.parse("not: valid: yaml: {{{{")
        assert rows == []
        assert len(errors) > 0

    def test_missing_ticker_produces_error(self):
        bad = (
            'report_date: "20250115"\n'
            'positions:\n'
            '  - account_id: "ACC001"\n'
            '    shares: 100\n'
            '    market_value: 100.00\n'
        )
        rows, errors = position_yaml.parse(bad)
        assert len(rows) == 0
        assert any(e["field"] == "ticker" for e in errors)
