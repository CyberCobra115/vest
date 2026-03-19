import pytest

from app import create_app, db as _db


@pytest.fixture(scope="session")
def app():
    app = create_app("testing")
    with app.app_context():
        _db.create_all()
        yield app
        _db.drop_all()


@pytest.fixture(autouse=True)
def clean_db(app):
    """
    Wipe all rows after every test via DELETE (not rollback) so that tests
    committing within their own app_context() are also cleaned up.
    The session-scoped app keeps the schema alive across all tests.
    """
    with app.app_context():
        yield
        for table in reversed(_db.metadata.sorted_tables):
            _db.session.execute(table.delete())
        _db.session.commit()


@pytest.fixture
def client(app):
    return app.test_client()


# ── Sample file content ──────────────────────────────────────────────────────

FORMAT_1_CONTENT = """\
TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate
2025-01-15,ACC001,AAPL,100,185.50,BUY,2025-01-17
2025-01-15,ACC001,MSFT,50,420.25,BUY,2025-01-17
2025-01-15,ACC002,GOOGL,75,142.80,BUY,2025-01-17
2025-01-15,ACC003,TSLA,150,238.45,SELL,2025-01-17
"""

# Format 2 is a trade file (pipe-delimited) per the spec.
# Negative SHARES = SELL fill; positive = BUY fill.
FORMAT_2_CONTENT = """\
REPORT_DATE|ACCOUNT_ID|SECURITY_TICKER|SHARES|MARKET_VALUE|SOURCE_SYSTEM
20250115|ACC001|AAPL|100|18550.00|CUSTODIAN_A
20250115|ACC001|MSFT|50|21012.50|CUSTODIAN_A
20250115|ACC002|GOOGL|75|10710.00|CUSTODIAN_B
20250115|ACC003|TSLA|-150|35767.50|CUSTODIAN_A
"""

FORMAT_3_CONTENT = """\
report_date: "20250115"
positions:
  - account_id: "ACC001"
    ticker: "AAPL"
    shares: 100
    market_value: 18550.00
    custodian_ref: "CUST_A_12345"
  - account_id: "ACC001"
    ticker: "MSFT"
    shares: 50
    market_value: 21012.50
    custodian_ref: "CUST_A_12346"
  - account_id: "ACC002"
    ticker: "GOOGL"
    shares: 75
    market_value: 10710.00
    custodian_ref: "CUST_B_22345"
"""
