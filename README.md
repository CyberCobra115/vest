# Vest — Portfolio Data Clearinghouse

A Flask-based portfolio data reconciliation service. Ingests trade and position
data from multiple file formats, computes cost basis and market value, detects
compliance concentration violations, and surfaces trade/position discrepancies.

---

## Table of Contents

- [Setup](#setup)
- [Running the Server](#running-the-server)
- [Running Tests](#running-tests)
- [Project Structure](#project-structure)
- [API Reference](#api-reference)
- [Data Formats](#data-formats)
- [Sample Data](#sample-data)

---

## Setup

```bash
# 1. Create and activate a virtual environment
python -m venv .venv

# Windows
.venv\Scripts\activate

# macOS / Linux
source .venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt
```

---

## Running the Server

```bash
python run.py
```

The server starts at `http://127.0.0.1:5000`.
Database tables are created automatically on first startup — no migration
commands needed.

---

## Running Tests

```bash
pytest tests/ -v
```

49 tests across three files:

| File | Coverage |
|------|----------|
| `tests/test_ingestion.py` | Format detection, all three parsers, field validation |
| `tests/test_endpoints.py` | All four API endpoints, HTTP status codes, response shape |
| `tests/test_quality_checks.py` | Duplicate detection, upsert logic, settlement date checks |

---

## Project Structure

```
vest/
├── app/
│   ├── __init__.py              # Flask app factory
│   ├── config.py                # development / testing / production configs
│   ├── models.py                # SQLAlchemy models: Trade, Position
│   │
│   ├── ingestion/               # File parsing layer
│   │   ├── detector.py          # Auto-detect format from file header
│   │   ├── format1.py           # CSV comma-delimited trade parser
│   │   ├── format2.py           # Pipe-delimited position parser
│   │   ├── format3.py           # YAML bank position parser
│   │   └── utils.py             # Shared date normalization
│   │
│   ├── services/                # Business logic layer
│   │   ├── ingestion_service.py     # Orchestrate parse → quality check → persist
│   │   ├── positions_service.py     # Cost basis + market value computation
│   │   ├── compliance_service.py    # Concentration breach detection
│   │   └── reconciliation_service.py # Trade vs position discrepancy detection
│   │
│   └── routes/                  # HTTP layer (thin — delegates to services)
│       ├── ingest.py
│       ├── positions.py
│       ├── compliance.py
│       └── reconciliation.py
│
├── data/                        # Sample input files for manual testing
│   ├── trades_format1.csv               # Clean Format 1 trades (10 rows)
│   ├── trades_format1_with_errors.csv   # Format 1 with deliberate quality issues
│   ├── positions_format2.txt            # Clean Format 2 positions (10 rows)
│   ├── positions_format3.yaml           # Clean Format 3 positions (10 rows)
│   ├── positions_format3_with_breaks.yaml  # Positions with reconciliation breaks
│   └── README_DATA.md           # Test sequence + expected responses
│
├── tests/
│   ├── conftest.py              # Fixtures, app factory, sample file constants
│   ├── test_ingestion.py        # Parser + detector unit tests
│   ├── test_endpoints.py        # API integration tests
│   └── test_quality_checks.py  # Quality check + upsert behaviour tests
│
├── .env.example                 # Environment variable template
├── .gitignore
├── requirements.txt
├── run.py                       # Entry point
└── README.md
```

---

## API Reference

### `POST /ingest`

Accepts a file upload or raw text body. Auto-detects format, runs quality
checks, and persists valid rows. Returns a data quality report.

**Request**

```bash
# Raw body
curl -X POST http://localhost:5000/ingest \
  --data-binary @data/trades_format1.csv \
  -H "Content-Type: text/plain"

# File upload (multipart)
curl -X POST http://localhost:5000/ingest \
  -F "file=@data/trades_format1.csv"

# PowerShell (Windows)
$content = Get-Content "data\trades_format1.csv" -Raw
Invoke-RestMethod -Uri "http://localhost:5000/ingest" -Method POST -Body $content -ContentType "text/plain"
```

**Response**

```json
{
  "format_detected": "FORMAT_1",
  "total_rows": 10,
  "rows_accepted": 10,
  "rows_rejected": 0,
  "rows_skipped_duplicate": 0,
  "rows_upserted": 0,
  "errors": [],
  "warnings": []
}
```

| HTTP Status | Meaning |
|-------------|---------|
| 200 | All rows accepted |
| 207 | Partial success — some rows rejected |
| 400 | No body, or unrecognized file format |

**Quality checks applied:**

- Missing or empty required fields
- Invalid `TradeType` (must be `BUY` or `SELL`)
- Non-positive price
- Non-integer quantity
- `SettlementDate` before `TradeDate`
- Intra-file duplicate rows
- Cross-file duplicate trades (already in DB — skipped, not re-inserted)
- Position upsert: existing record with changed values is updated in place
- Position unchanged: existing record with identical values is skipped with warning

---

### `GET /positions`

```
GET /positions?account=ACC001&date=2025-01-15
```

Returns all open positions for an account as of the given date.
Cost basis is computed from trade history (average cost method).
Market value is pulled from the most recent position record on or before the date.

**Response**

```json
{
  "account_id": "ACC001",
  "as_of": "2025-01-15",
  "positions": [
    {
      "ticker": "AAPL",
      "net_quantity": 100,
      "avg_cost_basis": "185.5000",
      "total_cost_basis": "18550.0000",
      "market_value": "18550.00",
      "unrealized_pnl": "0.00"
    }
  ]
}
```

---

### `GET /compliance/concentration`

```
GET /compliance/concentration?date=2025-01-15
```

Returns all accounts where any single equity exceeds 20% of total account
market value. Results sorted by worst offender first.

**Response**

```json
{
  "as_of": "2025-01-15",
  "threshold": "20%",
  "breach_count": 1,
  "breaches": [
    {
      "account_id": "ACC004",
      "ticker": "AAPL",
      "position_value": "92750.00",
      "total_account_value": "218825.00",
      "concentration_pct": "42.38%"
    }
  ]
}
```

---

### `GET /reconciliation`

```
GET /reconciliation?date=2025-01-15
```

Compares trade-derived net quantity against reported shares for every
account + ticker pair on the given date. Returns all breaks (delta != 0).

**Response**

```json
{
  "as_of": "2025-01-15",
  "break_count": 3,
  "breaks": [
    {
      "account_id": "ACC001",
      "ticker": "AAPL",
      "trade_derived_qty": 100,
      "reported_shares": 90,
      "delta": -10,
      "reported_market_value": "16695.00"
    }
  ]
}
```

---

## Data Formats

### Format 1 — CSV comma-delimited trades

```
TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate
2025-01-15,ACC001,AAPL,100,185.50,BUY,2025-01-17
```

Stored in: `trades` table.

### Format 2 — Pipe-delimited positions

```
REPORT_DATE|ACCOUNT_ID|SECURITY_TICKER|SHARES|MARKET_VALUE|SOURCE_SYSTEM
20250115|ACC001|AAPL|100|18550.00|CUSTODIAN_A
```

Dates in `YYYYMMDD` format. Negative `SHARES` = short position.
Stored in: `positions` table.

### Format 3 — YAML bank position file

```yaml
report_date: "20250115"
positions:
  - account_id: "ACC001"
    ticker: "AAPL"
    shares: 100
    market_value: 18550.00
    custodian_ref: "CUST_A_12345"
```

Stored in: `positions` table.

---

## Sample Data

See [`data/README_DATA.md`](data/README_DATA.md) for the full recommended test
sequence with expected responses for each file.
