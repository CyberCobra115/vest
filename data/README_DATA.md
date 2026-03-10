# Sample Data Files

This directory contains sample input files for manual testing and demonstration.
All files are based on the same set of accounts and tickers so results are cross-comparable.

---

## Accounts & Tickers

| AccountID | Holdings |
|-----------|----------|
| ACC001    | AAPL, MSFT, GOOGL |
| ACC002    | GOOGL, AAPL, NVDA |
| ACC003    | TSLA (short), NVDA |
| ACC004    | AAPL, MSFT |

---

## Files

### `trades_format1.csv`
Clean Format 1 (comma-delimited) trade file. All 10 rows should be accepted.

```
POST /ingest  (body = file contents)
```

Expected response:
```json
{
  "format_detected": "FORMAT_1",
  "total_rows": 10,
  "rows_accepted": 10,
  "rows_rejected": 0,
  "rows_skipped_duplicate": 0,
  "warnings": [],
  "errors": []
}
```

---

### `trades_format1_with_errors.csv`
Format 1 file with intentional quality issues to exercise validation:

| Line | Issue |
|------|-------|
| 3    | Missing AccountID |
| 4    | Invalid TradeType "HOLD" (not BUY/SELL) |
| 5    | Negative price (-505.30) |
| 6    | SettlementDate (2025-01-13) before TradeDate (2025-01-15) |
| 7    | Exact duplicate of line 1 — caught as intra-file duplicate |
| 8    | Non-integer Quantity ("abc") |

Expected: 2 rows accepted, 5 rejected, 1 duplicate warning.

---

### `positions_format2.txt`
Clean Format 2 (pipe-delimited) position file. 10 rows, all accepted.
Note ACC003 TSLA has negative shares (-150) representing a short position.

```
POST /ingest  (body = file contents)
```

Expected response:
```json
{
  "format_detected": "FORMAT_2",
  "total_rows": 10,
  "rows_accepted": 10,
  "rows_rejected": 0
}
```

---

### `positions_format3.yaml`
Clean Format 3 (YAML) bank position file. Same data as `positions_format2.txt`
but from a different custodian system.

Ingest this **after** `positions_format2.txt` to observe upsert behavior:
- Different `custodian_ref` values → new rows inserted (not duplicates)

Ingest this **twice** to observe duplicate handling:
- Second ingest: all 10 rows detected as unchanged, skipped with warnings.

---

### `positions_format3_with_breaks.yaml`
Position file with deliberate discrepancies vs `trades_format1.csv`:

| Account | Ticker | Trade-Derived Qty | Reported Shares | Delta |
|---------|--------|-------------------|-----------------|-------|
| ACC001  | AAPL   | 100               | 90              | -10   |
| ACC002  | AAPL   | 200               | 250             | +50   |
| ACC002  | NVDA   | 120               | 100             | -20   |

Use to validate the reconciliation endpoint:

```
GET /reconciliation?date=2025-01-15
```

---

## Recommended Test Sequence

### 1. Load trades
```bash
curl -X POST http://localhost:5000/ingest \
  --data-binary @data/trades_format1.csv \
  -H "Content-Type: text/plain"
```

### 2. Load clean positions
```bash
curl -X POST http://localhost:5000/ingest \
  --data-binary @data/positions_format3.yaml \
  -H "Content-Type: text/plain"
```

### 3. Check positions with cost basis
```bash
curl "http://localhost:5000/positions?account=ACC001&date=2025-01-15"
```

### 4. Check compliance concentration
```bash
curl "http://localhost:5000/compliance/concentration?date=2025-01-15"
```

ACC004 holds AAPL at market value 92750 out of total ~218825 = ~42% → breach expected.

### 5. Load break positions and reconcile
```bash
curl -X POST http://localhost:5000/ingest \
  --data-binary @data/positions_format3_with_breaks.yaml \
  -H "Content-Type: text/plain"

curl "http://localhost:5000/reconciliation?date=2025-01-15"
```

### 6. Test quality checks
```bash
curl -X POST http://localhost:5000/ingest \
  --data-binary @data/trades_format1_with_errors.csv \
  -H "Content-Type: text/plain"
```

### 7. Test duplicate detection (re-ingest same file)
```bash
curl -X POST http://localhost:5000/ingest \
  --data-binary @data/trades_format1.csv \
  -H "Content-Type: text/plain"
# All 10 rows skipped as duplicates, rows_skipped_duplicate: 10
```
