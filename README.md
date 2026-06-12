# polymarket-history

The full point-in-time history of Polymarket prediction markets, published as
monthly-partitioned Parquet files.

**~24,300 markets · ~851k daily bars (Nov 2022 – present) · ~7,800 resolutions (back to 2020)**

Data sourced from Polymarket via [Pancake](https://usepancake.com) — the backtesting
platform where Claude builds strategies and Pancake verifies them.

> **License:** CC BY 4.0 — attribution required.
> See [LICENSE](LICENSE) and the license block in every `_manifest.json`.

---

## What is this?

A clean, research-ready export of Polymarket prediction market data, normalized to
Pancake's public data schema (see [pipeline/schema-notes.md](#schema-and-point-in-time-model)).

Five tables are included:

| Table | Description | Rows | Partitioned by |
|---|---|---|---|
| `markets` | Full market catalog (titles, categories, resolution rules, outcome legs) | ~24,300 | `created_at` month |
| `outcomes` | All outcome instruments (Yes/No legs, multi-outcome) | ~15,800 | market `created_at` month |
| `candles_1d` | Daily OHLCV bars | ~851k | `observed_at` month |
| `resolutions` | Resolution events with winning outcome | ~7,800 | `resolved_at` month |
| `trades` | Synthetic last-price tape (see caveat below) | ~133k | `observed_at` month |

**Not included (coming):** `quotes` / `book_l2` — license review pending.

### Trades caveat

All trade records in this dataset are **synthetic last-price observations**, not
genuine exchange executions. Polymarket's CLOB API does not expose individual trade
events. Instead, Pancake's live normalizer detects price changes via the Gamma API
and synthesizes a trade record for each change (ADR-0027, rule 106). Every trade row
carries `derived: true` and `derivation_method: "polymarket_last_trade_price_change_detection"`.

Consumers who need only genuine exchange data should filter `derived == false` — this
will return an empty set for the trades table (no non-synthetic trades have been ingested yet).

Two further notes for reproducibility:

- **Deduplication.** The production source contained 565 duplicate synthetic rows
  (the same mark event inserted more than once, an upstream idempotency artifact).
  This release keeps exactly one row per `external_trade_id` — the earliest by
  `received_at` — so the table has 132,868 rows rather than the raw 133,433.
- **Cutoff.** This snapshot's trades end at the export time on 2026-06-11 (UTC).
  Rows arriving after that moment appear in the next release, never retroactively
  in this one (releases are immutable — cite the `snapshot_id`).

---

## Quickstart

### DuckDB (recommended — no Python setup)

```sql
-- Install DuckDB: https://duckdb.org/docs/installation/
-- Query candles directly from local files:

SELECT
    strftime(to_timestamp(observed_at), '%Y-%m') AS month,
    COUNT(*) AS bars,
    COUNT(DISTINCT market_id) AS markets
FROM read_parquet('datasets/v1/polymarket/candles_1d/**/*.parquet')
GROUP BY 1
ORDER BY 1;
```

### Python (pandas + DuckDB)

```bash
pip install duckdb pandas pyarrow
```

```python
import duckdb
import pandas as pd

con = duckdb.connect(':memory:')
con.execute("""
    CREATE VIEW candles AS
    SELECT * FROM read_parquet('datasets/v1/polymarket/candles_1d/**/*.parquet')
""")

# Top markets by volume
con.execute("""
    SELECT title, ROUND(total_volume_usd/1e6, 1) AS vol_m
    FROM read_parquet('datasets/v1/polymarket/markets/**/*.parquet')
    ORDER BY total_volume_usd DESC NULLS LAST
    LIMIT 10
""").df()
```

See `notebooks/` for worked examples.

---

## Schema and point-in-time model

### The four timestamps (P2 — contract §1)

Every fact record carries four timestamps. **You must gate on `released_at`, not `observed_at`.**

| Field | Semantics |
|---|---|
| `observed_at` | The calendar moment the value describes (bar close time, trade time). INT64 unix seconds. |
| `source_timestamp` | Timestamp reported by Polymarket for this value. INT64 unix seconds. |
| `received_at` | Wall-clock time when Pancake's ingest received the upstream response. INT64 unix seconds. |
| `released_at` | **Earliest time a consumer may use this value without lookahead bias.** Always `>= observed_at`. INT64 unix seconds. |

A backtest that gates on `observed_at <= t` instead of `released_at <= t` is subject
to lookahead bias. `released_at` is the correct gate.

### Field types in Parquet

- `observed_at`, `source_timestamp`, `received_at`, `released_at`: **INT64 unix seconds**
- `created_at` (markets): ISO 8601 string (this is an ingestion timestamp, not a fact timestamp)
- `resolves_at_planned` (markets): ISO 8601 string or null
- `external_ids`, `outcomes`, `license`, `resolution`: JSON strings
- All numeric prices/volumes: DOUBLE
- `derived`: BOOLEAN
- Low-cardinality strings (`venue`, `bar_period`, `currency`, `market_kind`, `status`): dictionary-encoded

### Markets schema

| Field | Type | Notes |
|---|---|---|
| `market_id` | STRING (UUID) | Pancake-canonical stable ID |
| `venue` | STRING | Always `"polymarket"` in this dataset |
| `title` | STRING | Market question text |
| `category` | STRING (nullable) | Provider-reported category |
| `market_kind` | STRING | `binary` \| `multi_outcome` \| `scalar` \| `range` |
| `status` | STRING | `open` \| `resolved` (no voided markets in prod) |
| `resolution_rule` | STRING | Full resolution rule text |
| `resolves_at_planned` | STRING (nullable) | ISO 8601 planned resolution datetime |
| `total_volume_usd` | DOUBLE (nullable) | Total traded volume in USD |
| `created_at` | STRING | ISO 8601 ingestion timestamp |
| `external_ids` | JSON STRING | `{"polymarket_condition_id": "0x..."}` |
| `outcomes` | JSON STRING | Array of `{instrument_id, label, payoff_on_resolve, sort_order}` |
| `derived` | BOOLEAN | Always `false` — markets are catalog records |
| `license` | JSON STRING | Attribution block |
| `resolution` | JSON STRING (nullable) | Resolution event if resolved |

### Candles schema

| Field | Type | Notes |
|---|---|---|
| `instrument_id` | STRING (UUID) | Outcome instrument ID |
| `market_id` | STRING (UUID) | Parent market ID (denormalized) |
| `outcome_label` | STRING | e.g. `"Yes"`, `"No"` |
| `venue` | STRING | `"polymarket"` |
| `external_ids` | JSON STRING | e.g. `{"polymarket_clob_token_id": "0x..."}` |
| `observed_at` | INT64 | Bar close time (unix seconds) — **gate on `released_at`** |
| `source_timestamp` | INT64 | Provider timestamp (unix seconds) |
| `received_at` | INT64 | Ingest wall-clock (unix seconds) |
| `released_at` | INT64 | Lookahead-bias gate (unix seconds) |
| `bar_period` | STRING | `"1d"` |
| `open/high/low/close` | DOUBLE | Price (0–1 USDC per share) |
| `volume` | DOUBLE (nullable) | Traded volume in bar period |
| `currency` | STRING | `"USDC"` |
| `derived` | BOOLEAN | `false` — sourced from Polymarket Gamma |
| `derivation_method` | STRING (nullable) | null when `derived=false` |

### Resolutions schema

| Field | Type | Notes |
|---|---|---|
| `resolution_id` | STRING (UUID) | Stable resolution event ID |
| `market_id` | STRING (UUID) | Market that resolved |
| `venue` | STRING | `"polymarket"` |
| `resolved_at` | INT64 | Settlement timestamp (unix seconds) |
| `released_at` | INT64 | When Pancake published this (unix seconds, >= resolved_at) |
| `received_at` | INT64 | Ingest wall-clock (unix seconds) |
| `winning_outcome_instrument_id` | STRING (UUID) | The winning outcome leg |
| `winning_outcome_label` | STRING (nullable) | e.g. `"Yes"` (denormalized) |
| `resolution_source` | STRING | e.g. `"polymarket_uma"` |
| `evidence_uri` | STRING (nullable) | Link to resolution evidence |

### Trades schema

| Field | Type | Notes |
|---|---|---|
| `trade_id` | STRING (UUID) | Pancake-stable trade ID |
| `instrument_id` | STRING (UUID) | Outcome instrument |
| `market_id` | STRING (UUID) | Parent market |
| `outcome_label` | STRING | Outcome leg label |
| `observed_at` | INT64 | Trade time (unix seconds) |
| `price` | DOUBLE | Trade price (0–1) |
| `size` | DOUBLE | Always `0` for synthetic rows (size unknown) |
| `side` | STRING (nullable) | Always `null` for synthetic rows |
| `external_trade_id` | STRING (nullable) | `"polymarket-mark-{conditionId}-{ts}"` for synthetic |
| `derived` | BOOLEAN | Always `true` in this dataset |
| `derivation_method` | STRING | `"polymarket_last_trade_price_change_detection"` |

---

## Dataset layout

```
datasets/v1/polymarket/
  markets/
    2026/05/
      part-0000.parquet
      _manifest.json
    2026/06/
      part-0000.parquet
      _manifest.json
  outcomes/
    2026/05/  ...
  candles_1d/
    2022/11/  ...  (earliest bars)
    ...
    2026/06/  ...  (latest bars)
  resolutions/
    2020/11/  ...  (earliest resolutions)
    ...
    2026/06/
  trades/
    2026/04/  ...  (live normalizer started 2026-04-21)
    2026/05/
    2026/06/
```

Each partition directory contains:
- `part-0000.parquet` — the data (Parquet v2, Snappy compressed)
- `_manifest.json` — sha256, row count, size, license block, and partition `snapshot_id`

---

## _manifest.json schema

```json
{
  "snapshot_id": "<sha256-hex-64>",
  "schema_version": 1,
  "created_at": "2026-06-12T00:00:00.000Z",
  "venue": "polymarket",
  "table": "candles_1d",
  "period": "2026-05",
  "files": [
    {
      "path": "part-0000.parquet",
      "sha256": "<hex-64>",
      "row_count": 6644,
      "size_bytes": 409600
    }
  ],
  "total_row_count": 6644,
  "license": {
    "usage_rights": "attribution_required",
    "redistribution_allowed": "attribution_only",
    "attribution_required": true,
    "attribution_text": "Data sourced from Polymarket via Pancake (usepancake.com)",
    "commercial_use_allowed": false
  }
}
```

The release-level `snapshot_id` (covering all tables) is printed by `pipeline/manifest.py`
and is the SHA-256 of the sorted concatenation of all per-file sha256 strings.

---

## Reproducing this dataset

### Prerequisites

- Node.js >= 20
- Python >= 3.11
- Pancake production Supabase credentials in `../../pancake-production/.env.local`

### Pipeline steps

```bash
# 1. Install Python dependencies
python -m venv pipeline/.venv
source pipeline/.venv/bin/activate  # or: pipeline\.venv\Scripts\activate (Windows)
pip install -r pipeline/requirements.txt

# 2. Export from prod to JSONL (resumable — safe to re-run)
node pipeline/export.mjs                   # all tables
node pipeline/export.mjs --table=candles_1d  # single table
node pipeline/export.mjs --resume           # skip already-done months

# 3. Convert JSONL to Parquet
python pipeline/to_parquet.py

# 4. Generate manifests
python pipeline/manifest.py

# 5. Run notebooks (optional verification)
cd notebooks
jupyter nbconvert --to notebook --execute 01-quickstart-duckdb.ipynb
jupyter nbconvert --to notebook --execute 02-pandas-backtest-naive.ipynb
```

### What's excluded from git

The `data/raw/` directory (JSONL intermediates) is excluded via `.gitignore`.
The `datasets/` directory is **included** in git for now; hosting split is
founder-gated and will be decided separately (GitHub LFS / HuggingFace / Kaggle).

---

## Zero-setup backtesting

This dataset is what powers [Pancake](https://usepancake.com): the backtesting platform
where you describe a strategy in plain English, Claude writes it, and Pancake runs a
rigorous point-in-time backtest and publishes a verifiable result — no data wrangling,
no environment setup, no code.

---

## License

Data: [CC BY 4.0](LICENSE) — attribution required.
Attribution text: **"Data sourced from Polymarket via Pancake (usepancake.com)"**

Code (pipeline scripts, notebooks): MIT — see [LICENSE](LICENSE).
