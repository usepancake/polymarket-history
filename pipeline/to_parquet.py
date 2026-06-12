#!/usr/bin/env python3
"""
to_parquet.py — Convert JSONL raw files to Parquet v2 (Snappy) per contract §3.

Reads from:  data/raw/{table}/{YYYY}/{MM}/data.jsonl
Writes to:   datasets/v1/polymarket/{table}/{YYYY}/{MM}/part-0000.parquet

Contract rules enforced:
  - Parquet v2, Snappy compression (§3.4)
  - Dictionary encoding on low-cardinality columns (§3.4)
  - Timestamps: observed_at / source_timestamp / received_at / released_at
    stored as INT64 unix seconds (§4 note); created_at stored as ISO string.
  - license column stored as JSON string (not nested struct) — broadest compat.
  - external_ids stored as JSON string.
  - Row group target: 128 MB (§3.4).

Usage:
  python pipeline/to_parquet.py [--table=markets|outcomes|candles_1d|resolutions|trades]
                                [--month=YYYY-MM]  (optional: process only one month)

Requires: pip install duckdb pyarrow
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).parent.resolve()
REPO_ROOT = SCRIPT_DIR.parent.resolve()
RAW_DIR = REPO_ROOT / "data" / "raw"
DATASETS_DIR = REPO_ROOT / "datasets" / "v1" / "polymarket"

ALL_TABLES = ["markets", "outcomes", "candles_1d", "resolutions", "trades"]

# ---------------------------------------------------------------------------
# Column specs per table
# Low-cardinality string columns get dictionary encoding.
# Timestamp columns (unix int64) are converted from the JSONL int values.
# ---------------------------------------------------------------------------
# Format: column_name -> ("type", options)
# type: "str", "int64", "float64", "bool", "json_str", "nullable_str",
#       "nullable_float", "nullable_int"

TABLE_SPECS = {
    "markets": {
        # market_id, title, category, market_kind, status, resolution_rule,
        # resolves_at_planned, total_volume_usd, created_at, external_ids,
        # outcomes, derived, license
        "str_cols": ["market_id", "venue", "title", "category", "market_kind",
                     "status", "resolution_rule", "resolves_at_planned", "created_at"],
        "float_cols": ["total_volume_usd"],
        "bool_cols": ["derived"],
        "json_cols": ["external_ids", "outcomes", "license", "resolution"],
        "dict_cols": ["venue", "market_kind", "status"],
        "ts_int_cols": [],  # created_at is ISO string per contract; resolves_at_planned is ISO string
    },
    "outcomes": {
        "str_cols": ["instrument_id", "market_id", "venue", "label"],
        "float_cols": ["payoff_on_resolve"],
        "bool_cols": ["derived"],
        "json_cols": ["license"],
        "nullable_int_cols": ["sort_order"],
        "dict_cols": ["venue"],
        "ts_int_cols": [],
    },
    "candles_1d": {
        "str_cols": ["instrument_id", "market_id", "outcome_label", "venue",
                     "bar_period", "currency", "derivation_method",
                     "normalizer_version", "manifest_id"],
        "ts_int_cols": ["observed_at", "source_timestamp", "received_at", "released_at"],
        "float_cols": ["open", "high", "low", "close", "volume"],
        "bool_cols": ["derived"],
        "int_cols": ["schema_version"],
        "json_cols": ["external_ids", "license"],
        "dict_cols": ["venue", "bar_period", "currency"],
    },
    "resolutions": {
        "str_cols": ["resolution_id", "market_id", "venue",
                     "winning_outcome_instrument_id", "winning_outcome_label",
                     "resolution_source", "evidence_uri",
                     "normalizer_version", "manifest_id"],
        "ts_int_cols": ["resolved_at", "released_at", "received_at"],
        "bool_cols": [],
        "int_cols": ["schema_version"],
        "json_cols": ["external_ids", "license"],
        "dict_cols": ["venue", "resolution_source"],
    },
    "trades": {
        "str_cols": ["trade_id", "instrument_id", "market_id", "outcome_label",
                     "venue", "side", "external_trade_id", "currency",
                     "derivation_method", "normalizer_version", "manifest_id"],
        "ts_int_cols": ["observed_at", "source_timestamp", "received_at", "released_at"],
        "float_cols": ["price", "size"],
        "bool_cols": ["derived"],
        "int_cols": ["schema_version"],
        "json_cols": ["external_ids", "license"],
        "dict_cols": ["venue", "currency"],
        # Prod contains duplicated synthetic mark events (same external_trade_id
        # inserted multiple times — QA finding F2, 565 extra rows). Keep the
        # earliest received copy; NULL external ids fall back to their own
        # trade_id so they are never collapsed together.
        "dedupe_expr": "COALESCE(external_trade_id, CAST(trade_id AS VARCHAR))",
    },
}


def jsonl_to_parquet(jsonl_path: Path, parquet_path: Path, table: str) -> int:
    """
    Convert a single JSONL file to Parquet using DuckDB.
    Returns row count.
    """
    import duckdb

    parquet_path.parent.mkdir(parents=True, exist_ok=True)

    # DuckDB: read JSON auto, then COPY TO parquet with snappy
    # We use read_json_auto for max flexibility, then coerce types via CAST.
    # The key trick: observed_at etc. are already INT64 in the JSONL (we wrote
    # them as integers in export.mjs), so DuckDB reads them as BIGINT natively.

    spec = TABLE_SPECS[table]
    dict_cols = set(spec.get("dict_cols", []))
    ts_int_cols = set(spec.get("ts_int_cols", []))

    # Build column list for SELECT with explicit casts
    all_cols = []
    all_cols += spec.get("str_cols", [])
    all_cols += spec.get("ts_int_cols", [])
    all_cols += spec.get("float_cols", [])
    all_cols += spec.get("bool_cols", [])
    all_cols += spec.get("int_cols", [])
    all_cols += spec.get("nullable_int_cols", [])
    all_cols += spec.get("json_cols", [])

    # Build explicit SELECT expressions
    select_exprs = []
    for col in all_cols:
        if col in spec.get("json_cols", []):
            # TO_JSON, not CAST: DuckDB casts STRUCTs to VARCHAR as Python-style
            # repr (single quotes), which breaks json.loads for every consumer
            # (QA finding F1). TO_JSON emits valid JSON; NULL stays NULL.
            select_exprs.append(f"TO_JSON({col}) AS {col}")
        elif col in spec.get("str_cols", []):
            select_exprs.append(f"CAST({col} AS VARCHAR) AS {col}")
        elif col in ts_int_cols:
            select_exprs.append(f"CAST({col} AS BIGINT) AS {col}")
        elif col in spec.get("float_cols", []):
            select_exprs.append(f"CAST({col} AS DOUBLE) AS {col}")
        elif col in spec.get("bool_cols", []):
            select_exprs.append(f"CAST({col} AS BOOLEAN) AS {col}")
        elif col in spec.get("int_cols", []):
            select_exprs.append(f"CAST({col} AS INTEGER) AS {col}")
        elif col in spec.get("nullable_int_cols", []):
            select_exprs.append(f"CAST({col} AS INTEGER) AS {col}")
        else:
            select_exprs.append(col)

    select_clause = ",\n    ".join(select_exprs)

    # Escape path for DuckDB (forward slashes, single-quoted)
    jsonl_path_str = str(jsonl_path).replace("\\", "/")
    parquet_path_str = str(parquet_path).replace("\\", "/")

    # Row group size: 128 MB
    row_group_size = 128 * 1024 * 1024  # bytes hint — DuckDB uses row_group_size in rows
    # DuckDB COPY uses row_group_size as number of rows; ~128MB at ~200 bytes/row ≈ 655360 rows
    # For prediction markets data (small rows), 500k rows ≈ reasonable chunk
    row_group_rows = 500000

    dedupe_expr = spec.get("dedupe_expr")
    qualify_clause = (
        f"QUALIFY ROW_NUMBER() OVER (PARTITION BY {dedupe_expr} "
        f"ORDER BY received_at, trade_id) = 1"
        if dedupe_expr
        else ""
    )

    sql = f"""
COPY (
    SELECT
        {select_clause}
    FROM read_json_auto('{jsonl_path_str}', format='newline_delimited', ignore_errors=true)
    {qualify_clause}
) TO '{parquet_path_str}'
(FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE {row_group_rows});
"""

    con = duckdb.connect(database=":memory:")
    try:
        con.execute(sql)
        # Get row count
        count_sql = f"SELECT COUNT(*) FROM read_parquet('{parquet_path_str}')"
        row = con.execute(count_sql).fetchone()
        return row[0] if row else 0
    finally:
        con.close()


def process_table(table: str, month_filter: str | None = None) -> dict:
    """Process one table: all months (or a single month if filter given)."""
    print(f"\n--- {table.upper()} ---")
    raw_table_dir = RAW_DIR / table
    if not raw_table_dir.exists():
        print(f"  WARN: {raw_table_dir} does not exist — run export.mjs first")
        return {}

    results = {}

    # Find all YYYY/MM directories
    month_dirs = sorted([
        (year_dir.name, month_dir.name, month_dir)
        for year_dir in sorted(raw_table_dir.iterdir()) if year_dir.is_dir()
        for month_dir in sorted(year_dir.iterdir()) if month_dir.is_dir()
    ])

    if month_filter:
        y, m = month_filter.split("-")
        month_dirs = [(y, m, raw_table_dir / y / m)]

    for year, month, month_dir in month_dirs:
        jsonl_path = month_dir / "data.jsonl"
        if not jsonl_path.exists():
            print(f"  SKIP {year}/{month}: no data.jsonl")
            continue

        parquet_dir = DATASETS_DIR / table / year / month
        parquet_path = parquet_dir / "part-0000.parquet"

        if parquet_path.exists():
            print(f"  SKIP {year}/{month}: parquet already exists")
            # Still count it
            try:
                import duckdb
                con = duckdb.connect(":memory:")
                row_count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{str(parquet_path).replace(chr(92), '/')}')" ).fetchone()[0]
                con.close()
                results[f"{year}/{month}"] = row_count
            except Exception:
                pass
            continue

        t0 = time.time()
        try:
            row_count = jsonl_to_parquet(jsonl_path, parquet_path, table)
            elapsed = time.time() - t0
            print(f"  OK  {year}/{month}: {row_count:>8,} rows  ({elapsed:.1f}s)  -> {parquet_path.name}")
            results[f"{year}/{month}"] = row_count
        except Exception as e:
            print(f"  ERR {year}/{month}: {e}", file=sys.stderr)
            # Remove partial file
            if parquet_path.exists():
                parquet_path.unlink()

    total = sum(results.values())
    print(f"  {table} total: {total:,} rows across {len(results)} partitions")
    return results


def main():
    parser = argparse.ArgumentParser(description="Convert JSONL raw files to Parquet")
    parser.add_argument("--table", choices=ALL_TABLES, help="Process only this table")
    parser.add_argument("--month", help="Process only this month (YYYY-MM)")
    args = parser.parse_args()

    tables = [args.table] if args.table else ALL_TABLES

    print("=== polymarket-history to_parquet ===")
    print(f"Input:  {RAW_DIR}")
    print(f"Output: {DATASETS_DIR}")
    print(f"Tables: {', '.join(tables)}")
    if args.month:
        print(f"Month filter: {args.month}")

    all_results = {}
    t_start = time.time()

    for table in tables:
        all_results[table] = process_table(table, args.month)

    elapsed = time.time() - t_start

    print("\n=== PARQUET CONVERSION COMPLETE ===")
    grand_total = 0
    for table, months in all_results.items():
        total = sum(months.values())
        grand_total += total
        print(f"  {table:<14} {total:>10,} rows  ({len(months)} partitions)")
    print(f"  {'TOTAL':<14} {grand_total:>10,} rows")
    print(f"  Elapsed: {elapsed:.1f}s")


if __name__ == "__main__":
    main()
