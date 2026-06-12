#!/usr/bin/env python3
"""
manifest.py — Generate _manifest.json per release partition.

For each datasets/v1/polymarket/{table}/{YYYY}/{MM}/ directory that contains
.parquet files, writes a _manifest.json conforming to contract §3.2.

The snapshot_id for the RELEASE-LEVEL manifest (covering all tables) is:
  sha256(concatenation of all per-file sha256 hex strings, sorted by path)

Usage:
  python pipeline/manifest.py [--dry-run]
  python pipeline/manifest.py --table=candles_1d --month=2024/10

Also prints the release-level snapshot_id to stdout at the end.
"""

import argparse
import hashlib
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).parent.resolve()
REPO_ROOT = SCRIPT_DIR.parent.resolve()
DATASETS_DIR = REPO_ROOT / "datasets" / "v1" / "polymarket"

ALL_TABLES = ["markets", "outcomes", "candles_1d", "resolutions", "trades"]

LICENSE = {
    "usage_rights": "attribution_required",
    "redistribution_allowed": "attribution_only",
    "attribution_required": True,
    "attribution_text": "Data sourced from Polymarket via Pancake (usepancake.com)",
    "commercial_use_allowed": False,
}


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def write_partition_manifest(
    table: str,
    year: str,
    month: str,
    parquet_files: list[Path],
    dry_run: bool = False,
) -> dict | None:
    """Write _manifest.json for one partition. Returns the manifest dict."""
    if not parquet_files:
        return None

    period = f"{year}-{month}"
    created_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")

    files_info = []
    for pf in sorted(parquet_files):
        sha = sha256_file(pf)
        size = pf.stat().st_size
        # Row count via duckdb
        try:
            import duckdb
            con = duckdb.connect(":memory:")
            row_count = con.execute(
                f"SELECT COUNT(*) FROM read_parquet('{str(pf).replace(chr(92), '/')}')"
            ).fetchone()[0]
            con.close()
        except Exception as e:
            print(f"  WARN: could not count rows in {pf.name}: {e}", file=sys.stderr)
            row_count = -1

        files_info.append({
            "path": pf.name,
            "sha256": sha,
            "row_count": row_count,
            "size_bytes": size,
        })

    total_row_count = sum(f["row_count"] for f in files_info)

    # Partition snapshot_id: sha256 of concatenated file hashes
    concat_hashes = "".join(f["sha256"] for f in files_info)
    snapshot_id = hashlib.sha256(concat_hashes.encode()).hexdigest()

    manifest = {
        "snapshot_id": snapshot_id,
        "schema_version": 1,
        "created_at": created_at,
        "venue": "polymarket",
        "table": table,
        "period": period,
        "files": files_info,
        "total_row_count": total_row_count,
        "license": LICENSE,
    }

    manifest_path = parquet_files[0].parent / "_manifest.json"
    if not dry_run:
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2)
        print(f"  OK  {table}/{year}/{month}: {total_row_count:>8,} rows  snapshot={snapshot_id[:12]}...")
    else:
        print(f"  DRY {table}/{year}/{month}: {total_row_count:>8,} rows  snapshot={snapshot_id[:12]}...")

    return manifest


def main():
    parser = argparse.ArgumentParser(description="Generate _manifest.json files for parquet partitions")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be done without writing")
    parser.add_argument("--table", choices=ALL_TABLES, help="Process only this table")
    parser.add_argument("--month", help="Process only YYYY/MM (e.g. 2024/10)")
    args = parser.parse_args()

    tables = [args.table] if args.table else ALL_TABLES

    print("=== polymarket-history manifest generator ===")
    print(f"Datasets dir: {DATASETS_DIR}")
    print(f"Dry run: {args.dry_run}")

    all_file_hashes = []  # for release-level snapshot_id
    grand_total_rows = 0
    partition_count = 0

    for table in tables:
        print(f"\n--- {table.upper()} ---")
        table_dir = DATASETS_DIR / table
        if not table_dir.exists():
            print(f"  SKIP: {table_dir} does not exist")
            continue

        for year_dir in sorted(table_dir.iterdir()):
            if not year_dir.is_dir():
                continue
            for month_dir in sorted(year_dir.iterdir()):
                if not month_dir.is_dir():
                    continue

                year = year_dir.name
                month = month_dir.name

                if args.month and f"{year}/{month}" != args.month:
                    continue

                parquet_files = sorted(month_dir.glob("*.parquet"))
                if not parquet_files:
                    continue

                manifest = write_partition_manifest(
                    table=table,
                    year=year,
                    month=month,
                    parquet_files=parquet_files,
                    dry_run=args.dry_run,
                )
                if manifest:
                    for f in manifest["files"]:
                        all_file_hashes.append(f["sha256"])
                    grand_total_rows += manifest["total_row_count"]
                    partition_count += 1

    # Release-level snapshot_id
    if all_file_hashes:
        concat = "".join(sorted(all_file_hashes))
        release_snapshot_id = hashlib.sha256(concat.encode()).hexdigest()
    else:
        release_snapshot_id = "(no files)"

    print("\n=== MANIFEST GENERATION COMPLETE ===")
    print(f"  Partitions processed: {partition_count}")
    print(f"  Total rows:           {grand_total_rows:,}")
    print(f"  Release snapshot_id:  {release_snapshot_id}")
    print("")
    print("  NOTE: The release snapshot_id is computed from the sorted concatenation")
    print("  of all per-file sha256 hashes. It is stable if the data does not change.")

    return release_snapshot_id


if __name__ == "__main__":
    main()
