"""
load_data.py
Main script to fetch all World Cup data and load it into Snowflake.

Usage:
    python load_data.py              # fetch from web + load into Snowflake
    python load_data.py --no-cache   # force re-download even if cached
    python load_data.py --dry-run    # fetch only, skip Snowflake load
"""

import argparse
import logging
import sys
from pathlib import Path

# Make sure src/ is importable
sys.path.insert(0, str(Path(__file__).parent / "src"))

from data_fetcher import fetch_all_data
from snowflake_connector import load_all_tables

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch FIFA World Cup data and load into Snowflake."
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Re-download data even if local CSV cache exists.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch data but skip loading into Snowflake.",
    )
    parser.add_argument(
        "--no-overwrite",
        action="store_true",
        help="Append to existing Snowflake tables instead of replacing them.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    # ── Step 1: Fetch data ─────────────────────────────────────
    logger.info("━━━━  Step 1 of 2: Fetching data  ━━━━")
    data = fetch_all_data(cache=not args.no_cache)

    if not data:
        logger.error("No data was fetched. Check your internet connection.")
        sys.exit(1)

    logger.info("Fetched %d tables total.", len(data))
    for name, df in data.items():
        logger.info("  %-35s %d rows  %d cols", name, len(df), len(df.columns))

    if args.dry_run:
        logger.info("--dry-run flag set: skipping Snowflake load.")
        return

    # ── Step 2: Load into Snowflake ───────────────────────────
    logger.info("━━━━  Step 2 of 2: Loading into Snowflake  ━━━━")
    results = load_all_tables(data, overwrite=not args.no_overwrite)

    # Summary
    ok  = [t for t, s in results.items() if s]
    bad = [t for t, s in results.items() if not s]

    logger.info("━━━━  Done  ━━━━")
    logger.info("  Succeeded: %d tables", len(ok))
    if bad:
        logger.warning("  Failed:    %d tables — %s", len(bad), ", ".join(bad))

    if bad:
        sys.exit(1)


if __name__ == "__main__":
    main()
