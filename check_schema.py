"""check_schema.py

Helper to compare the destination DB table columns with a sample record from the API
and produce a suggested column mapping (table_col -> source_col).

Usage:
  python check_schema.py --table air_quality --out suggested_mapping.json

This script imports the engine creation routine from `main.py` so it uses the same
connection logic (and driver fallbacks). It will not alter any tables.
"""

from __future__ import annotations

import argparse
import json
import logging
from typing import Dict

import requests
from sqlalchemy import inspect

# import helpers from main.py (engine creation & API_URL)
from main import API_URL, get_engine_from_env

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def normalize(name: str) -> str:
    import unicodedata

    if name is None:
        return ""
    nf = unicodedata.normalize("NFKD", str(name))
    no_acc = "".join(ch for ch in nf if not unicodedata.combining(ch))
    cleaned = no_acc.lower().replace(" ", "_").replace("-", "_")
    return cleaned


def fetch_sample_record(api_url: str):
    logger.info("Fetching sample record from API: %s", api_url)
    resp = requests.get(api_url, timeout=30)
    resp.raise_for_status()
    js = resp.json()
    records = js.get("result", {}).get("records", [])
    if not records:
        return None
    return records[0]


def main():
    parser = argparse.ArgumentParser(
        description="Check DB table schema vs API sample and suggest mapping"
    )
    parser.add_argument("--table", default="air_quality", help="Destination table name")
    parser.add_argument(
        "--out", default=None, help="Write suggested mapping JSON to this file"
    )
    parser.add_argument("--api", default=API_URL, help="API URL to sample from")
    args = parser.parse_args()

    sample = fetch_sample_record(args.api)
    if sample is None:
        logger.error("No sample records found from API; aborting")
        return

    engine = get_engine_from_env()
    inspector = inspect(engine)
    if not inspector.has_table(args.table):
        logger.error("Table %s does not exist in database", args.table)
        return

    cols = [c["name"] for c in inspector.get_columns(args.table)]
    logger.info("DB columns for table %s: %s", args.table, cols)

    src_keys = list(sample.keys())
    logger.info("Sample API keys: %s", src_keys)

    # Build mapping suggestions
    normalized_src = {normalize(k): k for k in src_keys}
    mapping: Dict[str, str] = {}

    for tcol in cols:
        # exact
        if tcol in sample:
            mapping[tcol] = tcol
            continue
        # case-insensitive
        found = next((k for k in src_keys if k.lower() == tcol.lower()), None)
        if found:
            mapping[tcol] = found
            continue
        # normalized
        norm = normalize(tcol)
        if norm in normalized_src:
            mapping[tcol] = normalized_src[norm]
            continue
        # heuristics for lat/lon
        if tcol.lower() in ("latitude", "lat", "y"):
            for cand in ("latitude", "lat", "y"):
                if cand in sample:
                    mapping[tcol] = cand
                    break
        if tcol.lower() in ("longitude", "lon", "lng", "x"):
            for cand in ("longitude", "long", "lng", "x"):
                if cand in sample:
                    mapping[tcol] = cand
                    break

    logger.info(
        "Suggested mapping (table_col -> source_key):\n%s",
        json.dumps(mapping, indent=2, ensure_ascii=False),
    )

    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(mapping, f, indent=2, ensure_ascii=False)
        logger.info("Wrote suggested mapping to %s", args.out)


if __name__ == "__main__":
    main()
