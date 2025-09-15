"""air_quality_sync pipeline

This script fetches data from the Montreal open data API, loads it into a Polars
DataFrame and upserts it into a TiDB/MySQL-compatible database.

Features:
- Modular functions: fetch, transform, ensure table, upsert
- Uses environment variable TIDB for the SQLAlchemy connection string
- Logging, retries, and a scheduler option to run daily at 06:00
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import time
import unicodedata
from typing import Any, Dict, List

import polars as pl
import requests
import sqlalchemy as sa
# Scheduler for daily run
from apscheduler.schedulers.blocking import BlockingScheduler
from sqlalchemy import Column, MetaData, Table
from sqlalchemy.dialects.mysql import insert as mysql_insert

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


API_URL = "https://donnees.montreal.ca/api/3/action/datastore_search?resource_id=ccd5a4b7-cbca-4f5f-a746-ad8c576af374"
DEFAULT_TABLE = "air_quality_joined"


def fetch_json(url: str, retries: int = 3, backoff: float = 1.0) -> Dict[str, Any]:
    """Fetch JSON from an HTTP endpoint with simple retry/backoff.

    Returns the parsed JSON as a dict.
    """
    for attempt in range(1, retries + 1):
        try:
            logger.debug("Fetching URL: %s (attempt %d)", url, attempt)
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            logger.warning("Fetch attempt %d failed: %s", attempt, exc)
            if attempt == retries:
                logger.exception("All retries failed for fetching URL: %s", url)
                raise
            time.sleep(backoff * attempt)


def records_to_polars(records: List[Dict[str, Any]]) -> pl.DataFrame:
    """Convert list-of-dicts JSON records to a Polars DataFrame."""
    if not records:
        # Return empty dataframe
        return pl.DataFrame()
    df = pl.DataFrame(records)
    logger.info("Loaded %d records into Polars DataFrame", df.height)
    return df


def _map_polars_dtype_to_sql(col_name: str, dtype) -> Column:
    """Return a SQLAlchemy Column object for a given polars dtype.

    This uses simple heuristics and defaults to TEXT for unknowns.
    """
    # Use name so Column can be constructed by caller
    if dtype == pl.Int64 or dtype == pl.Int32:
        return Column(col_name, sa.BigInteger)
    if dtype == pl.Float64 or dtype == pl.Float32:
        return Column(col_name, sa.Float)
    if dtype == pl.Boolean:
        return Column(col_name, sa.Boolean)
    # Dates and datetimes
    if dtype == pl.Date:
        return Column(col_name, sa.Date)
    if dtype == pl.Datetime:
        return Column(col_name, sa.DateTime)
    # Default: strings
    return Column(col_name, sa.Text)


def ensure_table(engine: sa.engine.Engine, table_name: str, df: pl.DataFrame) -> Table:
    """Ensure the destination table exists. Create it if missing using df schema.

    If df is empty and table exists, just reflect and return it.
    """
    metadata = MetaData()
    metadata.bind = engine

    inspector = sa.inspect(engine)
    if inspector.has_table(table_name):
        metadata.reflect(only=[table_name], bind=engine)
        logger.info("Table %s exists - reflected metadata.", table_name)
        return metadata.tables[table_name]

    # Build columns from dataframe schema
    columns = []
    # If df is empty, create a minimal table with an ingest_id primary key
    if df.is_empty():
        logger.info("DataFrame empty — creating minimal table %s", table_name)
        columns.append(
            Column("ingest_id", sa.BigInteger, primary_key=True, autoincrement=True)
        )
    else:
        for col_name, dtype in df.schema.items():
            col = _map_polars_dtype_to_sql(col_name, dtype)
            # Keep polars naming as-is; set _id or id as primary key if present
            if col_name in ("_id", "id", "ID"):
                col = Column(col_name, col.type, primary_key=True)
            columns.append(col)

        # If no primary key detected, add an ingest_id PK
        if not any(c.primary_key for c in columns):
            columns.insert(
                0,
                Column(
                    "ingest_id", sa.BigInteger, primary_key=True, autoincrement=True
                ),
            )

    table = Table(table_name, metadata, *columns)
    metadata.create_all(engine)
    logger.info(
        "Created table %s with columns: %s", table_name, [c.name for c in table.columns]
    )
    return table


def upsert_polars_df(engine: sa.engine.Engine, table: Table, df: pl.DataFrame) -> int:
    """Upsert a Polars DataFrame into the given table.

    Returns the number of affected rows (best-effort using result.rowcount).
    """
    if df.is_empty():
        logger.info("No rows to insert for table %s", table.name)
        return 0

    source_rows = df.to_dicts()

    # Table column names (as stored in DB)
    table_cols = [c.name for c in table.columns]
    table_cols_set = set(table_cols)

    # Source column names
    src_cols = set()
    for r in source_rows:
        src_cols.update(r.keys())

    def normalize(name: str) -> str:
        """Normalize a column/key name into a predictable snake_case token.

        - strips accents
        - converts camelCase/PascalCase to snake_case
        - replaces spaces and hyphens with underscores
        - removes other non-alphanumeric characters
        - lowercases the result
        """
        if name is None:
            return ""
        s = str(name)
        # remove accents
        nf = unicodedata.normalize("NFKD", s)
        no_acc = "".join(ch for ch in nf if not unicodedata.combining(ch))
        # convert camelCase/PascalCase to snake_case (aB -> a_b)
        s1 = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", no_acc)
        # replace spaces and hyphens with underscores
        s1 = s1.replace(" ", "_").replace("-", "_")
        # remove any characters that are not alphanumeric or underscore
        s1 = re.sub(r"[^0-9A-Za-z_]", "", s1)
        return s1.lower()

    # Prepare normalized source columns mapping: normalized -> original
    normalized_src = {normalize(c): c for c in src_cols}

    # Common French -> English synonyms (normalized form)
    synonyms: Dict[str, str] = {
        "polluant": "pollutant",
        "valeur": "value",
        "heure": "hour",
        "adresse": "address",
        # sometimes station id is camelCase "stationId" -> normalized will become station_id
        # keep some common variants
        "stationid": "station_id",
        "idstation": "station_id",
    }

    # Build mapping from table column -> source column (original name) using heuristics
    col_mapping: dict[str, str] = {}
    for tcol in table_cols:
        # 1) exact match
        if tcol in src_cols:
            col_mapping[tcol] = tcol
            continue
        # 2) case-insensitive match
        lower_matches = [s for s in src_cols if s.lower() == tcol.lower()]
        if lower_matches:
            col_mapping[tcol] = lower_matches[0]
            continue
        # 3) normalized match (handles snake_case vs camelCase)
        norm_t = normalize(tcol)
        if norm_t in normalized_src:
            col_mapping[tcol] = normalized_src[norm_t]
            continue
        # 3b) try synonyms: if a source normalized name maps (via synonyms) to this table
        found = False
        for s_norm, orig in normalized_src.items():
            # direct synonym mapping: source (French) -> canonical (English)
            if synonyms.get(s_norm) == norm_t:
                col_mapping[tcol] = orig
                found = True
                break
            # or table token is a French form that maps to a source token
            if synonyms.get(norm_t) == s_norm:
                col_mapping[tcol] = orig
                found = True
                break
        if found:
            continue
        # 4) special common synonyms: x/y -> longitude/latitude
        if tcol.lower() in ("longitude", "lon", "lng"):
            for candidate in ("longitude", "long", "lng", "x"):
                if candidate in src_cols:
                    col_mapping[tcol] = candidate
                    break
            if tcol in col_mapping:
                continue
        if tcol.lower() in ("latitude", "lat", "y"):
            for candidate in ("latitude", "lat", "y"):
                if candidate in src_cols:
                    col_mapping[tcol] = candidate
                    break
            if tcol in col_mapping:
                continue
        # Not found — leave unmapped; we'll omit this column so DB default/null used

    # Build cleaned rows that only contain keys the table accepts, using mapping
    cleaned_rows: List[Dict[str, Any]] = []
    dropped_source_cols = set()
    for r in source_rows:
        new_r: Dict[str, Any] = {}
        for tcol, srccol in col_mapping.items():
            # Only add if source row has the value
            if srccol in r:
                new_r[tcol] = r[srccol]
        # Optionally, if table accepts arbitrary columns not mapped, ignore
        cleaned_rows.append(new_r)
        # track dropped
        dropped_source_cols.update(set(r.keys()) - set(new_r.keys()))

    if dropped_source_cols:
        logger.debug(
            "Dropped source columns not present in table or unmapped: %s",
            sorted(dropped_source_cols),
        )

    if not any(cleaned_rows):
        logger.info(
            "No mappable columns found between source and table %s; nothing to insert.",
            table.name,
        )
        return 0

    # Ensure rows contain all primary key columns; drop any rows that don't to avoid DB integrity errors
    pk_cols = [c.name for c in table.columns if c.primary_key]
    if pk_cols:
        before = len(cleaned_rows)
        filtered_rows = [
            r
            for r in cleaned_rows
            if all((pk in r and r.get(pk) is not None) for pk in pk_cols)
        ]
        dropped = before - len(filtered_rows)
        if dropped:
            logger.info(
                "Dropped %d rows because they were missing primary key columns %s",
                dropped,
                pk_cols,
            )
        cleaned_rows = filtered_rows

    if not cleaned_rows:
        logger.info(
            "No rows remain after primary-key filtering for table %s; nothing to insert.",
            table.name,
        )
        return 0

    # Build an upsert statement appropriate for the database dialect
    dialect_name = engine.dialect.name.lower()

    if dialect_name in ("mysql", "mariadb"):
        insert_stmt = mysql_insert(table).values(cleaned_rows)
        update_cols = {
            c.name: insert_stmt.inserted[c.name]
            for c in table.columns
            if not c.primary_key
        }
        if update_cols:
            stmt = insert_stmt.on_duplicate_key_update(**update_cols)
        else:
            stmt = insert_stmt
    elif dialect_name == "sqlite":
        # SQLite: use INSERT OR REPLACE into a table. This replaces the row when PK/unique conflict.
        # Build a plain INSERT... values statement, but prepend 'OR REPLACE'.
        insert_stmt = sa.insert(table).values(cleaned_rows)
        # SQLAlchemy doesn't have a direct 'or replace' builder, but we can compile a textual prefix
        stmt = insert_stmt.prefix_with("OR REPLACE")
    else:
        # Fallback: plain INSERT; duplicates may cause integrity errors
        stmt = sa.insert(table).values(cleaned_rows)

    with engine.begin() as conn:
        result = conn.execute(stmt)
        rowcount = getattr(result, "rowcount", -1)
        logger.info("Upsert executed, rowcount=%s", rowcount)
        return rowcount


def apply_mapping_and_filter(
    table: Table, rows: List[Dict[str, Any]], mapping: dict | None
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Apply explicit mapping (table_col -> source_key or const:VALUE) then filter rows missing PK values.

    mapping format example:
      { "station_id": "_id", "pollutant": "pm25", "hour": "heure" }
    or supporting constants:
      { "source": "const:montreal" }
    """
    if mapping is None:
        # No mapping provided: return original rows as-is, and no discarded rows
        return rows, []

    pk_cols = [c.name for c in table.columns if c.primary_key]
    mapped_rows: List[Dict[str, Any]] = []
    discarded: List[Dict[str, Any]] = []

    for r in rows:
        new_r: Dict[str, Any] = {}
        for tcol, rule in mapping.items():
            if isinstance(rule, str) and rule.startswith("const:"):
                new_r[tcol] = rule.split("const:", 1)[1]
            else:
                # simple mapping: use value from source if present
                if rule in r:
                    new_r[tcol] = r[rule]
        # drop rows that miss any PKs (to avoid IntegrityError on insert)
        missing_pks = [
            pk for pk in pk_cols if (pk not in new_r) or (new_r.get(pk) is None)
        ]
        if missing_pks:
            reason = f"missing_pk:{','.join(missing_pks)}"
            discarded.append(
                {
                    "reason": reason,
                    "original": r,
                    "mapped": new_r,
                }
            )
            logger.debug(
                "Dropping row because missing PKs %s: original=%s mapped=%s",
                missing_pks,
                r,
                new_r,
            )
            continue
        mapped_rows.append(new_r)

    logger.info(
        "After mapping and PK filtering: %d rows will be inserted (from %d), discarded=%d",
        len(mapped_rows),
        len(rows),
        len(discarded),
    )
    return mapped_rows, discarded


def get_engine_from_env() -> sa.engine.Engine:
    """Create a SQLAlchemy engine using TIDB env var.

    Expects environment variable TIDB to hold a valid SQLAlchemy URL.
    """
    connstr = os.getenv("TIDB")
    if not connstr:
        logger.error(
            "Environment variable TIDB is not set. Please set it to the SQLAlchemy connection string."
        )
        raise RuntimeError("TIDB connection string not found in environment")

    # Some users put quotes around the value (TIDB="mysql://...") — strip surrounding quotes
    if isinstance(connstr, str):
        connstr = connstr.strip()
        if (connstr.startswith('"') and connstr.endswith('"')) or (
            connstr.startswith("'") and connstr.endswith("'")
        ):
            connstr = connstr[1:-1]
            logger.debug("Stripped surrounding quotes from TIDB env var")

    # If user provided a bare mysql:// URL (without driver), SQLAlchemy will try MySQLdb
    # which requires the MySQLdb module (mysqlclient). Try common fallbacks and provide
    # clear guidance if a DBAPI is missing.
    tried = []
    # If connstr already specifies a driver (contains '+'), try as-is first
    candidates = (
        [connstr]
        if "+" in connstr.split("://", 1)[0]
        else [
            connstr,
            # prefer mysql+mysqlconnector (mysql-connector-python)
            connstr.replace("mysql://", "mysql+mysqlconnector://", 1),
            # pure-Python alternative
            connstr.replace("mysql://", "mysql+pymysql://", 1),
        ]
    )

    last_exc: Exception | None = None
    for c in candidates:
        try:
            logger.info("Attempting to create engine with URL: %s", c)
            engine = sa.create_engine(c, pool_pre_ping=True)
            logger.info("SQLAlchemy engine created using %s", c.split("://", 1)[0])
            return engine
        except ModuleNotFoundError as mnfe:
            # Specific missing DBAPI
            tried.append((c, str(mnfe)))
            last_exc = mnfe
            logger.warning("DBAPI missing for URL %s: %s", c, mnfe)
            continue
        except Exception as exc:
            tried.append((c, str(exc)))
            last_exc = exc
            # Common failures here include network/credential errors. Provide actionable advice.
            logger.warning("Engine creation failed for URL %s: %s", c, exc)
            if hasattr(exc, "__cause__") and exc.__cause__:
                logger.debug("Underlying exception: %s", exc.__cause__)
            continue

    # If we reach here, none worked. Provide actionable guidance.
    msg_lines = [
        "Could not create SQLAlchemy engine with the provided TIDB connection string.",
        "Tried the following connection strings and errors:",
    ]
    for c, e in tried:
        msg_lines.append(f" - {c} -> {e}")

    msg_lines.append("")
    msg_lines.append("Common fixes:")
    msg_lines.append(
        " - Install a MySQL DBAPI. On Debian/Ubuntu for mysqlclient (C extension):"
    )
    msg_lines.append(
        "     sudo apt-get install build-essential default-libmysqlclient-dev && pip install mysqlclient"
    )
    msg_lines.append(
        " - Or install a pure-Python driver and use mysql+mysqlconnector:// or mysql+pymysql://"
    )
    msg_lines.append("     pip install mysql-connector-python")
    msg_lines.append("     pip install pymysql")
    msg_lines.append(
        " - Ensure your TIDB string includes the driver prefix, e.g. mysql+mysqlconnector:// or mysql+pymysql://"
    )

    full_msg = "\n".join(msg_lines)
    logger.error(full_msg)
    # Raise the last caught exception for visibility
    if last_exc:
        raise last_exc
    raise RuntimeError("Failed to create SQLAlchemy engine; see logs for details")


def run_once(table_name: str = DEFAULT_TABLE, mapping: dict | None = None) -> None:
    """Run one sync cycle: fetch -> transform -> ensure table -> upsert."""
    logger.info("Starting sync run for table %s", table_name)

    json_obj = fetch_json(API_URL)
    records = json_obj.get("result", {}).get("records", [])
    df = records_to_polars(records)

    engine = get_engine_from_env()
    table = ensure_table(engine, table_name, df)

    source_rows = df.to_dicts()
    # Apply explicit mapping and filter rows missing primary keys (to avoid IntegrityError)
    rows_to_insert, discarded = apply_mapping_and_filter(table, source_rows, mapping)

    if discarded:
        # Ensure artifacts folder exists
        artifacts_dir = os.path.join(os.path.dirname(__file__), "artifacts")
        os.makedirs(artifacts_dir, exist_ok=True)
        ts = time.strftime("%Y%m%d_%H%M%S")
        discard_path = os.path.join(
            artifacts_dir, f"discarded_rows_{table_name}_{ts}.json"
        )
        with open(discard_path, "w", encoding="utf-8") as f:
            json.dump(discarded, f, ensure_ascii=False, indent=2)
        logger.info("Wrote %d discarded rows to %s", len(discarded), discard_path)

    if not rows_to_insert:
        logger.info(
            "No rows to insert after mapping/filtering for table %s", table_name
        )
        return

    # Build a small DataFrame from mapped rows to pass to upsert (reuse existing logic expects Polars DF)
    df_mapped = pl.DataFrame(rows_to_insert)
    affected = upsert_polars_df(engine, table, df_mapped)
    logger.info("Sync run complete: %d rows affected", affected)

    # Write a small run summary into the artifacts folder for easy inspection
    try:
        artifacts_dir = os.path.join(os.path.dirname(__file__), "artifacts")
        os.makedirs(artifacts_dir, exist_ok=True)
        ts = time.strftime("%Y-%m-%dT%H:%M:%S")
        summary = {
            "table": table_name,
            "timestamp": ts,
            "rows_fetched": df.height,
            "rows_affected": affected,
        }
        summary_path = os.path.join(artifacts_dir, f"last_run_{table_name}.json")
        with open(summary_path, "w", encoding="utf-8") as sf:
            json.dump(summary, sf, ensure_ascii=False, indent=2)
        logger.info("Wrote run summary to %s", summary_path)
    except Exception:
        logger.debug("Failed to write run summary to artifacts", exc_info=True)


def schedule_daily(
    table_name: str = DEFAULT_TABLE,
    hour: int = 6,
    minute: int = 0,
    mapping: dict | None = None,
) -> None:
    """Start a blocking scheduler that runs run_once daily at given time."""
    scheduler = BlockingScheduler()

    scheduler.add_job(
        lambda: run_once(table_name, mapping=mapping),
        trigger="cron",
        hour=hour,
        minute=minute,
    )
    logger.info("Scheduler started: daily at %02d:%02d", hour, minute)
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped")


if __name__ == "__main__":
    import argparse

    from dotenv import load_dotenv

    load_dotenv()  # allow reading TIDB from a .env file for convenience

    parser = argparse.ArgumentParser(description="air_quality_sync pipeline")
    parser.add_argument("--once", action="store_true", help="Run one sync and exit")
    parser.add_argument(
        "--schedule",
        action="store_true",
        help="Run as a scheduler (daily at 06:00 by default)",
    )
    parser.add_argument("--table", default=DEFAULT_TABLE, help="Destination table name")
    parser.add_argument(
        "--mapping",
        default=None,
        help="Path to JSON mapping file (table_col -> source_key or const:VALUE)",
    )
    parser.add_argument(
        "--hour", type=int, default=6, help="Hour for scheduled run (0-23)"
    )
    parser.add_argument(
        "--minute", type=int, default=0, help="Minute for scheduled run (0-59)"
    )

    args = parser.parse_args()

    # Validate TIDB presence early
    try:
        _ = os.getenv("TIDB")
        if not _:
            logger.warning(
                "TIDB env var not found. You can set it via environment or a .env file."
            )
    except Exception:
        pass

    # Load mapping if provided
    mapping = None
    if args.mapping:
        try:
            with open(args.mapping, "r", encoding="utf-8") as f:
                mapping = json.load(f)
        except Exception as e:
            logger.error("Failed to load mapping file %s: %s", args.mapping, e)
            raise

    if args.once:
        run_once(args.table, mapping=mapping)
    elif args.schedule:
        # Schedule should pass mapping through
        schedule_daily(args.table, hour=args.hour, minute=args.minute, mapping=mapping)
    else:
        # Default: run once
        run_once(args.table, mapping=mapping)
