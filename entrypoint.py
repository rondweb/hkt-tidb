"""Container entrypoint: reads environment variables and runs the sync job.

Environment variables supported:
 - TIDB: SQLAlchemy connection string (required to actually write)
 - RUN_MODE: 'once' or 'schedule' (default 'once')
 - ONCE: if set to '1' will run once and exit (alternative to RUN_MODE)
 - SCHEDULE_HOUR: hour for daily schedule (default 6)
 - SCHEDULE_MINUTE: minute for daily schedule (default 0)
 - TABLE: destination table name (default from main.py)
 - MAPPING_PATH: optional path inside container to a JSON mapping file

This script imports main.run_once and main.schedule_daily and executes based on env.
"""

from __future__ import annotations

import json
import logging
import os

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from main import DEFAULT_TABLE, run_once, schedule_daily


def load_mapping(path: str | None):
    if not path:
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error("Failed to load mapping file %s: %s", path, e)
        raise


def main():
    table = os.getenv("TABLE", DEFAULT_TABLE)
    run_mode = os.getenv("RUN_MODE", "once").lower()
    once_flag = os.getenv("ONCE", "0")

    mapping_path = os.getenv("MAPPING_PATH")
    mapping = load_mapping(mapping_path)

    # Ensure artifacts dir exists so SQLite can create files there
    artifacts_dir = os.path.join(os.path.dirname(__file__), "artifacts")
    try:
        os.makedirs(artifacts_dir, exist_ok=True)
    except Exception:
        logger.debug("Could not ensure artifacts directory exists: %s", artifacts_dir)

    # Diagnostic: try writing a small file to artifacts to check mount/permissions
    test_path = os.path.join(artifacts_dir, ".write_test")
    try:
        with open(test_path, "w", encoding="utf-8") as tf:
            tf.write("ok")
        with open(test_path, "r", encoding="utf-8") as tf:
            content = tf.read()
        logger.info("Artifacts dir check OK: %s (content=%s)", artifacts_dir, content)
        try:
            os.remove(test_path)
        except Exception:
            pass
    except Exception as e:
        logger.error(
            "Artifacts dir NOT writable or accessible: %s -> %s", artifacts_dir, e
        )
        logger.error(
            "If you're mounting a host volume, ensure the host path exists and Docker has permission to write to it."
        )
        # Continue anyway; the subsequent DB connect will surface the same error with full traceback

    if once_flag == "1" or run_mode == "once":
        logger.info("Running single sync (table=%s)", table)
        run_once(table_name=table, mapping=mapping)
        return

    # schedule mode
    try:
        hour = int(os.getenv("SCHEDULE_HOUR", "6"))
        minute = int(os.getenv("SCHEDULE_MINUTE", "0"))
    except ValueError:
        logger.warning("Invalid schedule hour/minute; falling back to 06:00")
        hour = 6
        minute = 0

    # Optional: run once immediately on start before entering the blocking scheduler
    run_on_start = os.getenv("RUN_ON_START", "0")
    if run_on_start == "1":
        logger.info("RUN_ON_START is set: running one sync before starting scheduler")
        try:
            run_once(table_name=table, mapping=mapping)
        except Exception as e:
            logger.exception("Immediate run failed: %s", e)

    logger.info("Starting scheduler (table=%s) daily at %02d:%02d", table, hour, minute)
    schedule_daily(table_name=table, hour=hour, minute=minute, mapping=mapping)


if __name__ == "__main__":
    main()
