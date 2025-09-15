# (removed leading markdown fence)
# air_quality_sync

  
  # Or, if you prefer mysql-connector:
  # TIDB=mysql+mysqlconnector://user:password@host:3306/dbname
  
  # PowerShell on Windows
  docker run --env-file .env -v $(pwd).Path\artifacts:/app/artifacts air_quality_sync:latest
# air_quality_sync

Small pipeline to sync Montreal air quality data into a TiDB/MySQL-compatible database.

Requirements

- Python 3.9+

- Install dependencies from `requirements.txt`:
# air_quality_sync

Small pipeline to sync Montreal air quality data into a TiDB/MySQL-compatible database.

Requirements

- Python 3.9+

- Install dependencies from `requirements.txt`:

  pip install -r requirements.txt

Configuration

- Set environment variable `TIDB` to a SQLAlchemy-compatible connection string, for example:

  mysql+mysqlconnector://user:password@host:3306/dbname

- For convenience you can create a `.env` file with the common variables used by the container. Do NOT wrap values in surrounding quotes.

  Example (pymysql):

  TIDB=mysql+pymysql://user:password@host:3306/dbname

Usage

- Run once locally (quick test):

  python main.py --once

- Run scheduler (blocking) to execute daily at 06:00 local time:

  python main.py --schedule

Notes

- The script will attempt to create the destination table if it does not exist. It will try to detect primary keys named `_id`, `id` or `ID`. If none exist, it will create an `ingest_id` autoincrement primary key.

- Upserts use dialect-specific semantics (MySQL: ON DUPLICATE KEY UPDATE; SQLite: INSERT OR REPLACE). Ensure appropriate unique/primary keys are present for deduplication.

Troubleshooting

- If you see connection errors, verify your `TIDB` string and network access (firewalls, allowed IPs).

## Docker

Build the image:

  docker build -t air_quality_sync:latest .

Run with an `.env` file (copy `.env.example` to `.env` and edit):

  # POSIX / macOS / Linux
  docker run --env-file .env -v $(pwd)/artifacts:/app/artifacts air_quality_sync:latest

  # PowerShell on Windows (pwsh)
  docker run --env-file .env -v ${PWD}\artifacts:/app/artifacts air_quality_sync:latest

Or use docker-compose (reads `.env` automatically):

  docker-compose up --build -d

Notes:

- Ensure your `.env` contains a valid `TIDB` SQLAlchemy URL (for example `mysql+pymysql://user:pass@host:3306/db`).
- Artifacts such as discarded rows will be written to the mounted `artifacts/` folder.
- The compose file includes a healthcheck that verifies `/app/artifacts/last_run_${TABLE}.json` is fresh (start_period 5m, freshness window 24h). Use `RUN_ON_START=1` to force an immediate run on startup so health becomes green quickly.

Testing locally with SQLite (quick smoke test)

We provide `.env.test` for a safe local run using SQLite. You can run:

  # POSIX
  docker run --env-file .env.test -v $(pwd)/artifacts:/app/artifacts air_quality_sync:latest

  # PowerShell
  docker run --env-file .env.test -v ${PWD}\artifacts:/app/artifacts air_quality_sync:latest

This uses an in-container SQLite DB located at `/app/artifacts/test_db.sqlite` and is intended for quick smoke tests without external DB access.

Production deployment notes

- Recommended `.env` snippet for production (replace placeholders):

  TIDB=mysql+pymysql://<DB_USER>:<DB_PASS>@<DB_HOST>:<DB_PORT>/<DB_NAME>
  RUN_MODE=schedule
  # Optional: run one sync immediately on container start, then continue scheduling
  RUN_ON_START=1
  SCHEDULE_HOUR=06
  SCHEDULE_MINUTE=00
  TABLE=air_quality_joined
  MAPPING_PATH=/app/config/mapping.json

- Use secrets (Docker secrets, Kubernetes Secrets, Vault, etc.) for credentials instead of storing them in `.env` in source control.
- If your DB requires TLS or special CA, ensure the container image has the CA installed and your TIDB URL/driver supports TLS options.

Health and observability

- The container writes `artifacts/last_run_<table>.json` after each successful run with a timestamp and rows counts. This file is used by the compose `healthcheck`.
- Logs are written to stdout/stderr so they can be collected by your logging stack.
