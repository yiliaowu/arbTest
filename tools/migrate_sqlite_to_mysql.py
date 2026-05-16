# -*- coding: utf-8 -*-
"""One-time migration from database/arb_master.db to MySQL.

The application is MySQL-only after the migration. This script is intentionally
kept as a standalone bridge for moving the existing local SQLite data once.
"""

import argparse
import os
import sqlite3
import sys

import pandas as pd
from sqlalchemy import inspect, text

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, PROJECT_ROOT)

from arbcore.database.db_manager import DatabaseManager


CORE_TABLES = {
    "access_sync_status",
    "etf_raw_api_data",
    "etf_rotation_list",
    "exchange_rate",
    "fund_basket_weights",
    "fund_daily_factors",
    "fund_data",
    "fund_purchase_status",
    "futures_daily",
    "index_daily",
    "jsl_fund_list",
    "raw_api_data",
    "system_health",
    "usa_etf_daily_prices",
}


def quote_identifier(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def get_sqlite_tables(sqlite_conn):
    rows = sqlite_conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name <> 'sqlite_sequence' ORDER BY name"
    ).fetchall()
    return [row[0] for row in rows]


def migrate_table(sqlite_conn, db: DatabaseManager, table: str, reset_core: bool):
    df = pd.read_sql(f'SELECT * FROM "{table}"', sqlite_conn)
    engine = db.get_engine()
    inspector = inspect(engine)
    existing_tables = set(inspector.get_table_names())

    if table in CORE_TABLES:
        if reset_core and table in existing_tables:
            with engine.begin() as conn:
                conn.execute(text(f"TRUNCATE TABLE {quote_identifier(table)}"))
        if not df.empty:
            df.to_sql(table, engine, if_exists="append", index=False)
    else:
        df.to_sql(table, engine, if_exists="replace", index=False)

    with engine.connect() as conn:
        mysql_count = conn.execute(text(f"SELECT COUNT(*) FROM {quote_identifier(table)}")).scalar_one()
    return len(df), mysql_count


def main():
    parser = argparse.ArgumentParser(description="Migrate database/arb_master.db to MySQL arb_master.")
    parser.add_argument(
        "--sqlite-path",
        default=os.path.join(PROJECT_ROOT, "database", "arb_master.db"),
        help="Path to the source SQLite database.",
    )
    parser.add_argument(
        "--append-core",
        action="store_true",
        help="Append into core MySQL tables instead of truncating them first.",
    )
    args = parser.parse_args()

    if not os.path.exists(args.sqlite_path):
        raise FileNotFoundError(f"SQLite database not found: {args.sqlite_path}")

    db = DatabaseManager()
    sqlite_conn = sqlite3.connect(args.sqlite_path)
    try:
        tables = get_sqlite_tables(sqlite_conn)
        print(f"Found {len(tables)} SQLite tables.")
        for table in tables:
            sqlite_count, mysql_count = migrate_table(sqlite_conn, db, table, reset_core=not args.append_core)
            print(f"{table}: sqlite={sqlite_count}, mysql={mysql_count}")
    finally:
        sqlite_conn.close()


if __name__ == "__main__":
    main()
