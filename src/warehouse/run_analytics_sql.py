"""
Analytical SQL Query Runner.

Executes all 7 analytical queries under sql/analytics/ against the
relational warehouse (MySQL or local SQLite warehouse) and prints
formatted result tables.
"""

import argparse
import sys
import time
from pathlib import Path
from typing import Dict, List

import pandas as pd
from sqlalchemy import create_engine, event, text

# Add project root to sys.path
BASE_DIR = Path(__file__).resolve().parent.parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from config.logging_config import setup_logger
from config.pipeline_config import config

logger = setup_logger("run_analytics_sql")


def get_warehouse_engine(use_sqlite: bool = False, db_url: str = None):
    """Creates SQLAlchemy engine with appropriate dialect adaptations."""
    if db_url:
        target_url = db_url
    elif use_sqlite:
        db_path = BASE_DIR / "data" / "warehouse" / "ecommerce_dwh.db"
        if not db_path.exists():
            raise FileNotFoundError(
                f"SQLite warehouse not found at {db_path.resolve()}. "
                "Run `python src/warehouse/warehouse_loader.py --sqlite --reset` first."
            )
        target_url = f"sqlite:///{db_path.resolve()}"
    else:
        target_url = config.database.connection_url

    engine = create_engine(target_url, pool_pre_ping=True)

    if "sqlite" in target_url:
        @event.listens_for(engine, "connect")
        def register_sqlite_functions(dbapi_connection, connection_record):
            dbapi_connection.create_function(
                "CONCAT", -1, lambda *args: "".join(str(a) if a is not None else "" for a in args)
            )
            dbapi_connection.create_function(
                "LPAD", 3, lambda val, length, pad: str(val).rjust(length, pad[:1])
            )

            def sqlite_datediff(d1, d2):
                if d1 is None or d2 is None:
                    return None
                dt1 = pd.to_datetime(str(d1)[:10])
                dt2 = pd.to_datetime(str(d2)[:10])
                return (dt1 - dt2).days

            dbapi_connection.create_function("DATEDIFF", 2, sqlite_datediff)

    return engine


def run_all_analytical_queries(engine) -> Dict[str, List[pd.DataFrame]]:
    """Reads and executes all 7 analytical SQL scripts."""
    sql_dir = BASE_DIR / "sql" / "analytics"
    query_files = sorted(sql_dir.glob("*.sql"))

    results: Dict[str, List[pd.DataFrame]] = {}
    logger.info("Executing %d analytical SQL scripts from %s...", len(query_files), sql_dir.resolve())

    with engine.connect() as conn:
        for q_file in query_files:
            logger.info("Executing analytical query: %s", q_file.name)
            raw_sql = q_file.read_text(encoding="utf-8")

            # Split statements by semicolon
            dfs = []
            for stmt in raw_sql.split(";"):
                clean_lines = [l for l in stmt.splitlines() if not l.strip().startswith("--")]
                clean_code = "\n".join(clean_lines).strip()
                if clean_code and not clean_code.upper().startswith("USE "):
                    try:
                        df = pd.read_sql_query(text(clean_code), conn)
                        dfs.append(df)
                    except Exception as e:
                        logger.error("Error executing statement in %s: %s", q_file.name, e)
                        raise

            results[q_file.name] = dfs

    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Analytical SQL Queries")
    parser.add_argument("--sqlite", action="store_true", help="Execute queries against SQLite warehouse.")
    parser.add_argument("--db-url", type=str, default=None, help="Custom database connection URL.")
    args = parser.parse_args()

    engine = get_warehouse_engine(use_sqlite=args.sqlite, db_url=args.db_url)
    results = run_all_analytical_queries(engine)

    print("\n" + "=" * 80)
    print("ANALYTICAL SQL EXECUTION REPORT")
    print("=" * 80)
    for script_name, dfs in results.items():
        print(f"\n>>> Script: {script_name} ({len(dfs)} query results)")
        for idx, df in enumerate(dfs, 1):
            print(f"  --- Result Set #{idx} ({len(df)} rows) ---")
            print(df.head(10).to_string(index=False))
            print()
