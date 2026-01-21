from pathlib import Path
from dotenv import load_dotenv
import os

import pandas as pd
from sqlalchemy import create_engine, text


# ---- Load environment variables (.env) ----
load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD")  # must exist in .env
DB_NAME = os.getenv("DB_NAME", "data_center_portafolio")
DB_PORT = int(os.getenv("DB_PORT", "3306"))

if not DB_PASSWORD:
    raise ValueError("DB_PASSWORD is missing. Add it to your .env file (and do NOT commit .env to GitHub).")


# ---- Robust project-relative path to the Excel file ----
PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXCEL_PATH = PROJECT_ROOT / "data" / "data_center_simulated_3_months.xlsx"

if not EXCEL_PATH.exists():
    raise FileNotFoundError(f"Excel file not found at: {EXCEL_PATH}")


# ---- MySQL engine ----
engine = create_engine(
    f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)


def truncate(conn, table_name: str) -> None:
    conn.execute(text(f"TRUNCATE TABLE {table_name};"))


def main() -> None:
    # 1) Read Excel
    ops = pd.read_excel(EXCEL_PATH, sheet_name="dc_operations")
    costs = pd.read_excel(EXCEL_PATH, sheet_name="dc_energy_costs")

    # 2) Convert datetimes
    ops["datetime"] = pd.to_datetime(ops["datetime"])
    costs["datetime"] = pd.to_datetime(costs["datetime"])

    # 3) Clean staging
    with engine.begin() as conn:
        truncate(conn, "stg_operations")
        truncate(conn, "stg_energy_costs")

    # 4) Load to MySQL
    ops.to_sql(
        "stg_operations",
        engine,
        if_exists="append",
        index=False,
        chunksize=2000,
        method="multi"
    )

    costs.to_sql(
        "stg_energy_costs",
        engine,
        if_exists="append",
        index=False,
        chunksize=2000,
        method="multi"
    )

    # 5) Validate
    with engine.connect() as conn:
        ops_count = conn.execute(text("SELECT COUNT(*) FROM stg_operations")).scalar()
        costs_count = conn.execute(text("SELECT COUNT(*) FROM stg_energy_costs")).scalar()

    print("✅ Carga STG completada")
    print(f"stg_operations rows: {ops_count}")
    print(f"stg_energy_costs rows: {costs_count}")


if __name__ == "__main__":
    main()
