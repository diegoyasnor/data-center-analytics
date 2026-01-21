import pandas as pd
import getpass
from sqlalchemy import create_engine, text

USER = "root"
PASSWORD = getpass.getpass("MySQL password: ")
HOST = "localhost"
PORT = 3306
DB = "data_center_portafolio"

engine = create_engine(
    f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}"
)

def truncate(conn, table):
    conn.execute(text(f"TRUNCATE TABLE {table};"))

def main():
    ops = pd.read_sql("SELECT * FROM stg_operations", engine)
    costs = pd.read_sql("SELECT * FROM stg_energy_costs", engine)

    print(f"STG operations: {len(ops)} filas")
    print(f"STG energy_costs: {len(costs)} filas")

    ops["datetime"] = pd.to_datetime(ops["datetime"])
    costs["datetime"] = pd.to_datetime(costs["datetime"])

    ops["zone"] = ops["zone"].str.strip().str.upper()
    ops["rack_id"] = ops["rack_id"].str.strip().str.upper()
    costs["zone"] = costs["zone"].str.strip().str.upper()

    ops = ops.dropna(subset=[
        "datetime", "zone", "rack_id",
        "power_kw", "temperature_c",
        "utilization_pct", "incidents"
    ])

    ops = ops[
        (ops["power_kw"] >= 0) &
        (ops["utilization_pct"].between(0, 100)) &
        (ops["incidents"].isin([0, 1]))
    ]

    ops["incidents"] = ops["incidents"].astype(int)

    costs = costs.dropna(subset=[
        "datetime", "zone",
        "total_power_kw", "cost_per_kw_usd"
    ])

    costs = costs[
        (costs["total_power_kw"] >= 0) &
        (costs["cost_per_kw_usd"] > 0)
    ]

    costs["date"] = costs["datetime"].dt.date
    costs = costs[["date", "zone", "total_power_kw", "cost_per_kw_usd"]]

    print(f"CLN operations: {len(ops)} filas")
    print(f"CLN energy_costs: {len(costs)} filas")

    with engine.begin() as conn:
        truncate(conn, "cln_operations")
        truncate(conn, "cln_energy_costs")

    ops.to_sql(
        "cln_operations",
        engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=2000
    )

    costs.to_sql(
        "cln_energy_costs",
        engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=2000
    )

    print("✅ Clean data creado correctamente en MySQL")

if __name__ == "__main__":
    main()
