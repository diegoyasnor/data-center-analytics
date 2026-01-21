import pandas as pd
import getpass
from sqlalchemy import create_engine, text

USER = "root"
PASSWORD = getpass.getpass("MySQL password: ")
HOST = "localhost"
PORT = 3306
DB = "data_center_portafolio"

engine = create_engine(f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}")

def truncate(conn, table):
    conn.execute(text(f"TRUNCATE TABLE {table};"))

def main():
    # 1) Leer CLEAN
    ops = pd.read_sql("SELECT * FROM cln_operations", engine)
    costs = pd.read_sql("SELECT * FROM cln_energy_costs", engine)

    ops["datetime"] = pd.to_datetime(ops["datetime"])
    costs["date"] = pd.to_datetime(costs["date"]).dt.date

    # 2) DIM_ZONE
    zones = pd.DataFrame({
        "zone": pd.Series(pd.concat([ops["zone"], costs["zone"]]).unique()).sort_values()
    })

    # 3) DIM_TIME_HOUR (solo horas presentes en operations)
    times = pd.DataFrame({"datetime": pd.Series(ops["datetime"].unique()).sort_values()})
    times["date"] = times["datetime"].dt.date
    times["hour"] = times["datetime"].dt.hour.astype(int)
    times["month"] = times["datetime"].dt.month.astype(int)
    times["year"] = times["datetime"].dt.year.astype(int)

    with engine.begin() as conn:
        truncate(conn, "dim_zone")
        truncate(conn, "dim_time_hour")

    zones.to_sql("dim_zone", engine, if_exists="append", index=False, method="multi", chunksize=2000)
    times.to_sql("dim_time_hour", engine, if_exists="append", index=False, method="multi", chunksize=2000)

    # 4) Mapear keys
    dim_zone = pd.read_sql("SELECT zone_key, zone FROM dim_zone", engine)
    dim_time = pd.read_sql("SELECT time_key, datetime FROM dim_time_hour", engine)

    zone_map = dict(zip(dim_zone["zone"], dim_zone["zone_key"]))
    time_map = dict(zip(pd.to_datetime(dim_time["datetime"]), dim_time["time_key"]))

    # 5) FACT operations_sampled (grain real)
    ops_fact = ops.copy()
    ops_fact["zone_key"] = ops_fact["zone"].map(zone_map)
    ops_fact["time_key"] = ops_fact["datetime"].map(time_map)

    ops_fact = ops_fact.dropna(subset=["zone_key", "time_key"]).copy()
    ops_fact["zone_key"] = ops_fact["zone_key"].astype(int)
    ops_fact["time_key"] = ops_fact["time_key"].astype(int)

    ops_fact = ops_fact[["time_key", "zone_key", "rack_id", "power_kw", "temperature_c", "utilization_pct", "incidents"]]

    # 6) FACT zone_hourly (agregado real para series temporales)
    zone_hour = (
        ops_fact.groupby(["time_key", "zone_key"], as_index=False)
        .agg(
            avg_power_kw=("power_kw", "mean"),
            avg_temperature_c=("temperature_c", "mean"),
            avg_utilization_pct=("utilization_pct", "mean"),
            incident_count=("incidents", "sum"),
            sample_count=("incidents", "count")
        )
    )

    # 7) FACT energy_cost_daily
    cost_fact = costs.copy()
    cost_fact["zone_key"] = cost_fact["zone"].map(zone_map)
    cost_fact = cost_fact.dropna(subset=["zone_key"]).copy()
    cost_fact["zone_key"] = cost_fact["zone_key"].astype(int)
    cost_fact["daily_cost_usd"] = cost_fact["total_power_kw"] * cost_fact["cost_per_kw_usd"]
    cost_fact = cost_fact[["date", "zone_key", "total_power_kw", "cost_per_kw_usd", "daily_cost_usd"]]

    with engine.begin() as conn:
        truncate(conn, "fact_operations_sampled")
        truncate(conn, "fact_zone_hourly")
        truncate(conn, "fact_energy_cost_daily")

    ops_fact.to_sql("fact_operations_sampled", engine, if_exists="append", index=False, method="multi", chunksize=2000)
    zone_hour.to_sql("fact_zone_hourly", engine, if_exists="append", index=False, method="multi", chunksize=2000)
    cost_fact.to_sql("fact_energy_cost_daily", engine, if_exists="append", index=False, method="multi", chunksize=2000)

    print("✅ MART construido: dim_zone, dim_time_hour, fact_* listos")

if __name__ == "__main__":
    main()
