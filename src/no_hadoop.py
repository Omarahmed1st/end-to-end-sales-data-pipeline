from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, year, month, date_format,
    sum as _sum, avg as _avg, count as _count,
    round as _round, desc
)
import os

INPUT_PATH = "data/100k_Sales_Records.csv"
OUT_DIR = "outputs"

def ensure_dirs():
    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs(f"{OUT_DIR}/csv", exist_ok=True)

def to_pandas_csv(sdf, out_path: str):
    # IMPORTANT: KPI tables are small enough to collect safely
    sdf.toPandas().to_csv(out_path, index=False, encoding="utf-8")

def main():
    ensure_dirs()

    spark = (
        SparkSession.builder
        .appName("Sales100K-ETL-NoHadoopWrite")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # 1) Read CSV
    df = spark.read.csv(INPUT_PATH, header=True, inferSchema=True)

    # 2) Rename columns (spaces -> snake_case)
    rename_map = {
        "Region": "region",
        "Country": "country",
        "Item Type": "item_type",
        "Sales Channel": "sales_channel",
        "Order Priority": "order_priority",
        "Order Date": "order_date",
        "Order ID": "order_id",
        "Ship Date": "ship_date",
        "Units Sold": "units_sold",
        "Unit Price": "unit_price",
        "Unit Cost": "unit_cost",
        "Total Revenue": "total_revenue",
        "Total Cost": "total_cost",
        "Total Profit": "total_profit",
    }
    for old, new in rename_map.items():
        df = df.withColumnRenamed(old, new)

    # 3) Parse dates
    df = df.withColumn("order_date", to_date(col("order_date"), "M/d/yyyy"))
    df = df.withColumn("ship_date", to_date(col("ship_date"), "M/d/yyyy"))

    # 4) Feature engineering
    df = (
        df
        .withColumn("order_year", year(col("order_date")))
        .withColumn("order_month", month(col("order_date")))
        .withColumn("order_year_month", date_format(col("order_date"), "yyyy-MM"))
        .withColumn(
            "profit_margin_pct",
            _round((col("total_profit") / col("total_revenue")) * 100, 2)
        )
    )

    df.cache()

    # ===== KPI 0: Summary =====
    summary = df.agg(
        _count("*").alias("rows"),
        _sum("total_revenue").alias("total_revenue"),
        _sum("total_profit").alias("total_profit"),
        _avg("profit_margin_pct").alias("avg_profit_margin_pct")
    )

    # ===== KPI 1: Monthly Trend =====
    monthly_kpi = (
        df.groupBy("order_year_month")
        .agg(
            _sum("total_revenue").alias("monthly_revenue"),
            _sum("total_profit").alias("monthly_profit"),
            _avg("profit_margin_pct").alias("avg_profit_margin_pct")
        )
        .orderBy("order_year_month")
    )

    # ===== KPI 2: Top Countries =====
    top_countries = (
        df.groupBy("country")
        .agg(
            _sum("total_revenue").alias("revenue"),
            _sum("total_profit").alias("profit")
        )
        .orderBy(desc("revenue"))
        .limit(10)
    )

    # ===== KPI 3: Channel Performance =====
    channel_kpi = (
        df.groupBy("sales_channel")
        .agg(
            _count("*").alias("orders"),
            _sum("total_revenue").alias("revenue"),
            _sum("total_profit").alias("profit"),
            _avg("profit_margin_pct").alias("avg_profit_margin_pct")
        )
        .orderBy(desc("revenue"))
    )

    # ===== KPI 4: Item Type Performance (Top 10) =====
    item_kpi = (
        df.groupBy("item_type")
        .agg(
            _count("*").alias("orders"),
            _sum("units_sold").alias("units_sold"),
            _sum("total_revenue").alias("revenue"),
            _sum("total_profit").alias("profit"),
            _avg("profit_margin_pct").alias("avg_profit_margin_pct")
        )
        .orderBy(desc("revenue"))
        .limit(10)
    )

    # ===== KPI 5: Priority Performance =====
    priority_kpi = (
        df.groupBy("order_priority")
        .agg(
            _count("*").alias("orders"),
            _sum("total_revenue").alias("revenue"),
            _sum("total_profit").alias("profit")
        )
        .orderBy(desc("revenue"))
    )

    # ===== Print for screenshots =====
    print("\n===== SUMMARY =====")
    summary.show(truncate=False)

    print("\n===== MONTHLY KPI (first 12) =====")
    monthly_kpi.show(12, truncate=False)

    print("\n===== TOP COUNTRIES =====")
    top_countries.show(truncate=False)

    print("\n===== CHANNEL KPI =====")
    channel_kpi.show(truncate=False)

    # ===== Export KPIs to CSV via Pandas (no Hadoop needed) =====
    to_pandas_csv(summary, f"{OUT_DIR}/csv/summary.csv")
    to_pandas_csv(monthly_kpi, f"{OUT_DIR}/csv/monthly_kpi.csv")
    to_pandas_csv(top_countries, f"{OUT_DIR}/csv/top_countries.csv")
    to_pandas_csv(channel_kpi, f"{OUT_DIR}/csv/channel_kpi.csv")
    to_pandas_csv(item_kpi, f"{OUT_DIR}/csv/item_kpi.csv")
    to_pandas_csv(priority_kpi, f"{OUT_DIR}/csv/priority_kpi.csv")

    # Quick validation: date parsing nulls
    null_order_dates = df.filter(col("order_date").isNull()).count()
    null_ship_dates = df.filter(col("ship_date").isNull()).count()
    print(f"\nNull order_date rows: {null_order_dates}")
    print(f"Null ship_date rows: {null_ship_dates}")

    spark.stop()
    print("\n✅ Done. Check outputs/csv/ for results.")

if __name__ == "__main__":
    main()
