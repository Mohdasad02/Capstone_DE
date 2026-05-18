# Databricks notebook source
storage_account_name = "asadretaildatalake"
storage_account_key  = "TdocxHu1Ozw/co0Hk0GurCJ23BHjG9SYZIhT1hdcLOYZ0NU3y7fBNdd7agg5NLPe0u3tu6JHqoOK+AStG0qK9g=="

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)
sc._jsc.hadoopConfiguration().set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

base_path = (
    f"abfss://datalake@{storage_account_name}"
    f".dfs.core.windows.net"
)

# silver streaming path
silver_streaming_path = (
    f"{base_path}/silver/fact_streaming_orders/"
)

# gold paths
gold_streaming_path  = (
    f"{base_path}/gold/streaming/fact_streaming_orders/"
)
gold_products_path   = f"{base_path}/gold/dim_products/"
gold_location_path   = f"{base_path}/gold/dim_location/"
gold_date_path       = f"{base_path}/gold/dim_date/"

print("Paths configured")

# COMMAND ----------

from pyspark.sql.functions import (
    col, date_format, hour,
    round as spark_round,
    sum as spark_sum,
    count as spark_count,
    avg as spark_avg,
    current_timestamp, lit
)
from delta.tables import DeltaTable
from datetime import date

today = date.today().strftime("%Y-%m-%d")

# read silver streaming orders
streaming_df = spark.read \
    .format("delta") \
    .load(silver_streaming_path)

total = streaming_df.count()
print(f"Total streaming orders : {total}")
print(f"Today's date           : {today}")

if total == 0:
    print("No streaming orders to process")
    dbutils.notebook.exit("No streaming orders")

streaming_df.show(3, truncate=False)

# COMMAND ----------

# read existing gold dimensions
# built by 05_silver_to_gold
dim_products = spark.read \
    .format("delta") \
    .load(gold_products_path)

dim_location = spark.read \
    .format("delta") \
    .load(gold_location_path)

dim_date = spark.read \
    .format("delta") \
    .load(gold_date_path)

print(f"dim_products : {dim_products.count()} rows")
print(f"dim_location : {dim_location.count()} rows")
print(f"dim_date     : {dim_date.count()} rows")

# COMMAND ----------

# join streaming orders with dimensions
# to get all attributes needed for dashboard

fact_streaming = streaming_df.alias("o") \
    .join(
        dim_products.alias("p"),
        col("o.product_id") == col("p.product_id"),
        how="left"
    ) \
    .join(
        dim_location.alias("l"),
        col("o.city") == col("l.city"),
        how="left"
    ) \
    .join(
        dim_date.alias("d"),
        col("o.order_date") == col("d.date_str"),
        how="left"
    ) \
    .select(
        col("o.order_id"),
        col("o.customer_id"),
        col("o.product_id"),
        col("p.product_name"),
        col("p.category"),
        col("l.location_id"),
        col("o.city"),
        col("d.date_id"),
        col("o.order_date"),
        col("o.order_hour"),
        col("o.order_time"),
        col("o.amount_inr"),
        col("o.amount_usd"),
        col("o.temperature"),
        col("o.weather_condition"),
        col("o.wind_speed"),
        col("o.humidity"),
        col("o.usd_exchange_rate"),
        col("o.source"),
        current_timestamp().alias("processed_at")
    )

print(f"Streaming fact table: {fact_streaming.count()} rows")
fact_streaming.show(3, truncate=False)

# COMMAND ----------

try:
    streaming_gold_table = DeltaTable.forPath(
                            spark, gold_streaming_path)
    streaming_gold_table.alias("existing").merge(
        fact_streaming.alias("new"),
        "existing.order_id = new.order_id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
    print("Streaming orders merged into Gold")

except:
    fact_streaming.write \
        .format("delta") \
        .mode("overwrite") \
        .save(gold_streaming_path)
    print("Gold streaming table created")

count = spark.read \
    .format("delta") \
    .load(gold_streaming_path) \
    .count()
print(f"Total in gold/streaming/fact_streaming_orders: {count}")

# COMMAND ----------

# these aggregations are what
# Dashboard 2 will show in Power BI

gold_streaming = spark.read \
    .format("delta") \
    .load(gold_streaming_path)

print("=" * 55)
print("LIVE DASHBOARD KPIs")
print("=" * 55)

# KPI 1 → total orders and revenue today
print("\nKPI 1 — Today's Revenue:")
gold_streaming.agg(
    spark_count("order_id").alias("total_orders"),
    spark_round(
        spark_sum("amount_inr"), 2
    ).alias("revenue_inr"),
    spark_round(
        spark_sum("amount_usd"), 2
    ).alias("revenue_usd"),
    spark_round(
        spark_avg("amount_inr"), 2
    ).alias("avg_order_value_inr")
).show(truncate=False)

# KPI 2 → orders by city
print("KPI 2 — Orders by City:")
gold_streaming \
    .groupBy("city") \
    .agg(
        spark_count("order_id").alias("orders"),
        spark_round(
            spark_sum("amount_inr"), 2
        ).alias("revenue_inr")
    ) \
    .orderBy(col("revenue_inr").desc()) \
    .show(5, truncate=False)

# KPI 3 → orders by category
print("KPI 3 — Orders by Category:")
gold_streaming \
    .groupBy("category") \
    .agg(
        spark_count("order_id").alias("orders"),
        spark_round(
            spark_sum("amount_inr"), 2
        ).alias("revenue_inr")
    ) \
    .orderBy(col("revenue_inr").desc()) \
    .show(truncate=False)

# KPI 4 → orders by hour
print("KPI 4 — Orders by Hour:")
gold_streaming \
    .groupBy("order_hour") \
    .agg(
        spark_count("order_id").alias("orders")
    ) \
    .orderBy("order_hour") \
    .show(truncate=False)

# KPI 5 → weather impact on sales
print("KPI 5 — Sales by Weather Condition:")
gold_streaming \
    .groupBy("weather_condition") \
    .agg(
        spark_count("order_id").alias("orders"),
        spark_round(
            spark_sum("amount_inr"), 2
        ).alias("revenue_inr")
    ) \
    .orderBy(col("orders").desc()) \
    .show(truncate=False)

# COMMAND ----------

print("=" * 55)
print("GOLD STREAMING SUMMARY")
print("=" * 55)

print(f"\nDate           : {today}")
print(f"Total orders   : {gold_streaming.count()}")

print("\nGold streaming table schema:")
gold_streaming.printSchema()

print("\nSample rows:")
gold_streaming.select(
    "order_id", "city", "category",
    "amount_inr", "amount_usd",
    "temperature", "weather_condition",
    "order_time", "processed_at"
).show(5, truncate=False)

print("\n05_silver_to_gold_stream complete ✓")
print("Gold streaming layer ready for Power BI Dashboard 2!")