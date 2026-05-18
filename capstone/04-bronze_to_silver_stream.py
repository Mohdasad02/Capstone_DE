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

# bronze paths
streaming_orders_path = f"{base_path}/bronze/delta/streaming_orders/"
bronze_weather_path   = f"{base_path}/bronze/delta/weather/"
bronze_currency_path  = f"{base_path}/bronze/delta/currency/"

# silver streaming path
silver_streaming_path = f"{base_path}/silver/fact_streaming_orders/"

print("Paths configured")

# COMMAND ----------

# MAGIC %run /Workspace/Users/2211301121@stu.manit.ac.in/capstone/utils/pii_masking

# COMMAND ----------

# MAGIC %run /Workspace/Users/2211301121@stu.manit.ac.in/capstone/utils/api_enrichment

# COMMAND ----------

from pyspark.sql.functions import (
    col, lit, current_timestamp,
    date_format, hour, to_date,
    round as spark_round
)
from datetime import date

today = date.today().strftime("%Y-%m-%d")

# read all streaming orders
streaming_df = spark.read \
    .format("delta") \
    .load(streaming_orders_path)

total = streaming_df.count()
print(f"Total streaming orders in buffer: {total}")

if total == 0:
    print("No streaming orders yet")
    print("Will check again at next 60 min run")
    dbutils.notebook.exit("No streaming orders")

# show sample
streaming_df.select(
    "order_id", "customer_id",
    "product_id", "amount",
    "city", "timestamp"
).show(5, truncate=False)

# COMMAND ----------

# extract date and hour from timestamp
# needed for weather join

streaming_prepared = streaming_df \
    .withColumn(
        "order_date",
        date_format(col("timestamp"), "yyyy-MM-dd")
    ) \
    .withColumn(
        "order_hour",
        hour(col("timestamp"))
    ) \
    .withColumnRenamed("timestamp", "order_time") \
    .withColumn("source", lit("streaming")) \
    .filter(col("order_id").isNotNull()) \
    .filter(col("amount") > 0) \
    .dropDuplicates(["order_id"])

print(f"Streaming orders prepared: {streaming_prepared.count()}")
streaming_prepared.select(
    "order_id", "city",
    "order_time", "order_date", "order_hour"
).show(5, truncate=False)

# COMMAND ----------

# check if today's weather exists in bronze
# if not fetch it and store
# if yes skip API call

print("Checking enrichment for streaming orders...")

weather_count = spark.read \
    .format("delta") \
    .load(bronze_weather_path) \
    .filter(col("order_date") == today) \
    .count()

currency_count = spark.read \
    .format("delta") \
    .load(bronze_currency_path) \
    .filter(col("rate_date") == today) \
    .count()

print(f"Today's weather records : {weather_count}")
print(f"Today's currency records: {currency_count}")

if weather_count == 0 or currency_count == 0:
    print("\nToday's data missing → fetching...")
    run_api_enrichment(streaming_prepared, base_path)
    print("Today's enrichment data stored in Bronze")
else:
    print("\nToday's enrichment already in Bronze ✓")
    print("Skipping API calls")

# COMMAND ----------

# read full weather and currency tables
weather_df  = spark.read \
    .format("delta") \
    .load(bronze_weather_path)

currency_df = spark.read \
    .format("delta") \
    .load(bronze_currency_path)

print(f"Weather records  : {weather_df.count()}")
print(f"Currency records : {currency_df.count()}")

# COMMAND ----------

streaming_weather = streaming_prepared.alias("o") \
    .join(
        weather_df.alias("w"),
        (col("o.city")       == col("w.city")) &
        (col("o.order_date") == col("w.order_date")) &
        (col("o.order_hour") == col("w.order_hour")),
        how="left"
    ) \
    .select(
        col("o.order_id"),
        col("o.customer_id"),
        col("o.product_id"),
        col("o.amount"),
        col("o.currency"),
        col("o.city"),
        col("o.order_time"),
        col("o.order_date"),
        col("o.order_hour"),
        col("o.source"),
        col("w.temperature"),
        col("w.weather_condition"),
        col("w.wind_speed"),
        col("w.humidity")
    )

matched = streaming_weather \
    .filter(col("temperature").isNotNull()).count()
print(f"Orders with weather data: {matched}")

# COMMAND ----------

streaming_enriched = streaming_weather.alias("o") \
    .join(
        currency_df.alias("c"),
        col("o.order_date") == col("c.rate_date"),
        how="left"
    ) \
    .select(
        col("o.order_id"),
        col("o.customer_id"),
        col("o.product_id"),
        col("o.amount").alias("amount_inr"),
        spark_round(
            col("o.amount") * col("c.exchange_rate"),
            2
        ).alias("amount_usd"),
        col("o.currency"),
        col("o.city"),
        col("o.order_time"),
        col("o.order_date"),
        col("o.order_hour"),
        col("o.source"),
        col("o.temperature"),
        col("o.weather_condition"),
        col("o.wind_speed"),
        col("o.humidity"),
        col("c.exchange_rate").alias("usd_exchange_rate")
    )

print(f"Enriched streaming orders: {streaming_enriched.count()}")
print("\nSample enriched streaming orders:")
streaming_enriched.select(
    "order_id", "city", "amount_inr",
    "amount_usd", "temperature",
    "weather_condition", "order_time"
).show(5, truncate=False)

# COMMAND ----------

from delta.tables import DeltaTable

try:
    streaming_table = DeltaTable.forPath(
                        spark, silver_streaming_path)
    streaming_table.alias("existing").merge(
        streaming_enriched.alias("new"),
        "existing.order_id = new.order_id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
    print("Streaming orders merged into Silver")

except:
    streaming_enriched.write \
        .format("delta") \
        .mode("overwrite") \
        .save(silver_streaming_path)
    print("fact_streaming_orders Silver table created")

count = spark.read \
    .format("delta") \
    .load(silver_streaming_path) \
    .count()
print(f"Total in silver/fact_streaming_orders: {count}")

# COMMAND ----------

print("=" * 55)
print("STREAMING SILVER SUMMARY")
print("=" * 55)

streaming_silver = spark.read \
    .format("delta") \
    .load(silver_streaming_path)

print(f"\nTotal streaming orders : {streaming_silver.count()}")
print(f"Today's date          : {today}")

print("\nOrders by city:")
streaming_silver \
    .groupBy("city") \
    .count() \
    .orderBy(col("count").desc()) \
    .show(5, truncate=False)

print("\nRevenue so far today:")
from pyspark.sql.functions import sum as spark_sum
streaming_silver.agg(
    spark_round(
        spark_sum("amount_inr"), 2
    ).alias("total_inr"),
    spark_round(
        spark_sum("amount_usd"), 2
    ).alias("total_usd")
).show()

print("\nWeather distribution:")
streaming_silver \
    .groupBy("weather_condition") \
    .count() \
    .orderBy(col("count").desc()) \
    .show(truncate=False)

print("\nSample orders:")
streaming_silver.select(
    "order_id", "city", "amount_inr",
    "amount_usd", "temperature",
    "weather_condition", "order_time"
).show(5, truncate=False)

print("\n04_bronze_to_silver_stream complete ✓")
print("Ready to run 05_silver_to_gold_stream")