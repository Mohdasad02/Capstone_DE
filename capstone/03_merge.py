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

batch_orders_path     = f"{base_path}/bronze/delta/orders/"
streaming_orders_path = f"{base_path}/bronze/delta/streaming_orders/"

print("Paths configured")

# COMMAND ----------

from pyspark.sql.functions import (
    col, lit, current_timestamp,
    to_date, date_format, hour
)
from datetime import date, timedelta

# read all streaming orders
streaming_df = spark.read \
    .format("delta") \
    .load(streaming_orders_path)

total_streaming = streaming_df.count()
print(f"Total streaming orders in buffer: {total_streaming}")

if total_streaming == 0:
    print("No streaming orders to merge. Exiting.")
    dbutils.notebook.exit("No streaming orders")

# show breakdown by date
print("\nStreaming orders by date:")
streaming_df \
    .withColumn("date", to_date(col("timestamp"))) \
    .groupBy("date") \
    .count() \
    .orderBy("date") \
    .show()

print("Sample streaming orders:")
streaming_df.select(
    "order_id", "customer_id",
    "product_id", "amount",
    "city", "timestamp"
).show(5, truncate=False)

# COMMAND ----------

# streaming has 'timestamp' column
# batch has 'order_time' column
# align before merging

streaming_aligned = streaming_df \
    .withColumnRenamed("timestamp", "order_time") \
    .withColumn("source", lit("streaming")) \
    .select(
        col("order_id"),
        col("customer_id"),
        col("product_id"),
        col("amount"),
        col("currency"),
        col("order_time"),
        col("city"),
        col("ingested_at"),
        col("source")
    )

print("Schema aligned")
print(f"Records to merge: {streaming_aligned.count()}")
streaming_aligned.printSchema()

# COMMAND ----------

# check if any streaming order_ids
# already exist in batch orders
# this prevents duplicates

batch_df = spark.read \
    .format("delta") \
    .load(batch_orders_path)

batch_ids     = batch_df.select("order_id")
streaming_ids = streaming_aligned.select("order_id")

duplicates = streaming_ids.intersect(batch_ids).count()
new_orders = streaming_ids.subtract(batch_ids).count()

print(f"Batch orders currently   : {batch_df.count()}")
print(f"Streaming orders total   : {streaming_aligned.count()}")
print(f"Duplicates found         : {duplicates} (will be skipped)")
print(f"New orders to insert     : {new_orders}")

# COMMAND ----------

from delta.tables import DeltaTable

print("Starting merge...")

batch_table = DeltaTable.forPath(spark, batch_orders_path)

batch_table.alias("batch").merge(
    streaming_aligned.alias("stream"),
    # match on order_id to prevent duplicates
    "batch.order_id = stream.order_id"
) \
.whenNotMatchedInsertAll() \
.execute()

# verify
merged_count = spark.read \
    .format("delta") \
    .load(batch_orders_path) \
    .count()

print(f"\nMerge complete")
print(f"Before : {batch_df.count()} orders")
print(f"Added  : {new_orders} streaming orders")
print(f"After  : {merged_count} total orders")

# show source breakdown
print("\nOrders by source:")
spark.read.format("delta") \
    .load(batch_orders_path) \
    .groupBy("source") \
    .count() \
    .show()

# COMMAND ----------

# after merging streaming orders into batch
# we clear the streaming orders table
# so tomorrow's streaming starts fresh
# and dashboard 2 shows only today's orders

print("Clearing streaming orders buffer...")

# get count before clearing
before_count = spark.read \
    .format("delta") \
    .load(streaming_orders_path) \
    .count()

# overwrite with empty dataframe
# keeping same schema
empty_df = spark.read \
    .format("delta") \
    .load(streaming_orders_path) \
    .limit(0)  # zero rows same schema

empty_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "false") \
    .save(streaming_orders_path)

after_count = spark.read \
    .format("delta") \
    .load(streaming_orders_path) \
    .count()

print(f"Streaming orders before clear : {before_count}")
print(f"Streaming orders after clear  : {after_count}")
print("Streaming buffer ready for tomorrow")

# COMMAND ----------

print("=" * 55)
print("MERGE SUMMARY")
print("=" * 55)

final_batch = spark.read \
    .format("delta") \
    .load(batch_orders_path)

print(f"\nTotal orders in Bronze batch : {final_batch.count()}")

print("\nBreakdown by source:")
final_batch.groupBy("source").count().show()

print("Breakdown by date (last 5 dates):")
final_batch \
    .withColumn(
        "order_date",
        date_format(col("order_time"), "yyyy-MM-dd")
    ) \
    .groupBy("order_date") \
    .count() \
    .orderBy(col("order_date").desc()) \
    .show(5)

print("\nStreaming buffer status:")
buffer_count = spark.read \
    .format("delta") \
    .load(streaming_orders_path) \
    .count()
print(f"Orders in buffer: {buffer_count} (should be 0)")
print("\n03_merge complete ✓")
print("Ready to run 04_bronze_to_silver")