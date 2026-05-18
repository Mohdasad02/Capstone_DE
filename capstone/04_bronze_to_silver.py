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
bronze_orders_path    = f"{base_path}/bronze/delta/orders/"
bronze_customers_path = f"{base_path}/bronze/delta/customers/"
bronze_products_path  = f"{base_path}/bronze/delta/products/"
bronze_weather_path   = f"{base_path}/bronze/delta/weather/"
bronze_currency_path  = f"{base_path}/bronze/delta/currency/"

# silver paths
silver_customers_path = f"{base_path}/silver/dim_customers/"
silver_products_path  = f"{base_path}/silver/dim_products/"
silver_orders_path    = f"{base_path}/silver/fact_orders/"

print("Paths configured")

# COMMAND ----------

# MAGIC
# MAGIC %run /Workspace/Users/2211301121@stu.manit.ac.in/capstone/utils/pii_masking   
# MAGIC
# MAGIC

# COMMAND ----------

# test that masking functions are available
from pyspark.sql import Row

test_df = spark.createDataFrame([
    Row(name="Ankit Gupta", email="ankit@gmail.com", phone="9876543210")
])

result = mask_all_pii(test_df)
result.show(truncate=False)

# COMMAND ----------

# MAGIC
# MAGIC %run /Workspace/Users/2211301121@stu.manit.ac.in/capstone/utils/api_enrichment
# MAGIC

# COMMAND ----------

print("API enrichment functions ready")

# COMMAND ----------

from pyspark.sql.functions import (
    col, to_date, hour, date_format,
    when, lit, round as spark_round,
    trim, upper, initcap, regexp_replace
)

# read bronze tables
orders_df    = spark.read.format("delta").load(bronze_orders_path)
customers_df = spark.read.format("delta").load(bronze_customers_path)
products_df  = spark.read.format("delta").load(bronze_products_path)
weather_df   = spark.read.format("delta").load(bronze_weather_path)
currency_df  = spark.read.format("delta").load(bronze_currency_path)

print("Bronze tables loaded:")
print(f"  orders    : {orders_df.count()} rows")
print(f"  customers : {customers_df.count()} rows")
print(f"  products  : {products_df.count()} rows")
print(f"  weather   : {weather_df.count()} rows")
print(f"  currency  : {currency_df.count()} rows")

# COMMAND ----------

# verify enrichment data exists before running
from pyspark.sql.functions import date_format, col, hour

print("Checking enrichment data in Bronze...")

weather_count  = spark.read.format("delta") \
    .load(bronze_weather_path).count()
currency_count = spark.read.format("delta") \
    .load(bronze_currency_path).count()

print(f"Weather records  : {weather_count}")
print(f"Currency records : {currency_count}")

if weather_count == 0 or currency_count == 0:
    print("\nEnrichment data missing → running API enrichment...")
    run_api_enrichment(orders_df, base_path)
else:
    print("\nAll enrichment data already in Bronze ✓")
    print("Skipping API calls")
    print("Proceeding to Silver transformation...")

# COMMAND ----------

# clean customers
# apply PII masking
# standardize columns

dim_customers = customers_df \
    .select(
        col("customer_id").cast("integer"),
        trim(initcap(col("name"))).alias("name"),
        trim(col("email")).alias("email"),
        col("phone").cast("string"),
        trim(initcap(col("city"))).alias("city"),
        trim(col("country")).alias("country"),
        to_date(col("signup_date")).alias("signup_date")
    ) \
    .dropDuplicates(["customer_id"]) \
    .filter(col("customer_id").isNotNull()) \
    .filter(col("email").isNotNull())

# apply PII masking
dim_customers = mask_all_pii(dim_customers)

print(f"dim_customers: {dim_customers.count()} rows")
dim_customers.show(3, truncate=False)

# COMMAND ----------

from delta.tables import DeltaTable

try:
    # table exists → merge on customer_id
    customers_table = DeltaTable.forPath(
                        spark, silver_customers_path)
    customers_table.alias("existing").merge(
        dim_customers.alias("new"),
        "existing.customer_id = new.customer_id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
    print("dim_customers merged into Silver")

except:
    # first time → create table
    dim_customers.write \
        .format("delta") \
        .mode("overwrite") \
        .save(silver_customers_path)
    print("dim_customers Silver table created")

count = spark.read \
    .format("delta") \
    .load(silver_customers_path) \
    .count()
print(f"Total in silver/dim_customers: {count} rows")

# COMMAND ----------

dim_products = products_df \
    .select(
        col("product_id").cast("integer"),
        trim(col("product_name")).alias("product_name"),
        trim(col("category")).alias("category"),
        col("price").cast("double")
    ) \
    .dropDuplicates(["product_id"]) \
    .filter(col("product_id").isNotNull()) \
    .filter(col("price") > 0)

print(f"dim_products: {dim_products.count()} rows")
dim_products.show(3, truncate=False)

# save to silver
try:
    products_table = DeltaTable.forPath(
                        spark, silver_products_path)
    products_table.alias("existing").merge(
        dim_products.alias("new"),
        "existing.product_id = new.product_id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
    print("dim_products merged into Silver")

except:
    dim_products.write \
        .format("delta") \
        .mode("overwrite") \
        .save(silver_products_path)
    print("dim_products Silver table created")

count = spark.read \
    .format("delta") \
    .load(silver_products_path) \
    .count()
print(f"Total in silver/dim_products: {count} rows")

# COMMAND ----------

# extract date and hour from order_time
# needed for joining with weather table

orders_prepared = orders_df \
    .withColumn(
        "order_date",
        date_format(col("order_time"), "yyyy-MM-dd")
    ) \
    .withColumn(
        "order_hour",
        hour(col("order_time"))
    ) \
    .filter(col("order_id").isNotNull()) \
    .filter(col("amount") > 0) \
    .dropDuplicates(["order_id"])

print(f"Orders prepared: {orders_prepared.count()} rows")
orders_prepared.select(
    "order_id", "city",
    "order_time", "order_date", "order_hour"
).show(3, truncate=False)

# COMMAND ----------

# join orders with weather on city+date+hour
# every order gets weather at exact time placed

fact_orders_weather = orders_prepared.alias("o") \
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

print(f"Orders after weather join: {fact_orders_weather.count()}")

# check how many got weather data
matched = fact_orders_weather \
    .filter(col("temperature").isNotNull()) \
    .count()
unmatched = fact_orders_weather \
    .filter(col("temperature").isNull()) \
    .count()

print(f"  With weather data    : {matched}")
print(f"  Without weather data : {unmatched}")

# COMMAND ----------

# join orders with currency on order_date
# each order gets exact USD rate on that date

fact_orders_enriched = fact_orders_weather.alias("o") \
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

print(f"Orders after currency join: {fact_orders_enriched.count()}")
print("\nSample enriched orders:")
fact_orders_enriched.select(
    "order_id", "city", "amount_inr",
    "amount_usd", "temperature",
    "weather_condition", "order_date"
).show(5, truncate=False)

# COMMAND ----------

try:
    orders_table = DeltaTable.forPath(
                    spark, silver_orders_path)
    orders_table.alias("existing").merge(
        fact_orders_enriched.alias("new"),
        "existing.order_id = new.order_id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
    print("fact_orders merged into Silver")

except:
    fact_orders_enriched.write \
        .format("delta") \
        .mode("overwrite") \
        .save(silver_orders_path)
    print("fact_orders Silver table created")

count = spark.read \
    .format("delta") \
    .load(silver_orders_path) \
    .count()
print(f"Total in silver/fact_orders: {count} rows")

# COMMAND ----------

print("=" * 55)
print("SILVER LAYER SUMMARY")
print("=" * 55)

dim_c = spark.read.format("delta").load(silver_customers_path)
dim_p = spark.read.format("delta").load(silver_products_path)
fact  = spark.read.format("delta").load(silver_orders_path)

print(f"\nsilver/dim_customers : {dim_c.count()} rows")
print(f"silver/dim_products  : {dim_p.count()} rows")
print(f"silver/fact_orders   : {fact.count()} rows")

print("\nSample fact_orders:")
fact.select(
    "order_id", "customer_id", "city",
    "amount_inr", "amount_usd",
    "temperature", "weather_condition",
    "order_date", "source"
).show(5, truncate=False)

print("\nPII masking verification:")
dim_c.select("customer_id", "name", "email", "phone") \
     .show(3, truncate=False)

print("\nOrders by source:")
fact.groupBy("source").count().show()

print("\n04_bronze_to_silver complete ✓")
print("Ready to run 05_silver_to_gold")