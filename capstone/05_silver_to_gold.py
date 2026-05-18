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

# silver paths
silver_customers_path = f"{base_path}/silver/dim_customers/"
silver_products_path  = f"{base_path}/silver/dim_products/"
silver_orders_path    = f"{base_path}/silver/fact_orders/"

# gold paths
gold_customers_path = f"{base_path}/gold/dim_customers/"
gold_products_path  = f"{base_path}/gold/dim_products/"
gold_date_path      = f"{base_path}/gold/dim_date/"
gold_location_path  = f"{base_path}/gold/dim_location/"
gold_orders_path    = f"{base_path}/gold/fact_orders/"

print("Paths configured")

# COMMAND ----------

from pyspark.sql.functions import (
    col, lit, current_date, current_timestamp,
    to_date, date_format, year, month, dayofmonth,
    dayofweek, quarter, weekofyear,
    monotonically_increasing_id, when,
    max as spark_max, round as spark_round
)
from delta.tables import DeltaTable

customers_df = spark.read.format("delta") \
    .load(silver_customers_path)
products_df  = spark.read.format("delta") \
    .load(silver_products_path)
orders_df    = spark.read.format("delta") \
    .load(silver_orders_path)

print("Silver tables loaded:")
print(f"  dim_customers : {customers_df.count()} rows")
print(f"  dim_products  : {products_df.count()} rows")
print(f"  fact_orders   : {orders_df.count()} rows")

# COMMAND ----------

# generate date dimension from all order dates
# contains useful date attributes for reporting

from pyspark.sql.functions import explode, sequence

# get min and max order dates
min_date = orders_df.agg(
    {"order_date": "min"}).collect()[0][0]
max_date = orders_df.agg(
    {"order_date": "max"}).collect()[0][0]

print(f"Date range: {min_date} to {max_date}")

# generate all dates between min and max
date_range = spark.sql(f"""
    SELECT explode(
        sequence(
            to_date('{min_date}'),
            to_date('{max_date}'),
            interval 1 day
        )
    ) as full_date
""")

# build date dimension with useful attributes
dim_date = date_range.select(
    date_format(col("full_date"), "yyyyMMdd")
        .cast("integer").alias("date_id"),
    col("full_date").alias("full_date"),
    date_format(col("full_date"), "yyyy-MM-dd")
        .alias("date_str"),
    year(col("full_date")).alias("year"),
    quarter(col("full_date")).alias("quarter"),
    month(col("full_date")).alias("month"),
    date_format(col("full_date"), "MMMM")
        .alias("month_name"),
    weekofyear(col("full_date")).alias("week_of_year"),
    dayofmonth(col("full_date")).alias("day_of_month"),
    dayofweek(col("full_date")).alias("day_of_week"),
    date_format(col("full_date"), "EEEE")
        .alias("day_name"),
    when(
        dayofweek(col("full_date")).isin([1, 7]),
        lit(True)
    ).otherwise(lit(False)).alias("is_weekend"),
    when(
        month(col("full_date")).isin([10, 11, 12]),
        lit(True)
    ).otherwise(lit(False)).alias("is_festive_season")
)

print(f"dim_date: {dim_date.count()} rows")
dim_date.show(5, truncate=False)

# COMMAND ----------

try:
    date_table = DeltaTable.forPath(spark, gold_date_path)
    date_table.alias("existing").merge(
        dim_date.alias("new"),
        "existing.date_id = new.date_id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
    print("dim_date merged into Gold")
except:
    dim_date.write \
        .format("delta") \
        .mode("overwrite") \
        .save(gold_date_path)
    print("dim_date Gold table created")

count = spark.read.format("delta") \
    .load(gold_date_path).count()
print(f"Total in gold/dim_date: {count} rows")

# COMMAND ----------

# location dimension from all cities in orders
dim_location = orders_df \
    .select("city") \
    .distinct() \
    .withColumn(
        "location_id",
        monotonically_increasing_id().cast("integer") + 1
    ) \
    .withColumn("country", lit("India")) \
    .select(
        col("location_id"),
        col("city"),
        col("country")
    )

print(f"dim_location: {dim_location.count()} rows")
dim_location.show(truncate=False)

# COMMAND ----------

try:
    location_table = DeltaTable.forPath(
                        spark, gold_location_path)
    location_table.alias("existing").merge(
        dim_location.alias("new"),
        "existing.city = new.city"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
    print("dim_location merged into Gold")
except:
    dim_location.write \
        .format("delta") \
        .mode("overwrite") \
        .save(gold_location_path)
    print("dim_location Gold table created")

count = spark.read.format("delta") \
    .load(gold_location_path).count()
print(f"Total in gold/dim_location: {count} rows")

# COMMAND ----------

# products don't change often
# use SCD Type 1 (simple overwrite)

dim_products_gold = products_df.select(
    col("product_id"),
    col("product_name"),
    col("category"),
    col("price"),
    current_timestamp().alias("updated_at")
)

print(f"dim_products: {dim_products_gold.count()} rows")
dim_products_gold.show(5, truncate=False)

# COMMAND ----------

try:
    products_table = DeltaTable.forPath(
                        spark, gold_products_path)
    products_table.alias("existing").merge(
        dim_products_gold.alias("new"),
        "existing.product_id = new.product_id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
    print("dim_products merged into Gold")
except:
    dim_products_gold.write \
        .format("delta") \
        .mode("overwrite") \
        .save(gold_products_path)
    print("dim_products Gold table created")

count = spark.read.format("delta") \
    .load(gold_products_path).count()
print(f"Total in gold/dim_products: {count} rows")

# COMMAND ----------

# SCD Type 2 tracks customer history
# when customer data changes:
# → old record gets end_date and is_current=False
# → new record inserted with is_current=True

# prepare new customer data
new_customers = customers_df.select(
    col("customer_id"),
    col("name"),
    col("email"),
    col("phone"),
    col("city"),
    col("country"),
    col("signup_date"),
    current_date().alias("start_date"),
    lit(None).cast("date").alias("end_date"),
    lit(True).alias("is_current")
)

print(f"New customer records: {new_customers.count()}")
new_customers.show(3, truncate=False)

# COMMAND ----------

try:
    # gold dim_customers exists
    # apply SCD Type 2

    gold_customers = DeltaTable.forPath(
                        spark, gold_customers_path)

    # step 1 → expire records that have changed
    # find customers whose city changed
    gold_customers.alias("existing").merge(
        new_customers.alias("new"),
        """existing.customer_id = new.customer_id
           AND existing.is_current = true
           AND existing.city != new.city"""
    ).whenMatchedUpdate(set={
        "is_current": lit(False),
        "end_date":   current_date()
    }).execute()

    print("Step 1 done: expired changed records")

    # step 2 → insert new versions of changed records
    # and insert brand new customers
    gold_customers.alias("existing").merge(
        new_customers.alias("new"),
        """existing.customer_id = new.customer_id
           AND existing.is_current = true"""
    ).whenNotMatchedInsertAll() \
     .execute()

    print("Step 2 done: inserted new records")

except:
    # first time → create table
    new_customers.write \
        .format("delta") \
        .mode("overwrite") \
        .save(gold_customers_path)
    print("dim_customers Gold table created (first time)")

count = spark.read.format("delta") \
    .load(gold_customers_path).count()
current_count = spark.read.format("delta") \
    .load(gold_customers_path) \
    .filter(col("is_current") == True).count()

print(f"Total records    : {count}")
print(f"Current records  : {current_count}")
print(f"Historical records: {count - current_count}")

# COMMAND ----------

# join orders with dimension tables
# to get surrogate keys and attributes

# read gold dimensions
gold_date     = spark.read.format("delta") \
    .load(gold_date_path)
gold_location = spark.read.format("delta") \
    .load(gold_location_path)

# build fact orders with dimension keys
fact_orders_gold = orders_df.alias("o") \
    .join(
        gold_date.alias("d"),
        col("o.order_date") == col("d.date_str"),
        how="left"
    ) \
    .join(
        gold_location.alias("l"),
        col("o.city") == col("l.city"),
        how="left"
    ) \
    .select(
        col("o.order_id"),
        col("o.customer_id"),
        col("o.product_id"),
        col("d.date_id"),
        col("l.location_id"),
        col("o.amount_inr"),
        col("o.amount_usd"),
        col("o.currency"),
        col("o.temperature"),
        col("o.weather_condition"),
        col("o.wind_speed"),
        col("o.humidity"),
        col("o.usd_exchange_rate"),
        col("o.order_time"),
        col("o.order_date"),
        col("o.source")
    )

print(f"fact_orders Gold: {fact_orders_gold.count()} rows")
fact_orders_gold.show(5, truncate=False)

# COMMAND ----------

try:
    orders_table = DeltaTable.forPath(
                    spark, gold_orders_path)
    orders_table.alias("existing").merge(
        fact_orders_gold.alias("new"),
        "existing.order_id = new.order_id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
    print("fact_orders merged into Gold")
except:
    fact_orders_gold.write \
        .format("delta") \
        .mode("overwrite") \
        .save(gold_orders_path)
    print("fact_orders Gold table created")

count = spark.read.format("delta") \
    .load(gold_orders_path).count()
print(f"Total in gold/fact_orders: {count} rows")

# COMMAND ----------

from pyspark.sql.functions import sum as spark_sum

# COMMAND ----------

print("=" * 55)
print("GOLD LAYER SUMMARY")
print("=" * 55)

dim_c  = spark.read.format("delta").load(gold_customers_path)
dim_p  = spark.read.format("delta").load(gold_products_path)
dim_d  = spark.read.format("delta").load(gold_date_path)
dim_l  = spark.read.format("delta").load(gold_location_path)
fact_o = spark.read.format("delta").load(gold_orders_path)

print(f"\ngold/dim_customers : {dim_c.count()} rows")
print(f"gold/dim_products  : {dim_p.count()} rows")
print(f"gold/dim_date      : {dim_d.count()} rows")
print(f"gold/dim_location  : {dim_l.count()} rows")
print(f"gold/fact_orders   : {fact_o.count()} rows")

print("\nSCD Type 2 verification:")
print(f"  Current customers  : "
      f"{dim_c.filter(col('is_current')==True).count()}")
print(f"  Historical records : "
      f"{dim_c.filter(col('is_current')==False).count()}")

print("\nSample fact_orders:")
fact_o.select(
    "order_id", "customer_id",
    "product_id", "date_id",
    "location_id", "amount_inr",
    "amount_usd", "weather_condition",
    "source"
).show(5, truncate=False)

print("\nRevenue by category (joining Gold tables):")
fact_o.alias("f") \
    .join(
        dim_p.alias("p"),
        col("f.product_id") == col("p.product_id")
    ) \
    .groupBy("p.category") \
    .agg(
        spark_round(
            spark_sum(col("f.amount_inr")) / 1000, 2
        ).alias("revenue_inr_thousands"),
        spark_round(
            spark_sum(col("f.amount_usd")), 2
        ).alias("revenue_usd")
    ) \
    .orderBy(col("revenue_inr_thousands").desc()) \
    .show(truncate=False)

print("\nOrders by source:")
fact_o.groupBy("source").count().show()

print("\n05_silver_to_gold complete ✓")
print("Gold layer ready for Power BI!")