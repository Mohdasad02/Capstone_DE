# Databricks notebook source

# replace these two values with yours
storage_account_name = "asadretaildatalake"
storage_account_key  = "TdocxHu1Ozw/co0Hk0GurCJ23BHjG9SYZIhT1hdcLOYZ0NU3y7fBNdd7agg5NLPe0u3tu6JHqoOK+AStG0qK9g=="

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

print("ADLS connection configured")

# COMMAND ----------

base_path = f"abfss://datalake@{storage_account_name}.dfs.core.windows.net"

# bronze source paths (where your CSVs are)
customers_csv_path = f"{base_path}/bronze/customers/"
products_csv_path  = f"{base_path}/bronze/products/"
orders_csv_path    = f"{base_path}/bronze/orders/"

# bronze delta output paths (where we write Delta tables)
customers_delta_path = f"{base_path}/bronze/delta/customers/"
products_delta_path  = f"{base_path}/bronze/delta/products/"
orders_delta_path    = f"{base_path}/bronze/delta/orders/"

print("Paths configured")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# --- CUSTOMERS ---
customers_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(customers_csv_path)

customers_df = customers_df.withColumn("ingested_at", current_timestamp()) \
                           .withColumn("source", lit("batch_csv"))

customers_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(customers_delta_path)

print(f"Customers loaded: {customers_df.count()} rows")

# --- PRODUCTS ---
products_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(products_csv_path)

products_df = products_df.withColumn("ingested_at", current_timestamp()) \
                         .withColumn("source", lit("batch_csv"))

products_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(products_delta_path)

print(f"Products loaded: {products_df.count()} rows")

# --- ORDERS ---
orders_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(orders_csv_path)

orders_df = orders_df.withColumn("ingested_at", current_timestamp()) \
                     .withColumn("source", lit("batch_csv"))

orders_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(orders_delta_path)

print(f"Orders loaded: {orders_df.count()} rows")

# COMMAND ----------

display(customers_df)

# COMMAND ----------

print("=== CUSTOMERS ===")
spark.read.format("delta").load(customers_delta_path).printSchema()
spark.read.format("delta").load(customers_delta_path).show(3)


# COMMAND ----------

print("=== PRODUCTS ===")
spark.read.format("delta").load(products_delta_path).printSchema()
spark.read.format("delta").load(products_delta_path).show(3)


# COMMAND ----------

print("=== ORDERS ===")
spark.read.format("delta").load(orders_delta_path).printSchema()
spark.read.format("delta").load(orders_delta_path).show(3)

# COMMAND ----------

# Streaming data 

