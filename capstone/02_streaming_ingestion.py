# Databricks notebook source
# run this cell first and wait for it to finish
%pip install azure-eventhub

# COMMAND ----------

# connecting to adls

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

print("ADLS connected")

# COMMAND ----------


# use the EVENT HUB LEVEL connection string
# the one with EntityPath=orders-stream at the end
EH_CONNECTION_STRING = "Endpoint=sb://retail-eventhubs.servicebus.windows.net/;SharedAccessKeyName=orders-stream-policy;SharedAccessKey=ZlE71gfGANsMDWna9+V5rQLEsoRCy0LFN+AEhPDlrUY=;EntityPath=orders-stream"

ehConf = {
    'eventhubs.connectionString': sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(EH_CONNECTION_STRING)
}

print("Event Hubs config ready")


# # your event hubs connection string
# CONNECTION_STRING = "Endpoint=sb://retail-eventhubs.servicebus.windows.net/;SharedAccessKeyName=orders-stream-policy;SharedAccessKey=ZlE71gfGANsMDWna9+V5rQLEsoRCy0LFN+AEhPDlrUY=;EntityPath=orders-stream"
# EVENTHUB_NAME     = "orders-stream"

# # build the connection string format that Spark needs
# EH_SASL = (
#     'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule '
#     'required username="$ConnectionString" '
#     f'password="{CONNECTION_STRING}";'
# )

# # event hubs namespace (just the hostname part)
# # format: your-namespace.servicebus.windows.net:9093
# EH_NAMESPACE = "retail-eventhubs.servicebus.windows.net:9093"

# print("Event Hubs config ready")

# COMMAND ----------

print(repr(EH_SASL))

# COMMAND ----------

base_path = f"abfss://datalake@{storage_account_name}.dfs.core.windows.net"

streaming_output_path     = f"{base_path}/bronze/delta/streaming_orders/"
streaming_checkpoint_path = f"{base_path}/bronze/checkpoints/streaming_orders/"

print("Paths configured")

# base_path = f"abfss://datalake@{storage_account_name}.dfs.core.windows.net"

# # where streaming orders will land in Bronze
# streaming_output_path     = f"{base_path}/bronze/delta/streaming_orders/"
# streaming_checkpoint_path = f"{base_path}/bronze/checkpoints/streaming_orders/"

# print(f"Output path: {streaming_output_path}")

# COMMAND ----------


streaming_df_raw = spark.readStream \
    .format("eventhubs") \
    .options(**ehConf) \
    .load()

print("Stream reader configured")
streaming_df_raw.printSchema()

# # Event Hubs supports Kafka protocol
# # so Spark can read it like a Kafka stream

# streaming_df_raw = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", EH_NAMESPACE) \
#     .option("kafka.security.protocol", "SASL_SSL") \
#     .option("kafka.sasl.mechanism", "PLAIN") \
#     .option("kafka.sasl.jaas.config", EH_SASL) \
#     .option("subscribe", EVENTHUB_NAME) \
#     .option("startingOffsets", "latest") \
#     .option("kafka.request.timeout.ms", "120000") \
#     .option("kafka.session.timeout.ms", "60000") \
#     .option("kafka.metadata.max.age.ms", "60000") \
#     .option("kafka.connections.max.idle.ms", "180000") \
#     .option("kafka.reconnect.backoff.ms", "1000") \
#     .option("kafka.reconnect.backoff.max.ms", "10000") \
#     .option("failOnDataLoss", "false") \
#     .load()

# print("Stream reader configured")

# COMMAND ----------


from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType

order_schema = StructType([
    StructField("order_id",    IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("product_id",  IntegerType(), True),
    StructField("amount",      DoubleType(),  True),
    StructField("currency",    StringType(),  True),
    StructField("city",        StringType(),  True),
    StructField("timestamp",   StringType(),  True)
])

streaming_df_parsed = streaming_df_raw \
    .select(
        from_json(
            col("body").cast("string"),
            order_schema
        ).alias("data")
    ) \
    .select("data.*") \
    .withColumn("ingested_at", current_timestamp())

print("Schema parsed")
streaming_df_parsed.printSchema()

# from pyspark.sql.functions import col, from_json, current_timestamp
# from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType

# # define the schema of your order events
# order_schema = StructType([
#     StructField("order_id",    IntegerType(), True),
#     StructField("customer_id", IntegerType(), True),
#     StructField("product_id",  IntegerType(), True),
#     StructField("amount",      DoubleType(),  True),
#     StructField("currency",    StringType(),  True),
#     StructField("city",        StringType(),  True),
#     StructField("timestamp",   StringType(),  True)
# ])

# # Event Hubs sends data as binary → cast to string → parse JSON
# streaming_df_parsed = streaming_df_raw \
#     .select(
#         from_json(
#             col("value").cast("string"),
#             order_schema
#         ).alias("data")
#     ) \
#     .select("data.*") \
#     .withColumn("ingested_at", current_timestamp()) \
#     .withColumn("source", col("city").cast("string"))

# print("Schema defined and stream parsed")
# streaming_df_parsed.printSchema()

# COMMAND ----------

streaming_query = streaming_df_parsed.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", streaming_checkpoint_path) \
    .trigger(processingTime="30 seconds") \
    .start(streaming_output_path)

print("Streaming job started...")
print("Check ADLS path:", streaming_output_path)

streaming_query.awaitTermination()

# # this cell starts the continuous streaming job
# # it will keep running until you stop it manually

# streaming_query = streaming_df_parsed.writeStream \
#     .format("delta") \
#     .outputMode("append") \
#     .option("checkpointLocation", streaming_checkpoint_path) \
#     .trigger(processingTime="30 seconds") \
#     .start(streaming_output_path)

# print("Streaming job started...")
# print("Events flowing from Event Hubs → ADLS Bronze")
# print("Check ADLS path:", streaming_output_path)

# # this keeps the stream running
# streaming_query.awaitTermination()

# COMMAND ----------

# run this in a new notebook to verify
base_path = (
    "abfss://datalake@asadretaildatalake"
    ".dfs.core.windows.net"
)

streaming_path = f"{base_path}/bronze/delta/streaming_orders/"

count = spark.read.format("delta") \
    .load(streaming_path).count()
print(f"Streaming orders in buffer: {count}")