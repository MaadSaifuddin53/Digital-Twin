# Databricks notebook source
secret_password = dbutils.secrets.get(scope="key-vault-scope", key="lss-password")



# COMMAND ----------

print('hello')

import os

spark.conf.set("fs.azure.account.auth.type.datalaketest010.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.datalaketest010.dfs.core.windows.net", 
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.datalaketest010.dfs.core.windows.net", os.environ.get("AZURE_CLIENT_ID"))
spark.conf.set("fs.azure.account.oauth2.client.secret.datalaketest010.dfs.core.windows.net", os.environ.get("AZURE_CLIENT_SECRET"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint.datalaketest010.dfs.core.windows.net", os.environ.get("AZURE_TENANT_ENDPOINT"))

# COMMAND ----------



# COMMAND ----------

df = spark.read.format("delta").load("abfss://bronze@datalaketest010.dfs.core.windows.net/bronze")
df.show()


# COMMAND ----------

# %sql
# CREATE DATABASE IF NOT EXISTS bronze_db;
# USE bronze_db;

# CREATE TABLE IF NOT EXISTS bronze_table
# USING DELTA
# LOCATION 'abfss://bronze@datalaketest010.dfs.core.windows.net/bronze';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM `dev-bronze`.default.lss_bronze
# MAGIC WHERE datetime_gmt >= '2025-03-05T16:44:45Z'
# MAGIC AND datetime_gmt <= '2025-03-05T18:44:45Z';

# COMMAND ----------

# Initialize Spark session
spark = SparkSession.builder.appName("DeltaExample").getOrCreate()

# Define the schema
raw_schema = StructType([
    StructField("sn", StringType(), True),
    StructField("datetime", StringType(), True),
    StructField("datetime_gmt", StringType(), True),
    StructField("metrics", MapType(StringType(), StringType()), True)  # Metrics as a dictionary
])

# Create an empty DataFrame with the schema
df = spark.createDataFrame([], schema=raw_schema)

# Save as a managed Delta table in the specified catalog and schema
df.write.format("delta").saveAsTable("`dev-bronze`.default.lss_bronze")

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

from pyspark.sql import SparkSession

from pyspark.sql.functions import from_json, col, explode, monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, MapType

class BronzeJob:
    def __init__(self, spark: SparkSession, kafka_topic: str, kafka_broker: str, bronze_table_path: str):
        self.spark = spark
        self.kafka_topic = kafka_topic
        self.kafka_broker = kafka_broker
        self.bronze_table_path = bronze_table_path

        self.raw_schema = StructType([
            StructField("sn", StringType(), True),
            StructField("datetime", StringType(), True),
            StructField("datetime_gmt", StringType(), True),
            StructField("metrics", MapType(StringType(), StringType()), True)  # Metrics as a dictionary
        ])


        # Define Schema for Incoming Data
        self.schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("sn", StringType(), True),
            StructField("datetime", TimestampType(), True),
            StructField("datetime_gmt", TimestampType(), True),
            StructField("metrics_name", StringType(), True),
            StructField("metrics_value", StringType(), True)
        ])

    def run(self):
        # Read data from Kafka
        
            # Define Kafka Bootstrap Servers
        kafka_bootstrap_servers = "eventhub-test-lss.servicebus.windows.net:9093"

        # Use the connection string as the password
        

        event_hub_connection_string = os.getenv("EVENT_HUB_CONNECTION_STRING")


        # Kafka SASL configuration
        sasl_config = f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{event_hub_connection_string}";'

        # Spark Kafka options
        kafka_options = {
            "kafka.bootstrap.servers": kafka_bootstrap_servers,
            "subscribe": "sensortopic",
            "kafka.sasl.mechanism": "PLAIN",
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.jaas.config": sasl_config,
            "startingOffsets": "earliest",
            "kafka.group.id": "databricks",  # Define your Kafka consumer group
            "kafka.failOnDataLoss": "false",  # Prevent Spark from failing on missing offsets

        }

        # Read stream
        df = spark.readStream.format("kafka").option("failOnDataLoss", "false").options(**kafka_options).load()
                # Parse JSON from Kafka messages
        df_parsed = df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), self.raw_schema).alias("data")) \
            .select("data.*")

    

        df_parsed.writeStream \
        .format("delta") \
        .option("checkpointLocation", f"{self.bronze_table_path}/_checkpoint") \
        .outputMode("append") \
        .toTable("`dev-bronze`.default.lss_bronze")


# Usage Example:
if __name__ == "__main__":
    spark = SparkSession.builder.appName("BronzeIngestion").getOrCreate()
    
    # Define Azure Data Lake Gen2 Path
    bronze_table_path = "abfss://bronze@datalaketest010.dfs.core.windows.net/bronze"

    job = BronzeJob(
        spark,
        kafka_topic="sensortopic",
        kafka_broker="eventhub-test-lss.servicebus.windows.net:9093",
        bronze_table_path=bronze_table_path
    )
    job.run()

# COMMAND ----------

!databricks secrets create-scope --scope databricks-eventhub-secrets

# COMMAND ----------

