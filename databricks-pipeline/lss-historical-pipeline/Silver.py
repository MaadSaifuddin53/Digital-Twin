# Databricks notebook source
secret_password = dbutils.secrets.get(scope="key-vault-scope", key="lss-password")



# COMMAND ----------


import os

spark.conf.set("fs.azure.account.auth.type.datalaketest010.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.datalaketest010.dfs.core.windows.net", 
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.datalaketest010.dfs.core.windows.net", os.environ.get("AZURE_CLIENT_ID"))
spark.conf.set("fs.azure.account.oauth2.client.secret.datalaketest010.dfs.core.windows.net", os.environ.get("AZURE_CLIENT_SECRET"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint.datalaketest010.dfs.core.windows.net", os.environ.get("AZURE_TENANT_ENDPOINT"))

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

df = spark.read.format("delta").load("abfss://bronze@datalaketest010.dfs.core.windows.net/bronze")
df.show()


# COMMAND ----------

from pyspark.sql.functions import expr
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Initialize Spark session
spark = SparkSession.builder.appName("DeltaExample").getOrCreate()

# Define the schema with `id` as StringType for UUID
silver_schema = StructType([
    StructField("id", StringType(), True),  # UUID as StringType
    StructField("sn", StringType(), True),
    StructField("datetime", TimestampType(), True),
    StructField("datetime_gmt", TimestampType(), True),
    StructField("metrics_name", StringType(), True),
    StructField("metrics_value", StringType(), True)
])

# Create an empty DataFrame with the schema
df = spark.createDataFrame([], schema=silver_schema)

# Save as a managed Delta table in the specified catalog and schema
df.withColumn("id", expr("uuid()")).write.format("delta").saveAsTable("`dev-silver`.default.lss_silver")

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

from pyspark.sql import SparkSession

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import col, explode, map_entries, monotonically_increasing_id, expr

class SilverJob:
    def __init__(self, spark: SparkSession, bronze_table: str, silver_table_path: str):
        self.spark = spark
        self.bronze_table = bronze_table
        self.silver_table_path = silver_table_path

    def run(self):
        # Read the Bronze Delta Table as a Streaming Source
        df_bronze = self.spark.readStream.format("delta").table(self.bronze_table)


        # Explode the metrics dictionary to create separate rows
        df_silver = df_bronze \
            .withColumn("metrics_exploded", explode(map_entries(col("metrics")))) \
            .withColumn("metrics_name", col("metrics_exploded.key")) \
            .withColumn("metrics_value", col("metrics_exploded.value")) \
            .withColumn("id", expr("uuid()")) \
            .select(
                col("id"),
                col("sn"),
                col("datetime").cast(TimestampType()),
                col("datetime_gmt").cast(TimestampType()),
                col("metrics_name"),
                col("metrics_value")
            )

        df_silver.writeStream \
            .format("delta") \
            .option("checkpointLocation", f"{self.silver_table_path}/_checkpoint2") \
            .outputMode("append") \
            .trigger(processingTime="1 minute") \
            .toTable("`dev-silver`.default.lss_silver")

        # Wait for the termination of the stream
       # df_silver.awaitTermination()

     

# Usage Example:
if __name__ == "__main__":
    spark = SparkSession.builder.appName("SilverIngestion").getOrCreate()

    # Define Azure Data Lake Gen2 Path for Silver Table
    silver_table_path = "abfss://silver@datalaketest010.dfs.core.windows.net"

    job = SilverJob(
        spark,
        bronze_table="`dev-bronze`.default.lss_bronze",
        silver_table_path=silver_table_path
    )
    job.run()

# COMMAND ----------

