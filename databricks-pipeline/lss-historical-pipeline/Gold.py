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

# MAGIC %sql
# MAGIC
# MAGIC -- Create Daily Aggregation Table
# MAGIC CREATE TABLE IF NOT EXISTS `dev-gold`.default.lss_gold_daily (
# MAGIC     time_period STRING,
# MAGIC     metrics_name STRING,
# MAGIC     avg_metrics_value DOUBLE,
# MAGIC     sum_metrics_value INT,
# MAGIC     count_metrics_value INT
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC -- Create Weekly Aggregation Table
# MAGIC CREATE TABLE IF NOT EXISTS `dev-gold`.default.lss_gold_weekly (
# MAGIC     time_period STRING,
# MAGIC     metrics_name STRING,
# MAGIC     avg_metrics_value DOUBLE,
# MAGIC     sum_metrics_value INT,
# MAGIC     count_metrics_value INT
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC -- Create Monthly Aggregation Table
# MAGIC CREATE TABLE IF NOT EXISTS `dev-gold`.default.lss_gold_monthly (
# MAGIC     time_period STRING,
# MAGIC     metrics_name STRING,
# MAGIC     avg_metrics_value DOUBLE,
# MAGIC     sum_metrics_value INT,
# MAGIC     count_metrics_value INT
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC -- Create Monthly Aggregation Table
# MAGIC CREATE TABLE IF NOT EXISTS `dev-gold`.default.lss_gold_hourly (
# MAGIC     time_period STRING,
# MAGIC     metrics_name STRING,
# MAGIC     avg_metrics_value DOUBLE,
# MAGIC     sum_metrics_value INT,
# MAGIC     count_metrics_value INT
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE `dev-gold`.default.lss_gold_monthly
# MAGIC SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
# MAGIC
# MAGIC ALTER TABLE `dev-gold`.default.lss_gold_weekly
# MAGIC SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
# MAGIC
# MAGIC ALTER TABLE `dev-gold`.default.lss_gold_daily
# MAGIC SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
# MAGIC
# MAGIC
# MAGIC ALTER TABLE `dev-gold`.default.lss_gold_hourly
# MAGIC SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
# MAGIC
# MAGIC ALTER TABLE `dev-silver`.default.lss_silver
# MAGIC SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
# MAGIC

# COMMAND ----------

# %sql
# DROP TABLE `dev-gold`.default.lss_gold_daily;
# DROP TABLE `dev-gold`.default.lss_gold_weekly;
# DROP TABLE `dev-gold`.default.lss_gold_monthly;


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count, expr
from delta.tables import DeltaTable

class IncrementalGoldJobStreaming:
    def __init__(self, spark: SparkSession, silver_table: str, gold_database: str):
        self.spark = spark
        self.silver_table = silver_table  # Silver table (source)
        self.gold_database = gold_database  # Gold database

    def ensure_gold_table_exists(self, gold_table_name):
        """
        Ensures the Gold table exists, creates it if not.
        """
        full_gold_table = f"{self.gold_database}.{gold_table_name}"
        try:
            DeltaTable.forName(self.spark, full_gold_table)
            print(f"✅ Gold table exists: {full_gold_table}")
        except:
            print(f"🚨 Gold table does not exist. Creating it: {full_gold_table}...")
            schema = """time_period STRING, metrics_name STRING, 
                        sum_metrics_value DOUBLE, count_metrics_value LONG, 
                        avg_metrics_value DOUBLE"""
            empty_df = self.spark.createDataFrame([], schema=schema)
            empty_df.write.format("delta").mode("overwrite").saveAsTable(full_gold_table)

    def process_batch(self, micro_batch_df, batch_id, time_col_expr, gold_table_name):
        """
        Processes each micro-batch and updates the Gold table.
        """
        if micro_batch_df.isEmpty():
            print(f"✅ No new data for {gold_table_name}, skipping update.")
            return

        full_gold_table = f"{self.gold_database}.{gold_table_name}"
        self.ensure_gold_table_exists(gold_table_name)

        # Aggregate new micro-batch data
        df_aggregated = micro_batch_df.withColumn("time_period", expr(time_col_expr)) \
            .groupBy("time_period", "metrics_name") \
            .agg(
                spark_sum("metrics_value").alias("sum_metrics_value"),
                count("metrics_value").alias("count_metrics_value")
            )

        gold_delta = DeltaTable.forName(self.spark, full_gold_table)

        # Perform Merge (Upsert)
        gold_delta.alias("gold").merge(
            df_aggregated.alias("silver"),
            "gold.time_period = silver.time_period AND gold.metrics_name = silver.metrics_name"
        ).whenMatchedUpdate(
            set={
                "sum_metrics_value": "gold.sum_metrics_value + silver.sum_metrics_value",
                "count_metrics_value": "gold.count_metrics_value + silver.count_metrics_value",
                "avg_metrics_value": "(gold.sum_metrics_value + silver.sum_metrics_value) / (gold.count_metrics_value + silver.count_metrics_value)"
            }
        ).whenNotMatchedInsert(
            values={
                "time_period": "silver.time_period",
                "metrics_name": "silver.metrics_name",
                "sum_metrics_value": "silver.sum_metrics_value",
                "count_metrics_value": "silver.count_metrics_value",
                "avg_metrics_value": "silver.sum_metrics_value / silver.count_metrics_value"
            }
        ).execute()

    def run(self):
        """
        Runs the streaming job to update the Gold table continuously.
        """
        print("🚀 Starting Structured Streaming Job for Incremental Gold Aggregation...")

        silver_stream_df = self.spark.readStream \
            .format("delta") \
            .table(self.silver_table)

        # Start streaming processing
        silver_stream_df.writeStream \
            .foreachBatch(lambda df, epoch_id: self.process_batch(df, epoch_id, 
                                                                  "date_format(datetime, 'yyyy-MM-dd HH:00')", 
                                                                  "lss_gold_hourly")) \
            .outputMode("append") \
            .option("checkpointLocation", f"/tmp/checkpoints/dev_gold_hourly") \
            .trigger(processingTime="5 minutes") \
            .start()


# Usage Example:
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("IncrementalGoldIngestionStreaming") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .getOrCreate()

    job = IncrementalGoldJobStreaming(
        spark,
        silver_table="`dev-silver`.default.lss_silver",
        gold_database="`dev-gold`.default"  # Gold tables are `dev-gold`.default.lss_gold_*
    )
    job.run()

    spark.streams.awaitAnyTermination()

# COMMAND ----------



# COMMAND ----------

df = spark.read.format("delta").load("abfss://bronze@datalaketest010.dfs.core.windows.net/bronze")
df.show()


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count, date_format, weekofyear, expr
from delta.tables import DeltaTable

class IncrementalGoldJob:
    def __init__(self, spark: SparkSession, silver_table: str, gold_database: str, use_cdc=True):
        self.spark = spark
        self.silver_table = silver_table  # Silver table
        self.gold_database = gold_database  # Gold database (e.g., "dev-gold.default")
        self.use_cdc = use_cdc  # Enable CDC if available

    def ensure_gold_table_exists(self, gold_table_name):
        """
        Ensures that the Gold table exists. If not, it initializes an empty Delta table.
        """
        full_gold_table = f"{self.gold_database}.{gold_table_name}"

        try:
            DeltaTable.forName(self.spark, full_gold_table)
            print(f"✅ Gold table exists: {full_gold_table}")
        except:
            print(f"🚨 Gold table does not exist. Creating a new Delta table: {full_gold_table}...")

            # Create an empty DataFrame with expected schema
            empty_df = self.spark.createDataFrame([], schema="""
                time_period STRING, 
                metrics_name STRING, 
                sum_metrics_value DOUBLE, 
                count_metrics_value LONG, 
                avg_metrics_value DOUBLE
            """)

            # Save as Delta table
            empty_df.write.format("delta").mode("overwrite").saveAsTable(full_gold_table)

    def get_latest_processed_timestamp(self, gold_table_name):
        """
        Retrieves the latest processed timestamp from the Gold table using DESCRIBE HISTORY.
        """
        full_gold_table = f"{self.gold_database}.{gold_table_name}"

        try:
            history_df = self.spark.sql(f"DESCRIBE HISTORY {full_gold_table}")
            if history_df.count() > 0:
                latest_timestamp = history_df.orderBy(col("timestamp").desc()).limit(1).collect()[0]["timestamp"]
                return latest_timestamp
        except:
            return None

        return None

    def read_silver_table_incrementally(self, gold_table_name):
        """
        Reads only new data from the Silver table using CDC or timestamp filtering.
        """
        if self.use_cdc:
            # Get the latest processed timestamp
            latest_timestamp = self.get_latest_processed_timestamp(gold_table_name)
            print(f"hmm latesttimestamp {latest_timestamp}")
            if latest_timestamp:
                print(f"🔍 Using latest timestamp {latest_timestamp} for CDC read")
                return self.spark.read.format("delta") \
                    .option("readChangeData", "true") \
                    .option("startingTimestamp", latest_timestamp) \
                    .table(self.silver_table)

            else:
                print("⚠️ No timestamp found, reading full Silver table (first run)")
                return self.spark.read.table(self.silver_table)

        # If CDC is not enabled, use timestamp filtering
        latest_timestamp = self.get_latest_processed_timestamp(gold_table_name)
        silver_df = self.spark.read.table(self.silver_table)

        if latest_timestamp:
            print(f"🔍 Filtering Silver table with datetime > {latest_timestamp}")
            silver_df = silver_df.filter(col("datetime") > latest_timestamp)

        return silver_df


    def process_gold_table(self, time_col_expr, gold_table_name):
        """
        Incrementally updates the specified Gold table with running aggregations.
        """
        full_gold_table = f"{self.gold_database}.{gold_table_name}"
        self.ensure_gold_table_exists(gold_table_name)

        # Read only new/updated Silver data
        silver_df = self.read_silver_table_incrementally(gold_table_name)

        if silver_df.isEmpty():
            print(f"✅ No new data for {full_gold_table}, skipping update.")
            return

        # Aggregate new data
        df_aggregated = silver_df.withColumn("time_period", expr(time_col_expr)) \
            .groupBy("time_period", "metrics_name") \
            .agg(
                spark_sum("metrics_value").alias("sum_metrics_value"),
                count("metrics_value").alias("count_metrics_value")
            )

        # Load Gold table
        gold_delta = DeltaTable.forName(self.spark, full_gold_table)

        # Merge logic: Maintain running sum, count, and accurate averages
        gold_delta.alias("gold").merge(
            df_aggregated.alias("silver"),
            "gold.time_period = silver.time_period AND gold.metrics_name = silver.metrics_name"
        ).whenMatchedUpdate(
            set={
                "sum_metrics_value": "gold.sum_metrics_value + silver.sum_metrics_value",
                "count_metrics_value": "gold.count_metrics_value + silver.count_metrics_value",
                "avg_metrics_value": "(gold.sum_metrics_value + silver.sum_metrics_value) / (gold.count_metrics_value + silver.count_metrics_value)"
            }
        ).whenNotMatchedInsert(
            values={
                "time_period": "silver.time_period",
                "metrics_name": "silver.metrics_name",
                "sum_metrics_value": "silver.sum_metrics_value",
                "count_metrics_value": "silver.count_metrics_value",
                "avg_metrics_value": "silver.sum_metrics_value / silver.count_metrics_value"
            }
        ).execute()

    def run(self):
        # Process daily, weekly, and monthly aggregations incrementally
        self.process_gold_table("date_format(datetime, 'yyyy-MM-dd HH:00')", "lss_gold_hourly")  # Hourly aggregation

        #self.process_gold_table("date_format(datetime, 'yyyy-MM-dd')", "lss_gold_daily")
        # self.process_gold_table("weekofyear(datetime)", "lss_gold_weekly")
        #self.process_gold_table("date_format(datetime, 'yyyy-MM')", "lss_gold_monthly")


# Usage Example:
if __name__ == "__main__":
    spark = SparkSession.builder.appName("IncrementalGoldIngestion").getOrCreate()

    job = IncrementalGoldJob(
        spark,
        silver_table="`dev-silver`.default.lss_silver",
        gold_database="`dev-gold`.default",  # Gold tables are `dev-gold`.default.lss_gold_*
        use_cdc=True  # Set to False if CDC is not enabled
    )
    job.run()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

