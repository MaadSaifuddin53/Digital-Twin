# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE main.silver.batch (
# MAGIC   id STRING NOT NULL,
# MAGIC   batchID STRING NOT NULL,
# MAGIC   batchSize INT,
# MAGIC   purpose STRING,
# MAGIC   dateEstablished DATE,
# MAGIC   dateClosed DATE,
# MAGIC   colonySize INT,
# MAGIC   localSampleName STRING,
# MAGIC   backUpBatchID STRING,
# MAGIC   CONSTRAINT pk_BatchID PRIMARY KEY (id)
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

silver_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, lit, trim, to_date
from pyspark.sql.types import *
from pyspark.sql import functions as F

# Define your source path or catalog reference
bronze_table = "main.bronze.batch"

# Read the raw Delta data
raw_df = spark.read.table(bronze_table).withColumn("id", F.expr("uuid()"))

# COMMAND ----------

from pyspark.sql.functions import col, to_date

# Clean and transform for silver
silver_df = raw_df.select(
    col("id"),
    col("BatchID").cast("string").alias("batchID"),
    col("BatchSize").cast("int").alias("batchSize"),
    col("Purpose").cast("string").alias("purpose"),
    to_date("DateEstablished", "yyyy-MM-dd").alias("dateEstablished"),
    to_date("DateClosed", "yyyy-MM-dd").alias("dateClosed"),
    col("ColonySize").cast("int").alias("colonySize"),
    col("LocalSampleName").cast("string").alias("localSampleName"),
    col("BackUpBatchID").cast("string").alias("backUpBatchID")
)

# Define Unity Catalog target
catalog = "main"
schema = "silver"
silver_table = "batch"

# Create silver delta table
silver_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.{silver_table}")