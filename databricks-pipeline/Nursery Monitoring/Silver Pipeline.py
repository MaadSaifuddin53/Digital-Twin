# Databricks notebook source
# MAGIC %md
# MAGIC **Create Unity Catalog Schema**

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS main.silver.NurseryMonitoring")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE if not EXISTS main.silver.NurseryMonitoring (
# MAGIC   id STRING Not NULL,
# MAGIC   batchID string,
# MAGIC   structureName string,
# MAGIC   structureRow string,
# MAGIC   dateEstablished date,
# MAGIC   dateMonitored date,
# MAGIC   dead int,
# MAGIC   alive int,
# MAGIC   ready2Outplant int,
# MAGIC   bleached BOOLEAN,
# MAGIC   diseased string,
# MAGIC   createdBy STRING,
# MAGIC   createdDateTime TIMESTAMP,
# MAGIC   modifiedBy STRING,
# MAGIC   modifiedDateTime TIMESTAMP,
# MAGIC   PRIMARY KEY (id)
# MAGIC )     

# COMMAND ----------

import uuid
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

generate_uuid = F.udf(lambda: str(uuid.uuid4()), StringType())


# COMMAND ----------

from pyspark.sql.functions import col, lit, trim, to_date
from pyspark.sql.types import *

# Define your source path or catalog reference
bronze_table = "main.bronze.nurserymonitoring"

raw_df = spark.read.table(bronze_table)
raw_df.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC **Data Cleaning and Transformation**

# COMMAND ----------

from pyspark.sql.functions import to_date, trim, col

date_cols = ['DateEstablished', 'DateMonitored']
for c in date_cols:
    raw_df = raw_df.withColumn(c, to_date(trim(col(c)), "dd/MM/yyyy"))
    raw_df.select(col(c)).distinct().show(truncate=False)


# COMMAND ----------

from pyspark.sql.functions import when, col

raw_df = raw_df.withColumn(
    "Bleached",
    when(col("Bleached") == "Yes", True)
    .when(col("Bleached") == "No", False)
    .otherwise(None)  # Handle unexpected values (optional)
)


# COMMAND ----------

# MAGIC %md
# MAGIC **Datatype Casting**

# COMMAND ----------

# Casting Datatypes
from pyspark.sql.types import BooleanType
raw_df = raw_df.withColumn("Dead", col("Dead").cast("int"))
raw_df = raw_df.withColumn("Alive", col("Alive").cast("int"))
raw_df = raw_df.withColumn("Ready2Outplant", col("Ready2Outplant").cast("int"))
raw_df = raw_df.withColumn("Bleached", col("Bleached").cast(BooleanType()))
raw_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **Schema Matching**

# COMMAND ----------

column_mapping = {
    "BatchID": "batchID",
    "StructureName": "structureName",
    "StructureRow": "structureRow",
    "DateEstablished": "dateEstablished",
    "DateMonitored": "dateMonitored",
    "Dead": "dead",
    "Alive": "alive",
    "Ready2Outplant": "ready2Outplant",
    "Bleached": "bleached",
    "Diseased": "diseased"
}
for old, new in column_mapping.items():
    raw_df = raw_df.withColumnRenamed(old, new)
nurseryM_df=raw_df
nurseryM_df = nurseryM_df.withColumn("id",generate_uuid()).persist()

# COMMAND ----------

nurseryM_df.printSchema()

# COMMAND ----------

#  Reorder columns to move "id" to the first position
cols = nurseryM_df.columns
new_col_order = ["id"] + [col for col in cols if col != "id"]
nurseryM_df = nurseryM_df.select(new_col_order)
nurseryM_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **Adding CreatedDateTime, CreateBy, ModifiedDateTime, ModifiedBy Values**

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, from_utc_timestamp, lit

# Add createdDateTime, modifiedDateTime, createdBy, and modifiedBy columns
nurseryM_df = nurseryM_df.withColumn("createdBy", lit("NurseryMonitoring-silver-layer-job")) \
        .withColumn("createdDateTime", from_utc_timestamp(current_timestamp(), "Asia/Riyadh")) \
       .withColumn("modifiedBy", lit("NurseryMonitoring-silver-layer-job")) \
       .withColumn("modifiedDateTime", from_utc_timestamp(current_timestamp(), "Asia/Riyadh")) \
        
       
nurseryM_df.show(2)

# COMMAND ----------

nurseryM_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **Storing into Silver layer**

# COMMAND ----------

nurseryM_df.write.mode("overwrite").saveAsTable("main.silver.NurseryMonitoring")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM main.silver.nurserymonitoring

# COMMAND ----------

