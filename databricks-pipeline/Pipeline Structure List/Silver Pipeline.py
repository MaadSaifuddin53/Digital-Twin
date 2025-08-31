# Databricks notebook source
# MAGIC %md
# MAGIC **Create Unity Catalog Schema**

# COMMAND ----------

# # spark.sql("DROP TABLE IF EXISTS main.silver.StructureList")
# spark.sql("SELECT * FROM main.silver.StructureList").show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC **Creating Schema into Unity Catalog for StructureList Table**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS main.silver.StructureList (
# MAGIC   id STRING NOT NULL,
# MAGIC   structureName STRING,
# MAGIC   structureType STRING,
# MAGIC   dateDeployed DATE,
# MAGIC   dateDecommissioned DATE, 
# MAGIC   installer STRING,
# MAGIC   latitude DOUBLE,
# MAGIC   longitude DOUBLE,
# MAGIC   capacity INT,
# MAGIC   purpose STRING,
# MAGIC   length double,
# MAGIC   width double,
# MAGIC   manufacturer STRING,
# MAGIC   materials STRING,
# MAGIC   notes STRING,
# MAGIC   createdBy STRING,
# MAGIC   createdDateTime TIMESTAMP,
# MAGIC   modifiedBy STRING,
# MAGIC   modifiedDateTime TIMESTAMP,
# MAGIC   PRIMARY KEY (id)
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

import uuid
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

generate_uuid = F.udf(lambda: str(uuid.uuid4()), StringType())


# COMMAND ----------

from pyspark.sql.functions import col, lit, trim, to_date
from pyspark.sql.types import *

# Define your source path or catalog reference
bronze_table = "main.bronze.StructureList"

raw_df = spark.read.table(bronze_table)
raw_df.show(2)

# COMMAND ----------

raw_df.select(F.col('DateDeployed')).distinct().show()

# COMMAND ----------

raw_df.select(F.col('DateDecommissioned')).distinct().show()

# COMMAND ----------

for col in raw_df.columns:
    print(raw_df.select(col).distinct().show(truncate=False))

# COMMAND ----------

# MAGIC %md
# MAGIC **Data Cleaning and Transformation**

# COMMAND ----------

from pyspark.sql.functions import to_date, trim, col

date_cols = ['DateDeployed', 'DateDecommissioned']
for c in date_cols:
    raw_df = raw_df.withColumn(c, to_date(trim(col(c)), "dd/MM/yyyy"))
    raw_df.select(col(c)).distinct().show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC **Datatype Casting** 

# COMMAND ----------

# Casting Datatypes
raw_df = raw_df.withColumn("Latitude", col("Latitude").cast("double"))
raw_df = raw_df.withColumn("Longitude", col("Longitude").cast("double"))
raw_df = raw_df.withColumn("Length", col("Length").cast("double"))
raw_df = raw_df.withColumn("Width", col("Width").cast("double"))
raw_df = raw_df.withColumn("Capacity", col("Capacity").cast("int"))
raw_df.printSchema()

# COMMAND ----------

import re
from pyspark.sql.functions import col

raw_df = raw_df.select([col(c).alias(c.lower()) for c in raw_df.columns])
raw_df.columns

# COMMAND ----------

# MAGIC %md
# MAGIC **Schema Matching**

# COMMAND ----------

rename_dict = {
    "structureName": "structureName",
    "structuretype":"structureType",
    "dateDeployed": "dateDeployed",
    "datedecommissioned":"dateDecommissioned"
}

for old, new in rename_dict.items():
    raw_df = raw_df.withColumnRenamed(old, new)
struncture_df=raw_df
struncture_df = struncture_df.withColumn("id",generate_uuid()).persist()

# COMMAND ----------

struncture_df.printSchema()

# COMMAND ----------

#  Reorder columns to move "id" to the first position
cols = struncture_df.columns
new_col_order = ["id"] + [col for col in cols if col != "id"]
struncture_df = struncture_df.select(new_col_order)
struncture_df.printSchema()

# COMMAND ----------

struncture_df.show(4)

# COMMAND ----------

# MAGIC %md
# MAGIC **Adding CreatedDateTime, CreateBy, ModifiedDateTime, ModifiedBy Values** 

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, from_utc_timestamp, lit

# Add createdDateTime, modifiedDateTime, createdBy, and modifiedBy columns
struncture_df = struncture_df.withColumn("createdBy", lit("structure-silver-layer-job")) \
        .withColumn("createdDateTime", from_utc_timestamp(current_timestamp(), "Asia/Riyadh")) \
       .withColumn("modifiedBy", lit("structure-silver-layer-job")) \
       .withColumn("modifiedDateTime", from_utc_timestamp(current_timestamp(), "Asia/Riyadh")) \
        
       
struncture_df.show(2)

# COMMAND ----------

struncture_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **Storing into Silver layer**

# COMMAND ----------

# MAGIC %md
# MAGIC Ingestion into Silver Layer

# COMMAND ----------

struncture_df.write.mode("overwrite").saveAsTable("main.silver.StructureList")


# COMMAND ----------

# MAGIC %md
# MAGIC **Bridge Tables Connection**

# COMMAND ----------


df_batch = spark.read.format("delta").table("main.silver.batch")     
df_colony = spark.read.format("delta").table("main.silver.colony")


# COMMAND ----------

print("Batch Data")
print(df_batch.show(2))
print("Colony Data")
print(df_colony.show(2))

# COMMAND ----------

# MAGIC %md
# MAGIC **Perform inner join on the linking keys**

# COMMAND ----------

df_bridge = df_batch.join(
    df_colony,
    df_batch.localSampleName == df_colony.sageUpdates,
    "inner"
).select(
    df_batch["id"].alias("batchUUID"),     # Replace with actual PK
    df_colony["id"].alias("colonyUUID")   # Replace with actual PK
).distinct()  # Ensure unique relationships
df_bridge.show(2)

# COMMAND ----------

df_bridge.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("main.silver.bridge_batch_colony")


# COMMAND ----------

# MAGIC %md
# MAGIC **Coral Outplant and Colony**

# COMMAND ----------

# Assuming these are your tables
df_coraloutplant  = spark.read.format("delta").table("main.silver.coraloutplant")     
df_colony = spark.read.format("delta").table("main.silver.colony")
print("Coral Outplant  Data")
print(df_coraloutplant.show(2))
print("Colony Data")
print(df_colony.show(2))


# COMMAND ----------

# MAGIC %md
# MAGIC **Perform inner join on the linking keys**

# COMMAND ----------

df_bridge_2 = df_coraloutplant.join(
    df_colony,
    df_coraloutplant.coralID == df_colony.sageUpdates,
    "inner"
).select(
    df_coraloutplant["id"].alias("coraloutplantUUID"),     # Replace with actual PK
    df_colony["id"].alias("colonyUUID")   # Replace with actual PK
).distinct()  # Ensure unique relationships
df_bridge_2.show(2)

# COMMAND ----------

df_bridge_2.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("main.silver.bridge_coraloutplant_colony")


# COMMAND ----------

# MAGIC %md
# MAGIC **CoralOutplant and Batch**

# COMMAND ----------

df_bridge_3 = df_coraloutplant.join(
    df_batch,
    df_coraloutplant.coralID == df_batch.localSampleName,
    "inner"
).select(
    df_coraloutplant["id"].alias("coraloutplantUUID"),     # Replace with actual PK
    df_batch["id"].alias("batchUUID")   # Replace with actual PK
).distinct()  # Ensure unique relationships
df_bridge_3.show(2)

# COMMAND ----------

df_bridge_3.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("main.silver.bridge_coraloutplant_batch")


# COMMAND ----------

# MAGIC %md
# MAGIC **Outplant Event and Structure List**

# COMMAND ----------

 # Assuming these are your tables
df_outplantevent  = spark.read.format("delta").table("main.silver.outplantevent")     
df_structurelist = spark.read.format("delta").table("main.silver.structurelist")
print("Outplant Event Data")
print(df_outplantevent.show(2))
print("Structurelist Data")
print(df_structurelist.show(2))


# COMMAND ----------

df_bridge_4 = df_outplantevent.join(
    df_structurelist,
    df_outplantevent.outplantID == df_structurelist.structureName,
    "inner"
).select(
    df_outplantevent["id"].alias("outplanteventUUID"),     # Replace with actual PK
    df_structurelist["id"].alias("structurelistUUID")   # Replace with actual PK
).distinct()  # Ensure unique relationships
df_bridge_4.show(2)

# COMMAND ----------

df_bridge_4.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("main.silver.bridge_OutplantEvent_Structurelist")
  