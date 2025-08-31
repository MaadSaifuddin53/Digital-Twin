# Databricks notebook source
# MAGIC %md
# MAGIC **Create Unity Catalog Schema**

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS main.silver.OutplantMonitoring")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE if not EXISTS main.silver.NurseryMonitoring(
# MAGIC id STRING Not NULL,
# MAGIC dateMonitored date,  
# MAGIC outplantID  string,
# MAGIC colonyCount  int,
# MAGIC percHealth100  int,
# MAGIC percHealth5099  int,
# MAGIC percHealth149  int,
# MAGIC percHealth0  int,
# MAGIC bleached  int,
# MAGIC diseased  int,
# MAGIC predation  int,
# MAGIC diverID  string,
# MAGIC notes string,
# MAGIC createdBy STRING,
# MAGIC createdDateTime TIMESTAMP,
# MAGIC modifiedBy STRING,
# MAGIC modifiedDateTime TIMESTAMP,
# MAGIC PRIMARY KEY (id)
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
bronze_table = "main.bronze.outplantmonitoring"

raw_df = spark.read.table(bronze_table)
raw_df.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC **Data Cleaning and Transformation**

# COMMAND ----------

from pyspark.sql.functions import to_date, trim, col

date_cols = ['DateMonitored']
for c in date_cols:
    raw_df = raw_df.withColumn(c, to_date(trim(col(c)),"yyyy-MM-dd" ))
    raw_df.select(col(c)).distinct().show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC **Datatype Casting**

# COMMAND ----------

# Casting Datatypes
from pyspark.sql.types import BooleanType
int_cols = ['ColonyCount', 'PercHealth100', 'PercHealth50_99', 'PercHealth1_49', 'PercHealth_0',
            'Bleached','Diseased','Predation']
for c in int_cols:
    raw_df = raw_df.withColumn(c, col(c).cast("int"))
raw_df.printSchema()

# COMMAND ----------

column_mapping = {
    "DateMonitored": "dateMonitored",
    "OutplantID": "outplantID",
    "ColonyCount": "colonyCount",
    "PercHealth100": "percHealth100",
    "PercHealth50_99": "percHealth5099",
    "PercHealth1_49": "percHealth149",
    "PercHealth_0": "percHealth0",
    "Bleached": "bleached",
    "Diseased": "diseased",
    "Predation": "predation",
    "DiverID": "diverID",
    "Notes": "notes"
}
for old, new in column_mapping.items():
    raw_df = raw_df.withColumnRenamed(old, new)
outplantM_df=raw_df
outplantM_df = outplantM_df.withColumn("id",generate_uuid()).persist()

# COMMAND ----------

outplantM_df.printSchema()

# COMMAND ----------

#  Reorder columns to move "id" to the first position
cols = outplantM_df.columns
new_col_order = ["id"] + [col for col in cols if col != "id"]
outplantM_df = outplantM_df.select(new_col_order)
outplantM_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **Adding CreatedDateTime, CreateBy, ModifiedDateTime, ModifiedBy Values**

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, from_utc_timestamp, lit

# Add createdDateTime, modifiedDateTime, createdBy, and modifiedBy columns
outplantM_df = outplantM_df.withColumn("createdBy", lit("OutplantMonitoring-silver-layer-job")) \
        .withColumn("createdDateTime", from_utc_timestamp(current_timestamp(), "Asia/Riyadh")) \
       .withColumn("modifiedBy", lit("OutplantMonitoring-silver-layer-job")) \
       .withColumn("modifiedDateTime", from_utc_timestamp(current_timestamp(), "Asia/Riyadh")) \
        
outplantM_df.show(2)

# COMMAND ----------

outplantM_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **Storing into Silver layer**

# COMMAND ----------

outplantM_df.write.mode("overwrite").saveAsTable("main.silver.OutplantMonitoring")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM main.silver.OutplantMonitoring

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Bridge Tables Connection**

# COMMAND ----------

df_outplantEvent = spark.read.format("delta").table("main.silver.outplantevent")
df_outplantMonitoring = spark.read.format("delta").table("main.silver.outplantmonitoring")

# COMMAND ----------

# MAGIC %md
# MAGIC Perform inner join on the linking keys

# COMMAND ----------

df_bridge = df_outplantEvent.join(
    df_outplantMonitoring,
    df_outplantEvent.outplantID == df_outplantMonitoring.outplantID,
    "inner"
).select(
    df_outplantEvent["id"].alias("outplantEventUUID"),     
    df_outplantMonitoring["id"].alias("outplantMonitoringUUID")   
).distinct()  # Ensure unique relationships
df_bridge.show(2)

# COMMAND ----------

df_bridge.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("main.silver.bridge_outplantEvent_outplantMonitoring")


# COMMAND ----------

