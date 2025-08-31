# Databricks notebook source
# MAGIC %md
# MAGIC **Create Unity Catalog Schema**

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS main.silver.MaintenanceList")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS main.silver.MaintenanceList(
# MAGIC     id STRING Not NULL,
# MAGIC     site STRING,
# MAGIC     structureName date,
# MAGIC     activityType STRING,
# MAGIC     activityDate STRING,
# MAGIC     newDepth STRING,
# MAGIC     notes STRING,
# MAGIC     createdBy STRING,
# MAGIC     createdDateTime TIMESTAMP,
# MAGIC     modifiedBy STRING,
# MAGIC     modifiedDateTime TIMESTAMP,
# MAGIC     PRIMARY KEY (id)
# MAGIC );
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
bronze_table = "main.bronze.MaintenanceList"

raw_df = spark.read.table(bronze_table)
raw_df.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC **Data Cleaning and Transformation**

# COMMAND ----------

from pyspark.sql.functions import to_date, trim, col

date_cols = ['activityDate']
for c in date_cols:
    raw_df = raw_df.withColumn(c, to_date(trim(col(c)),"yyyy-MM-dd" ))
    raw_df.select(col(c)).distinct().show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC **Datatype Casting**

# COMMAND ----------

# # Casting Datatypes
# from pyspark.sql.types import BooleanType
# int_cols = ['ColonyCount']
# for c in int_cols:
#     raw_df = raw_df.withColumn(c, col(c).cast("int"))
# raw_df.printSchema()

# COMMAND ----------

column_mapping = {
    "Site": "site",
    "StructureName": "structureName",
    "ActivityType": "activityType",
    "ActivityDate": "activityDate",
    "NewDepth": "newDepth",
    "Notes": "notes"
}

for old, new in column_mapping.items():
    raw_df = raw_df.withColumnRenamed(old, new)
maint_df=raw_df
maint_df = maint_df.withColumn("id",generate_uuid()).persist()

# COMMAND ----------

maint_df.printSchema()

# COMMAND ----------

#  Reorder columns to move "id" to the first position
cols = maint_df.columns
new_col_order = ["id"] + [col for col in cols if col != "id"]
maint_df = maint_df.select(new_col_order)
maint_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **Adding CreatedDateTime, CreateBy, ModifiedDateTime, ModifiedBy Values**

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, from_utc_timestamp, lit

# Add createdDateTime, modifiedDateTime, createdBy, and modifiedBy columns
maint_df = maint_df.withColumn("createdBy", lit("MaintaincePipeline-silver-layer-job")) \
        .withColumn("createdDateTime", from_utc_timestamp(current_timestamp(), "Asia/Riyadh")) \
       .withColumn("modifiedBy", lit("MaintaincePipeline-silver-layer-job")) \
       .withColumn("modifiedDateTime", from_utc_timestamp(current_timestamp(), "Asia/Riyadh")) \
        
maint_df.show(2)

# COMMAND ----------

maint_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **Storing into Silver layer**

# COMMAND ----------

maint_df.write.mode("overwrite").saveAsTable("main.silver.MaintaincePipeline")


# COMMAND ----------

