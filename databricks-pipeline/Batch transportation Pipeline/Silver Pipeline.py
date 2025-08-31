# Databricks notebook source
# MAGIC %md
# MAGIC **Create Unity Catalog Schema**

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS main.silver.BatchTransportation")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS main.silver.BatchTransportation (
# MAGIC     id STRING Not NULL,
# MAGIC     batchID STRING,
# MAGIC     dateMoved date,
# MAGIC      fromSite STRING,
# MAGIC     fromStructureID STRING,
# MAGIC     fromSubStructure STRING,
# MAGIC     toSite STRING,
# MAGIC     toStructureID STRING,
# MAGIC     toSubStructure STRING,
# MAGIC     transferMethod STRING,
# MAGIC     containerID STRING,
# MAGIC     surfaceTime STRING,
# MAGIC     reason STRING,
# MAGIC     colonyCount int,
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
bronze_table = "main.bronze.BatchTransportation"

raw_df = spark.read.table(bronze_table)
raw_df.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC **Data Cleaning and Transformation**

# COMMAND ----------

from pyspark.sql.functions import to_date, trim, col

date_cols = ['DateMoved']
for c in date_cols:
    raw_df = raw_df.withColumn(c, to_date(trim(col(c)),"dd/MM/yyyy" ))
    raw_df.select(col(c)).distinct().show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC **Datatype Casting**

# COMMAND ----------

# Casting Datatypes
from pyspark.sql.types import BooleanType
int_cols = ['ColonyCount']
for c in int_cols:
    raw_df = raw_df.withColumn(c, col(c).cast("int"))
raw_df.printSchema()

# COMMAND ----------

column_mapping = {
    "BatchID": "batchID",
    "DateMoved": "dateMoved",
    "FromSite": "fromSite",
    "FromStructureID": "fromStructureID",
    "FromSubStructure": "fromSubStructure",
    "ToSite": "toSite",
    "ToStructureID": "toStructureID",
    "ToSubStructure": "toSubStructure",
    "TransferMethod": "transferMethod",
    "ContainerID": "containerID",
    "SurfaceTime": "surfaceTime",
    "Reason": "reason",
    "ColonyCount": "colonyCount"
}

for old, new in column_mapping.items():
    raw_df = raw_df.withColumnRenamed(old, new)
batchT_df=raw_df
batchT_df = batchT_df.withColumn("id",generate_uuid()).persist()

# COMMAND ----------

batchT_df.printSchema()

# COMMAND ----------

#  Reorder columns to move "id" to the first position
cols = batchT_df.columns
new_col_order = ["id"] + [col for col in cols if col != "id"]
batchT_df = batchT_df.select(new_col_order)
batchT_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **Adding CreatedDateTime, CreateBy, ModifiedDateTime, ModifiedBy Values**

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, from_utc_timestamp, lit

# Add createdDateTime, modifiedDateTime, createdBy, and modifiedBy columns
batchT_df = batchT_df.withColumn("createdBy", lit("BatchTransportation-silver-layer-job")) \
        .withColumn("createdDateTime", from_utc_timestamp(current_timestamp(), "Asia/Riyadh")) \
       .withColumn("modifiedBy", lit("BatchTransportation-silver-layer-job")) \
       .withColumn("modifiedDateTime", from_utc_timestamp(current_timestamp(), "Asia/Riyadh")) \
        
batchT_df.show(2)

# COMMAND ----------

batchT_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **Storing into Silver layer**

# COMMAND ----------

batchT_df.write.mode("overwrite").saveAsTable("main.silver.BatchTransportation")
