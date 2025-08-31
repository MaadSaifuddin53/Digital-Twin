# Databricks notebook source
# MAGIC %md
# MAGIC **Create Unity Catalog Schema**

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS main.silver.DonorTransportation")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS main.silver.DonorTransportation(
# MAGIC     id STRING Not NULL,
# MAGIC     donorID  string,
# MAGIC     dateMoved  date,
# MAGIC     fromSiteID  string,
# MAGIC     toSiteID  string,
# MAGIC     toStructureID  string,
# MAGIC     transferMethod  string,
# MAGIC     containerID  string,
# MAGIC     surfaceTime  string,
# MAGIC     teamLead  string,
# MAGIC     notes string,
# MAGIC     createdBy string,
# MAGIC     createdDateTime TIMESTAMP,
# MAGIC     modifiedBy string,
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
bronze_table = "main.bronze.DonorTransportation"

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
    "DonorID": "donorID",
    "DateMoved": "dateMoved",
    "FromSiteID": "fromSiteID",
    "ToSiteID": "toSiteID",
    "ToStructureID": "toStructureID",
    "TransferMethod": "transferMethod",
    "ContainerID": "containerID",
    "SurfaceTime": "surfaceTime",
    "TeamLead": "teamLead",
    "Notes": "notes"
}

for old, new in column_mapping.items():
    raw_df = raw_df.withColumnRenamed(old, new)
donorT_df=raw_df
donorT_df = donorT_df.withColumn("id",generate_uuid()).persist()

# COMMAND ----------

donorT_df.printSchema()

# COMMAND ----------

#  Reorder columns to move "id" to the first position
cols = donorT_df.columns
new_col_order = ["id"] + [col for col in cols if col != "id"]
donorT_df = donorT_df.select(new_col_order)
donorT_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **Adding CreatedDateTime, CreateBy, ModifiedDateTime, ModifiedBy Values**

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, from_utc_timestamp, lit

# Add createdDateTime, modifiedDateTime, createdBy, and modifiedBy columns
donorT_df = donorT_df.withColumn("createdBy", lit("DonorTransportation-silver-layer-job")) \
        .withColumn("createdDateTime", from_utc_timestamp(current_timestamp(), "Asia/Riyadh")) \
       .withColumn("modifiedBy", lit("DonorTransportation-silver-layer-job")) \
       .withColumn("modifiedDateTime", from_utc_timestamp(current_timestamp(), "Asia/Riyadh")) \
        
donorT_df.show(2)

# COMMAND ----------

donorT_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **Storing into Silver layer**

# COMMAND ----------

donorT_df.write.mode("overwrite").saveAsTable("main.silver.DonorTransportation")


# COMMAND ----------

