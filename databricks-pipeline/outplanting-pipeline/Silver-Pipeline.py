# Databricks notebook source
# MAGIC %md
# MAGIC # Create Unity Catalog Schema

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- OutplantEvent Table
# MAGIC CREATE TABLE IF NOT EXISTS main.silver.OutplantEvent (
# MAGIC     id STRING,
# MAGIC     outplantID STRING,
# MAGIC     dateOutplanted DATE,
# MAGIC     outplantStructureID STRING,
# MAGIC     siteID STRING,  -- This is UUID from Site table
# MAGIC     
# MAGIC     PRIMARY KEY (id),
# MAGIC     --FOREIGN KEY (outplantStructureID) REFERENCES `main`.silver.structure(id),
# MAGIC     FOREIGN KEY (siteID) REFERENCES `main`.silver.Site(id)
# MAGIC
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC -- CoralOutplant Table
# MAGIC CREATE TABLE IF NOT EXISTS main.silver.CoralOutplant (
# MAGIC     id STRING,
# MAGIC     coralID STRING,
# MAGIC     outplantEvent STRING,  -- This is UUID from OutplantEvent
# MAGIC     colonyCount INT,
# MAGIC     avgSize FLOAT,
# MAGIC     diver STRING,
# MAGIC     attachmentMethod STRING,
# MAGIC     notes STRING,
# MAGIC     fromSite STRING,
# MAGIC     fromStructure STRING,
# MAGIC     fromSubStructure STRING,
# MAGIC     transportationMethod STRING,
# MAGIC     timeOnSurface FLOAT,
# MAGIC     depth FLOAT,
# MAGIC     outplantType STRING,
# MAGIC     orientation STRING,
# MAGIC     exposure STRING,
# MAGIC     outplantBase STRING,
# MAGIC     PRIMARY KEY (id),
# MAGIC     FOREIGN KEY (outplantEvent) REFERENCES `main`.silver.OutplantEvent(id)
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Helper Functions

# COMMAND ----------

import uuid
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

generate_uuid = F.udf(lambda: str(uuid.uuid4()), StringType())


# COMMAND ----------

from pyspark.sql.functions import col, lit, trim, to_date
from pyspark.sql.types import *

# Define your source path or catalog reference
bronze_table = "main.bronze.outplanting"

# Read the raw Delta data
raw_df = spark.read.table(bronze_table).withColumn(
    "DateOutplanted",
    to_date(trim(col("DateOutplanted")), "d-MMM-yyyy")
)

print(raw_df.count())

# COMMAND ----------

raw_df.select(F.col("DateOutplanted")).head(1000)

# COMMAND ----------

# MAGIC %md
# MAGIC # Extend new sites data to Site delta table

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit

# Step 1: Create site_append
site_append = raw_df.select(
    F.col("SiteID").alias("Site"),
    F.col("Latitude"),
    F.col("Longitude")
).withColumn("ReefName", F.lit("Shushah")) \
 .withColumn("id", F.expr("uuid()")).drop_duplicates(subset=['site'])

# Step 2: Load the Delta table (assuming it's a registered table in metastore)
site_table = DeltaTable.forName(spark, "main.silver.site")

# Step 3: Merge (Insert only if not matched)
site_table.alias("target").merge(
    site_append.alias("source"),
    "target.Site = source.Site"
).whenNotMatchedInsert(values={
    "id" : col("source.id"),
    "Site": col("source.Site"),
    "Latitude": col("source.Latitude"),
    "Longitude": col("source.Longitude"),
    "ReefName": col("source.ReefName")
}).execute()

# COMMAND ----------

# MAGIC %md
# MAGIC # Check for Null Values

# COMMAND ----------

df_null = raw_df.filter(col("OutplantID").isNull())
print(df_null.count())

# COMMAND ----------

site = "main.silver.site"

# Read the raw Delta data
site_df = spark.read.table(site)

# COMMAND ----------

raw_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, trim, to_date


outplant_event_df = raw_df.select(
    trim(col("OutplantID")).alias("outplantID"),
    F.col("DateOutplanted").alias("dateOutplanted"),
    trim(col("SiteID")).alias("siteID"),
    trim(col("OutplantStructureID")).alias("outplantStructureID")
).dropDuplicates()
outplant_event_df = outplant_event_df.withColumn("id", generate_uuid()).persist()

# COMMAND ----------

outplant_event_df.printSchema()

# COMMAND ----------

len(site_df.select(F.col("site")).distinct().collect())

# COMMAND ----------

len(set(site_df.select('id').distinct().rdd.flatMap(lambda x: x).collect()))

# COMMAND ----------

# MAGIC %md
# MAGIC # Construct Outplant Event Table 

# COMMAND ----------


# # Join with site_df to resolve SiteUUID FK
from pyspark.sql import functions as F

outplant_event_df = outplant_event_df.join(
    site_df.select(F.col("id").alias("site_uuid"), F.col("site")),  # select only needed columns and alias for clarity
    outplant_event_df["siteID"] == F.col("site"),  # match on renamed column
    how="left"
)

outplant_event_final = outplant_event_df.drop(*["site","siteID"])\
.withColumnRenamed("site_uuid", "siteID")  # rename 'id' to 'SiteID' if needed

# COMMAND ----------

outplant_event_df.head(100)

# COMMAND ----------

# MAGIC %md
# MAGIC # Construct Coral Outplant Table

# COMMAND ----------

outplant_event_df.columns

# COMMAND ----------

from pyspark.sql.functions import expr

coral_outplant_df = raw_df.select(
    trim(col("CoralID")).alias("coralID"),
    trim(col("OutplantID")).alias("outplantID"),
    col("ColonyCount").cast("int").alias("colonyCount"),
    trim(col("AvgSize")).cast("float").alias("avgSize"),
    trim(col("AttachmentMethod")).alias("attachmentMethod"),
    trim(col("Notes")).alias("notes"),
    trim(col("FromSite")).alias("fromSite"),
    trim(col("FromStructure")).alias("fromStructure"),
    trim(col("FromSubStructure")).alias("fromSubStructure"),
    trim(col("TransportationMethod")).alias("transportationMethod"),
    col("TimeOnSurface").cast("float").alias("timeOnSurface"),
    col("Depth").cast("float").alias("depth"),
    trim(col("OutplantType")).alias("outplantType"),
    trim(col("Orientation")).alias("orientation"),
    trim(col("Exposure")).alias("exposure"),
    trim(col("OutplantBase")).alias("outplantBase"),
    trim(col("Diver")).alias("diver"),
    F.col("DateOutplanted").alias("dateOutplanted"),
    trim(col("OutplantStructureID")).alias("outplantStructureID"),
    trim(col("SiteID")).alias("siteID"),

)

coral_outplant_df = coral_outplant_df.withColumn("id", generate_uuid()).persist()



coral = coral_outplant_df.alias("coral")
event = outplant_event_df.select(
    col("id").alias("outplantUUID"),
    col("outplantID"),
    col("siteID"),
    col("OutplantStructureID"),
    col("DateOutplanted")
).alias("event")

# Build the null-safe join condition as a single expression string
join_condition = expr("""
    coral.outplantID <=> event.outplantID AND
    coral.siteID <=> event.siteID AND
    coral.outplantStructureID <=> event.OutplantStructureID AND
    coral.dateOutplanted <=> event.DateOutplanted
""")

# Perform the join
coral_outplant_df_final = coral.join(
    event,
    on=join_condition,
    how="left"
).drop(
    "outplantID", "siteID", "OutplantStructureID", "DateOutplanted"
).withColumnRenamed("outplantUUID", "outplantEvent")


# COMMAND ----------

coral_outplant_df_final.head(100)

# COMMAND ----------

coral_outplant_df.head(10)

# COMMAND ----------

coral_outplant_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # Insert Data to delta-lake

# COMMAND ----------

outplant_event_final.printSchema()

# COMMAND ----------

outplant_event_final.write.mode("overwrite").saveAsTable("main.silver.OutplantEvent")


# COMMAND ----------

coral_outplant_df_final.write.mode("overwrite").saveAsTable("main.silver.CoralOutplant")

# COMMAND ----------

