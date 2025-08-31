# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS `main`.silver.Site(
# MAGIC     id STRING NOT NULL,
# MAGIC     reefName STRING,
# MAGIC     site STRING,
# MAGIC     latitude DOUBLE,
# MAGIC     longitude DOUBLE,
# MAGIC     PRIMARY KEY (id)
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS `main`.silver.Identification(
# MAGIC     id STRING NOT NULL,
# MAGIC     genus STRING,
# MAGIC     species STRING,
# MAGIC     PRIMARY KEY (id)
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS `main`.silver.Colony(
# MAGIC     id STRING NOT NULL,
# MAGIC     siteID STRING,
# MAGIC     identificationID STRING,
# MAGIC     tagNumber INT,
# MAGIC     count INT,
# MAGIC     tagColor STRING,
# MAGIC     depth DOUBLE,
# MAGIC     growthForm STRING,
# MAGIC     maximumDiameter DOUBLE,
# MAGIC     percentLivingTissue DOUBLE,
# MAGIC     imageCollected BOOLEAN,
# MAGIC     imageNo STRING,
# MAGIC     positiveIDMade BOOLEAN,
# MAGIC     colonyAvailableForMaterial BOOLEAN,
# MAGIC     dateCollected DATE,
# MAGIC     collector STRING,
# MAGIC     confidence STRING,
# MAGIC     donorReplicate INT,
# MAGIC     tempSpeciesName STRING,
# MAGIC     sageReviewed BOOLEAN,
# MAGIC     sageUpdates STRING,
# MAGIC     sampleType STRING,
# MAGIC     sampleCollection STRING,
# MAGIC     localSampleName STRING,
# MAGIC     localSampleName_ver2 STRING,
# MAGIC     localSampleName_ver1 STRING,
# MAGIC     geneticSampleCollected BOOLEAN,
# MAGIC     dateOfGeneticSampleCollection DATE,
# MAGIC     PRIMARY KEY (id),
# MAGIC     FOREIGN KEY (siteID) REFERENCES `main`.silver.Site(id),
# MAGIC     FOREIGN KEY (identificationID) REFERENCES `main`.silver.Identification(id)
# MAGIC
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS `main`.silver.ColonyNurseryDocument(
# MAGIC     id STRING NOT NULL,
# MAGIC     colonyID STRING,
# MAGIC     nurseryID STRING,
# MAGIC     DocumentedInNurseryI STRING,
# MAGIC     Notes STRING,
# MAGIC     DocumentedIntructure BOOLEAN,
# MAGIC     CoordinateNotes STRING,
# MAGIC     NotesMispechiesFromNurseryIToNurseryII STRING, -- Changed from UK to STRING
# MAGIC     DocumentedInNurseryII BOOLEAN,
# MAGIC     PRIMARY KEY (id),
# MAGIC     FOREIGN KEY (colonyID) REFERENCES `main`.silver.Colony(id)
# MAGIC     --FOREIGN KEY (NurseryID) REFERENCES Nursery(NurseryID)  --Removed FK because Nursery table does not exist
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS `main`.silver.FragmentationEvent(
# MAGIC     id STRING NOT NULL,
# MAGIC     colonyID STRING,
# MAGIC     destinationNumber INT,
# MAGIC     destinationSiteCellQuad STRING,
# MAGIC     destinationLocationStructure STRING,
# MAGIC     dateMovedToDestinationSite DATE,
# MAGIC     numberOfFragmentsProduced INT,
# MAGIC     donarColonyPurpuseSite STRING,
# MAGIC     PRIMARY KEY (id),
# MAGIC     FOREIGN KEY (colonyID) REFERENCES `main`.silver.Colony(id)
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

df_bronze = spark.read.table("main.bronze.`donor-colony`")


# COMMAND ----------

df_bronze.columns

# COMMAND ----------

# # Assuming colony_df holds the data with ColonyID generated in the previous step
# identification_df = df_bronze.select(
#     col("ColonyID"),
#     # Generating a composite key if needed (e.g., if multiple identifications per colony were possible)
#     # sha2(concat_ws('|', col('ColonyID'), col('Genus'), col('Species')), 256).alias("Composite_IdentificationID"),
#     col("ColonyID").alias("Composite_IdentificationID"), # Assuming ColonyID is the PK link
#     col("Genus"),
#     col("Species")
# ).filter(col("Genus").isNotNull() | col("Species").isNotNull()) # Only create rows if identification exists

# identification_df.write.format("delta").mode("overwrite").saveAsTable("Identification")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, DoubleType, DateType, BooleanType, StructType, StructField, ArrayType
from pyspark.sql.window import Window
import uuid


# COMMAND ----------

uuid_udf = F.udf(lambda: str(uuid.uuid4()), StringType())

# Function to safely convert 'YYMMDD' or similar strings to DateType
def parse_date_yyMMdd(date_col):
    return F.coalesce(
        F.to_date(date_col, 'yyMMdd'),
        F.to_date(date_col, 'yyyyMMdd'),
        F.to_date(date_col, 'MM/dd/yy'),
        F.to_date(date_col, 'yyyy-MM-dd')
    ).cast(DateType()) # Explicitly cast the result to DateType

def parse_boolean(bool_col_name):
    # Reference column by name passed as string
    return F.when(F.upper(F.trim(F.col(bool_col_name))).isin(['Y', 'YES', 'TRUE', '1']), True) \
            .when(F.upper(F.trim(F.col(bool_col_name))).isin(['N', 'NO', 'FALSE', '0']), False) \
            .otherwise(F.lit(None).cast(BooleanType())) # Ensure nulls are BooleanType



# COMMAND ----------

df_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Bronze Dataframe

# COMMAND ----------

# Create Sites
df_bronze_staged = df_bronze.withColumn("bronze_row_id", F.monotonically_increasing_id())
print(df_bronze_staged.columns)


# COMMAND ----------

# MAGIC %md
# MAGIC # Create Species identificaiton

# COMMAND ----------

df_bronze.head(5)

# COMMAND ----------


df_identification_silver = (df_bronze_staged # This has colony 'id' and 'bronze_row_id'
    .filter(F.col("Genus").isNotNull() | F.col("Species").isNotNull())
    .withColumn("identification_id", uuid_udf()).persist() # Generate unique ID for the identification record
    .select(
        F.col("identification_id").cast(StringType()).alias("id"),
        F.col("bronze_row_id"), # This is the Colony.id from df_colony_silver
        F.col("Genus").cast(StringType()).alias("genus"),
        F.col("Species").cast(StringType()).alias("species")
    )
    
)


df_identification_silver.head(5)

# COMMAND ----------

df_identification_silver.select("id","genus","species").dropDuplicates(subset=["genus","species"]).write\
 .format("delta")\
 .mode("overwrite")\
 .option("overwriteSchema", "false")\
 .saveAsTable("main.silver.Identification")

# COMMAND ----------

# MAGIC %md
# MAGIC - # Create Sites delta table

# COMMAND ----------

df_site_silver = (df_bronze
    .select("ReefName", "Site", "Latitude", "Longitude") # Use pre-cast numeric
    .filter(F.col("ReefName").isNotNull() | F.col("Site").isNotNull())
    .distinct()
    .withColumn("id", uuid_udf()).persist() # Generate unique site ID - already StringType
    .select(
        F.col("id").cast(StringType()).alias("id"), # Explicit cast for ID
        F.col("ReefName").cast(StringType()).alias("reefName"),
        F.col("Site").cast(StringType()).alias("site"),
        F.col("Latitude").cast(DoubleType()).alias("latitude"), # Use pre-cast double
        F.col("Longitude").cast(DoubleType()).alias("longitude") # Use pre-cast double
    )
)

df_site_silver = df_site_silver.dropDuplicates(subset=["Site", "ReefName"])



# COMMAND ----------

df_site_silver.head(25)  

# COMMAND ----------

df_site_silver.write\
 .format("delta")\
 .mode("overwrite")\
 .option("overwriteSchema", "false")\
 .saveAsTable("main.silver.Site")


# COMMAND ----------

df_bronze_staged.columns

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Colony

# COMMAND ----------

from pyspark.sql import functions as F


df_colony_silver_intermediate = (df_bronze_staged
    .withColumn("colony_id", uuid_udf()).persist() # Generate unique colony ID - already StringType
    .select(
        F.col("colony_id").cast(StringType()).alias("id"),
        F.col("Site").cast(StringType()).alias("siteID"), # From the join (already string)
        F.col("TagNumber").cast(IntegerType()).alias("tagNumber"), # Use pre-cast int
        F.col("Count").cast(IntegerType()).alias("count"), # Use pre-cast int
        F.col("TagColor").cast(StringType()).alias("tagColor"),
        F.col("Depth").cast(DoubleType()).alias("depth"), # Use pre-cast double
        F.col("GrowthForm").cast(StringType()).alias("growthForm"),
        F.col("MaximumDiameter").cast(DoubleType()).alias("maximumDiameter"), # Use pre-cast double
        F.col("Percent_Living_Tissue_______").cast(DoubleType()).alias("percentLivingTissue"), # Use pre-cast double
        parse_boolean("Image_collected__Y__N_").cast(BooleanType()).alias("imageCollected"), # Apply parser and cast
        F.col("Image_No").cast(StringType()).alias("imageNo"),
        # Keep Y/N as String based on schema, ensure it's cast
        parse_boolean("Positive_ID_made__Y/N_").cast(BooleanType()).alias("positiveIDMade"),
        parse_boolean("Colony_available_for_material").cast(BooleanType()).alias("colonyAvailableForMaterial"),
        F.to_date(F.col("DateCollected"), "d-MMM-yyyy").alias("dateCollected"),
        F.col("Collector").cast(StringType()).alias("collector"),
        F.col("Confidence").cast(StringType()).alias("confidence"),
        F.col("DonorReplicate").cast(IntegerType()).alias("donorReplicate"), # Use pre-cast int
        F.col("TempSpeciesName").cast(StringType()).alias("tempSpeciesName"),
        parse_boolean("SageReviewed").cast(BooleanType()).alias("sageReviewed"), # Apply parser and cast
        F.col("SageUpdates").cast(StringType()).alias("sageUpdates"),
        F.col("SampleType").cast(StringType()).alias("sampleType"),
        F.col("SampleCollection").cast(StringType()).alias("sampleCollection"),
        F.col("LocalSampleName").cast(StringType()).alias("localSampleName"),
        F.col("LocalSampleName_ver2").cast(StringType()).alias("localSampleName_ver2"),
        F.col("LocalSampleName_ver1").cast(StringType()).alias("localSampleName_ver1"),
        parse_boolean("Geneticample_Collected__Y_N_").cast(BooleanType()).alias("geneticSampleCollected"), # Apply parser and cast
        parse_date_yyMMdd("Date_of_Geneticample_Collection__YYMMDD_").cast(DateType()).alias("dateOfGeneticSampleCollection"),
        F.col("bronze_row_id")
    )
)

# 3. Join the two silver dataframes
df_colony_silver = (df_colony_silver_intermediate
    .join(
        df_site_silver.selectExpr(
        "site",                 # Keep the 'site' column (join key)
        "id AS site_uuid"),      # Alias 'site_uuid' to 'uuid' using SQL syntax
        df_colony_silver_intermediate["siteID"] == df_site_silver["site"], # Join on site name
        "left" # Keep all colonies
    ).join(
        df_identification_silver.selectExpr(
        "id AS identificationID",                 # Keep the 'site' column (join key)
        "bronze_row_id"),      # Alias 'site_uuid' to 'uuid' using SQL syntax
        df_colony_silver_intermediate["bronze_row_id"] == df_identification_silver["bronze_row_id"], # Join on site name
        "left" # Keep all colonies
    )
    .select(
        F.col("id"), # Colony UUID
        F.col("site_uuid").alias("siteID"),
        F.col("identificationID").alias("identificationID"),
        # Select all other columns from df_colony_silver_intermediate *except* the original siteID (name)
        *[df_colony_silver_intermediate[c] for c in df_colony_silver_intermediate.columns if c not in ['id', 'siteID','bronze_row_id','identificationID']]
    )
)

# COMMAND ----------

df_colony_silver.head(100)

# COMMAND ----------

df_colony_silver.write\
    .format("delta")\
    .mode("overwrite")\
    .option("overwriteSchema", "false")\
    .saveAsTable("main.silver.Colony")

# COMMAND ----------

print(df_bronze_staged.columns)

# COMMAND ----------

print(f"Processing Silver Table")
from pyspark.sql.functions import to_date, regexp_replace, col

df_colony_silver = spark.table('main.silver.Colony') # Replace with actual table name# Select colony id (aliased 'id' in df_colony_silver), donor site id, and destination columns

df_fragmentation_base = df_colony_silver_intermediate.join( # df_colony_silver has colony 'id', 'siteID', 'bronze_row_id'
    df_bronze_staged.select("bronze_row_id", *[c for c in df_bronze.columns if c.startswith("Destination_") or c.startswith("Donor_Colony_Purpose_Site_") or c.startswith("Number_of_Fragments_produced_Site_") or c.startswith("Date_moved_to_Destination_Site_")]),
    "bronze_row_id",
    "inner"
).select(
    F.col("id").alias("colonyID"), # Colony ID from df_colony_silver
    F.col("siteID"), # Donor Site ID from df_colony_silver
    F.col("*") # Select all the destination related columns from bronze
)

df_fragmentation_base = df_fragmentation_base.withColumnRenamed("Destination_Site_6__CellQuad_2", "Destination_Site_6__CellQuad_")
df_fragmentation_base = df_fragmentation_base.withColumnRenamed("Destination_Location__Structure_", "Destination_Location__Structure__1")
df_fragmentation_base = df_fragmentation_base.withColumn("Date_moved_to_Destination_Site_1", F.col("DateCollected"))


destination_cols = []
for i in range(1, 11):
    cell_quad_col = f"Destination_Site_{i}__CellQuad_" 
    if f"Destination_Site_{i}__CellQuad_" not in df_fragmentation_base.columns and cell_quad_col not in df_fragmentation_base.columns: raise Exception(f"{cell_quad_col}")

    location_col = f"Destination_Location__Structure__{i}"
    purpose_col = f"Donor_Colony_Purpose_Site_{i}"
    fragments_col = f"Number_of_Fragments_produced_Site_{i}"
    date_moved_col = f"Date_moved_to_Destination_Site_{i}"

    # print(location_col)
    # print(purpose_col)
    # print(fragments_col)
    # print(date_moved_col)

    if not all(c in df_fragmentation_base.columns for c in [cell_quad_col, location_col, purpose_col, fragments_col, date_moved_col]):
         for c in [cell_quad_col, location_col, purpose_col, fragments_col, date_moved_col]:
             if c not in df_fragmentation_base.columns:
                 print(c)
                 raise Exception(f"column {c} does not exist")
         continue

    destination_cols.append(
        F.struct(
            F.lit(i).cast(IntegerType()).alias("destinationNumber"), # Cast number literal
            F.col(cell_quad_col).cast(StringType()).alias("destinationSiteCellQuad"), # Cast string
            F.col(location_col).cast(StringType()).alias("destinationLocationStructure"), # Cast string
            F.col(purpose_col).cast(StringType()).alias("donarColonyPurpuseSite"), # Cast string
            F.when(F.to_date(F.trim(F.col(date_moved_col)), "yyyy-MM-dd").isNotNull(), F.to_date(F.trim(F.col(date_moved_col)), "yyyy-MM-dd"))
                  .when(F.to_date(F.trim(F.col(date_moved_col)), "dd/MM/yyyy").isNotNull(), F.to_date(F.trim(F.col(date_moved_col)), "dd/MM/yyyy"))
                  .otherwise(None)
            .alias("dateMovedToDestinationSite"),
            F.col(fragments_col).cast(IntegerType()).alias("numberOfFragmentsProduced") # Cast int
        ).alias(f"dest_{i}")
    )

# combine all destination columns into one main column called destinations
df_fragmentation_unpivoted = df_fragmentation_base.withColumn("destinations", F.array(*destination_cols))
# explode each group of destination cols into separate row 
df_fragmentation_exploded = df_fragmentation_unpivoted.select("colonyID", "siteID", F.explode("destinations").alias("destination_data"))
df_fragmentation_exploded.show(200, truncate=False)

# now finaly for each separate row get the desired columns
df_fragmentation_silver = (df_fragmentation_exploded
    # Select fields from struct, applying final casts
    .select(
        F.col("colonyID").cast(StringType()), # Ensure colonyID is string
        F.col("siteID").cast(StringType()), # Ensure siteID is string
        F.col("destination_data.destinationNumber").cast(IntegerType()).alias("destinationNumber"),
        F.col("destination_data.destinationSiteCellQuad").cast(StringType()).alias("destinationSiteCellQuad"),
        F.col("destination_data.destinationLocationStructure").cast(StringType()).alias("destinationLocationStructure"),
        F.col("destination_data.dateMovedToDestinationSite").cast(DateType()).alias("dateMovedToDestinationSite"),
        F.col("destination_data.numberOfFragmentsProduced").cast(IntegerType()).alias("numberOfFragmentsProduced"),
        F.col("destination_data.donarColonyPurpuseSite").cast(StringType()).alias("donarColonyPurpuseSite") # Typo matches schema
    )
    #.filter(F.col("destinationSiteCellQuad").isNotNull() | F.col("destinationLocationStructure").isNotNull()  | F.col("numberOfFragmentsProduced").isNotNull())
    .withColumn("frag_id", uuid_udf()) # Generate UUID
        # Reorder columns and ensure final ID cast
    .select(
        F.col("frag_id").cast(StringType()).alias("id"),
        F.col("colonyID"),
        F.col("destinationNumber"),
        F.col("destinationSiteCellQuad"),
        F.col("destinationLocationStructure"),
        F.col("dateMovedToDestinationSite"),
        F.col("numberOfFragmentsProduced"),
        F.col("donarColonyPurpuseSite")
        )
)


df_fragmentation_silver.write\
    .format("delta")\
    .mode("overwrite")\
    .option("overwriteSchema", "false")\
    .saveAsTable("main.silver.FragmentationEvent")

# COMMAND ----------

print(f"Processing Silver Table: Nursery Notes")

# Select relevant columns including the colony ID
df_nursery_base = df_colony_silver_intermediate.join(
    df_bronze_staged.select(
        "bronze_row_id",
        F.col("Documented_in_Nursery_I__red_text_okayed__bold_need_to_address").cast(StringType()).alias("DocumentedInNurseryI"),
        F.col("NOTES").cast(StringType()).alias("Notes"),
        parse_boolean("Documented_intructure").cast(BooleanType()).alias("DocumentedIntructure"),
        F.col("Coordinate_Notes").cast(StringType()).alias("CoordinateNotes"),
        F.col("Notes_from_Nursery_I_matchpecies_in_Nursery_II?").cast(StringType()).alias("NotesMispechiesFromNurseryIToNurseryII"),
        parse_boolean("Documented_in_Nursery_II").cast(BooleanType()).alias("DocumentedInNurseryII")
    ),
    "bronze_row_id",
    "inner"
    ).select(
        F.col("id").alias("colonyID"), # Colony ID from df_colony_silver
        F.col("*") # Select all the destination related columns from bronze
    )


df_nursery_base.withColumn("document_id", uuid_udf()).persist().select(
                            F.col("document_id").alias("id"),
                            "colonyID",
                            "DocumentedInNurseryI",
                            "Notes", 
                            "DocumentedIntructure", 
                            "CoordinateNotes", 
                            "NotesMispechiesFromNurseryIToNurseryII", 
                            "DocumentedInNurseryII").write\
                                                    .format("delta")\
                                                    .mode("overwrite")\
                                                    .option("overwriteSchema", "false")\
                                                    .saveAsTable("main.silver.ColonyNurseryDocument")


# COMMAND ----------

# Use df_colony_silver (includes Colony.id aliased as 'id') joined back to bronze_staged
df_colony_silver.show(10, truncate=True)


# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG main;
# MAGIC
# MAGIC SELECT current_catalog();
# MAGIC
# MAGIC

# COMMAND ----------

