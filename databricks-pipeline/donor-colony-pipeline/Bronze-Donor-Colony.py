# Databricks notebook source
# MAGIC %md
# MAGIC # Import Libraries

# COMMAND ----------

import re
from collections import Counter

# COMMAND ----------

dbutils.widgets.text("file_path", "")
file_path = dbutils.widgets.get("file_path")
print(file_path)

# COMMAND ----------

# MAGIC %md
# MAGIC # Get Databrick Secrets
# MAGIC Get Secrets for Mouning Bronze Storage 

# COMMAND ----------

client_id = dbutils.secrets.get(scope="databricks-keyvault", key="clientID")
client_secret = dbutils.secrets.get(scope="databricks-keyvault", key="clientSecret")



# COMMAND ----------

# MAGIC %md
# MAGIC # Mount Bronze ADLS Storage

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.datalaketest010.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.datalaketest010.dfs.core.windows.net", 
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.datalaketest010.dfs.core.windows.net", f"{client_id}")
spark.conf.set("fs.azure.account.oauth2.client.secret.datalaketest010.dfs.core.windows.net", f"{client_secret}")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.datalaketest010.dfs.core.windows.net", 
               "https://login.microsoftonline.com/c96ce56a-9324-4735-8602-90fe1c094d64/oauth2/token")



               

# COMMAND ----------

# MAGIC %md
# MAGIC # Example Read Table

# COMMAND ----------

df = spark.read.format("delta").load("abfss://bronze@datalaketest010.dfs.core.windows.net/bronze")
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Load Data
# MAGIC Load CSV data

# COMMAND ----------

csv_path = "abfss://bronze@datalaketest010.dfs.core.windows.net/ingestion/DonorList.csv"
df = spark.read.option("header", True).option("inferSchema", True).csv(csv_path)


# COMMAND ----------

# MAGIC %md
# MAGIC # Clean columns
# MAGIC Rename unstandard databricks catalog columns and escape bad characters

# COMMAND ----------

# Read CSV with schema inference

def clean_column(col_name):
    # Remove or replace invalid characters
    return re.sub(r"[ ,;{}()\n\t=#.]", "_", col_name.strip())

df_cleaned = df.toDF(*[clean_column(c) for c in df.columns])


# COMMAND ----------

# MAGIC %md
# MAGIC # Remove Duplicate column names 
# MAGIC Put name_{number} if column is duplicated

# COMMAND ----------


raw_names = df_cleaned.columns
cleaned_names = [clean_column(c) for c in raw_names]

# define counter
name_counts = Counter()
unique_cleaned_names = []

# Handle duplicates by appending suffixes
name_counts = Counter()
unique_cleaned_names = []

for name in cleaned_names:
    name_counts[name] += 1
    if name_counts[name] > 1:
        unique_cleaned_names.append(f"{name}_{name_counts[name]}")
    else:
        unique_cleaned_names.append(name)
# Show inferred schema (optional)

df_cleaned = df.toDF(*unique_cleaned_names)



# COMMAND ----------

# MAGIC %md
# MAGIC # PRint Schema

# COMMAND ----------

df_cleaned.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # Store to DeltaTable

# COMMAND ----------

# Write as a managed Delta table in Unity Catalog
df_cleaned.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true")\
    .saveAsTable("main.bronze.`donor-colony`")

# COMMAND ----------

