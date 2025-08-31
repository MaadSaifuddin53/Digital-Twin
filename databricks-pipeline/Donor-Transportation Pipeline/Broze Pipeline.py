# Databricks notebook source
client_id = dbutils.secrets.get(scope="databricks-keyvault", key="clientID")
client_secret = dbutils.secrets.get(scope="databricks-keyvault", key="clientSecret")



spark.conf.set("fs.azure.account.auth.type.datalaketest010.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.datalaketest010.dfs.core.windows.net", 
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.datalaketest010.dfs.core.windows.net", f"{client_id}")
spark.conf.set("fs.azure.account.oauth2.client.secret.datalaketest010.dfs.core.windows.net", f"{client_secret}")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.datalaketest010.dfs.core.windows.net", 
               "https://login.microsoftonline.com/c96ce56a-9324-4735-8602-90fe1c094d64/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading File From Datalake**

# COMMAND ----------

csv_path = "abfss://bronze@datalaketest010.dfs.core.windows.net/ingestion/DonorTransportation.csv"
df = spark.read.option("header", True).option("inferSchema", False).csv(csv_path)

# COMMAND ----------

df.count()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.show(4)

# COMMAND ----------

 import re

# Read CSV with schema inference
def clean_column(col_name):
    # Remove or replace invalid characters
    return re.sub(r"[ ,;{}()\n\t=#.]", "_", col_name.strip())

df_cleaned = df.toDF(*[clean_column(c) for c in df.columns])
df_cleaned.columns

# COMMAND ----------

# MAGIC %md
# MAGIC **Analyzing Nulls Value**

# COMMAND ----------

from pyspark.sql.functions import col, sum as _sum

null_counts = df.select([
    _sum(col(c).isNull().cast("int")).alias(c)
    for c in df.columns
])

null_counts.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Save DataFrame as Delta Table**

# COMMAND ----------

df_cleaned.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("main.bronze.DonorTransportation")