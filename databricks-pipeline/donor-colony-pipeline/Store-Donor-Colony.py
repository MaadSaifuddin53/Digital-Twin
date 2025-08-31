# Databricks notebook source
# MAGIC %md
# MAGIC Connect to DB
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Get DB Credentials

# COMMAND ----------

db_user = dbutils.secrets.get(scope="databricks-keyvault", key="db-user")
db_password = dbutils.secrets.get(scope="databricks-keyvault", key="db-password")
db_host = dbutils.secrets.get(scope="databricks-keyvault", key="db-host")
db_name = dbutils.secrets.get(scope="databricks-keyvault", key="db-name")
db_port = dbutils.secrets.get(scope="databricks-keyvault", key="db-port")


jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"
connection_properties = {
    "user": db_user,
    "password": db_password,
    "driver": "org.postgresql.Driver"
}



# COMMAND ----------

# MAGIC %md
# MAGIC # Get Silver Deltatable

# COMMAND ----------

df = spark.table('main.silver.Colony') 

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Store data to DB
# MAGIC
# MAGIC - Overwrite schema and data

# COMMAND ----------

df.write \
  .format("jdbc") \
  .option("url", jdbc_url) \
  .option("dbtable", "public.inventory_table") \
  .option("user", connection_properties["user"]) \
  .option("password", connection_properties["password"]) \
  .option("driver", connection_properties["driver"]) \
  .mode("overwrite") \
  .save()

# COMMAND ----------

