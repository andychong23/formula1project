# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Access Azure Data Lake Storage using SAS token
# MAGIC 1. Set the spark config for SAS token
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file
# MAGIC
# MAGIC Use abfs driver instead of html protocol for databricks as it is preferred, in this case, we will have to provide the URI for the file path <br>
# MAGIC <b><ins>"abfs[s]://[container_name]@[storage_account_name].dfs.core.windows.net" </b></ins>

# COMMAND ----------

dbutils.secrets.list(scope='formula1-scope')

# COMMAND ----------

sas_token = dbutils.secrets.get(scope='formula1-scope', key='newdatabrickscoursedl-demo-sas-token')

# COMMAND ----------

# Setting up the SAS configuration and the steps can be seen in the link below:
# https://learn.microsoft.com/en-us/azure/databricks/storage/azure-storage#access-azure-data-lake-storage-gen2-or-blob-storage-using-a-sas-token

spark.conf.set("fs.azure.account.auth.type.newdatabrickscoursedl.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.newdatabrickscoursedl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.newdatabrickscoursedl.dfs.core.windows.net", sas_token)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@newdatabrickscoursedl.dfs.core.windows.net"))

# COMMAND ----------

# To read the contents of the file, we can use the sparks dataframe api to do so
display(spark.read.csv("abfss://demo@newdatabrickscoursedl.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


