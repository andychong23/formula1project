# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Access Azure Data Lake Storage using access keys
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file
# MAGIC
# MAGIC Use abfs driver instead of html protocol for databricks as it is preferred, in this case, we will have to provide the URI for the file path <br>
# MAGIC <b><ins>"abfs[s]://[container_name]@[storage_account_name].dfs.core.windows.net" </b></ins>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Accessing Azure Data Lake Storage without any access keys

# COMMAND ----------

# We can use the file utilities object in databricks dbutils.fs to access the folder
dbutils.fs.ls("abfss://demo@newdatabrickscoursedl.dfs.core.windows.net")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Accessing Azure Data Lake Storage with access keys

# COMMAND ----------

account_key = dbutils.secrets.get(scope='formula1-scope', key='newdatabrickscoursedl-account-key')

# COMMAND ----------

# We will set the spark configuration here
# The configuration for spark has 2 parts
# Parameter 1: fs.azure.account.key.[end_point_to_storage_account]
# Parameter 2: storage account key
spark.conf.set(
    "fs.azure.account.key.newdatabrickscoursedl.dfs.core.windows.net",
    account_key
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@newdatabrickscoursedl.dfs.core.windows.net"))

# COMMAND ----------

# To read the contents of the file, we can use the sparks dataframe api to do so
display(spark.read.csv("abfss://demo@newdatabrickscoursedl.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


