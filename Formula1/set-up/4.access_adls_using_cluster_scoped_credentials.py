# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Access Azure Data Lake Storage using cluster scoped credentials
# MAGIC 1. Set the spark config fs.azure.account.key in the cluster
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file
# MAGIC
# MAGIC Use abfs driver instead of html protocol for databricks as it is preferred, in this case, we will have to provide the URI for the file path <br>
# MAGIC <b><ins>"abfs[s]://[container_name]@[storage_account_name].dfs.core.windows.net" </b></ins>
# MAGIC
# MAGIC #### You cannot use the dbutils.secrets package when configuring cluster scope credentials. You will have to configure it in the cluster permissions itself
# MAGIC To configure it in the cluster configuration, you will have to provide a key value pair separated by a space like:
# MAGIC fs.azure.account.key.<storage-account>.dfs.core.windows.net {{secrets/<secret-scope>/<secret-key>}}

# COMMAND ----------

# In this example, the spark configuration has been set within the cluster itself, the key-value pair in the cluster configuration follows the format below:
# [storage_account_end_point (can be accessed via endpoints settings in configuration under Data Lake Storage)] [access_key]

display(dbutils.fs.ls("abfss://demo@newdatabrickscoursedl.dfs.core.windows.net"))

# COMMAND ----------

# To read the contents of the file, we can use the sparks dataframe api to do so
display(spark.read.csv("abfss://demo@newdatabrickscoursedl.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


