# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Explore DBFS Root
# MAGIC 1. List all the folders in DBFS Root
# MAGIC 2. Interact with DBFS File Browser
# MAGIC 3. Upload File to DBFS Root
# MAGIC
# MAGIC DBFS is just a temporary storage and its just an abstraction to manage workspace storage, storage will be dropped when the workspace is deleted <br>
# MAGIC A mount allows you to read data without using access keys or SAS token <br>
# MAGIC FileStore is available to everyone and cannot be restricted to a specific user

# COMMAND ----------

display(dbutils.fs.ls('/'))

# COMMAND ----------

display(dbutils.fs.ls('/FileStore'))

# COMMAND ----------

# dont need the dbfs: as it is already mounted into the FileStore Folder
display(spark.read.csv('/FileStore/circuits.csv'))

# COMMAND ----------


