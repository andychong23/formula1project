-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC #### Drop all the tables

-- COMMAND ----------

-- CASCADE will drop all the tables under the database and the database itself
DROP DATABASE IF EXISTS f1_processed CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "abfss://processed@newdatabrickscoursedl.dfs.core.windows.net/"

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_presentation CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "abfss://presentation@newdatabrickscoursedl.dfs.core.windows.net/"

-- COMMAND ----------


