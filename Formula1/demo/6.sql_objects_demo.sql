-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Database basics
-- MAGIC
-- MAGIC #### Lesson Objectives
-- MAGIC 1. Spark SQL documentation
-- MAGIC 2. Create database demo
-- MAGIC 3. Data tab in the UI
-- MAGIC 4. SHOW command
-- MAGIC 5. DESCRIBE command
-- MAGIC 6. Find the current database

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ##### Creating database using SQL

-- COMMAND ----------

-- CREATE DATABASE STATEMENT
CREATE DATABASE DEMO;

-- COMMAND ----------

-- IT WILL THROW AN ERROR IF THE DATABASE ALREADY EXISTS
CREATE DATABASE DEMO;

-- COMMAND ----------

-- TO COUNTERACT THE ABOVE BEHAVIOUR, WE CAN MODIFY THE CREATE DATABASE STATEMENT WITH IF NOT EXISTS STATEMENT
-- This statement says to create a database if it doesnt exist
CREATE DATABASE IF NOT EXISTS DEMO;

-- COMMAND ----------

-- THIS COMMAND WILL LISTS ALL DATABASES IN THE HIVE METASTORE
SHOW DATABASES;

-- COMMAND ----------

-- THIS COMMAND WILL DESCRIBE A DATABASE
-- YOU CAN ADD THE EXTENDED STATEMENT TO SHOW THE PROPERTIES
DESCRIBE DATABASE DEMO

-- COMMAND ----------

-- THIS COMMAND WILL SHOW THE CURRENT DATABASE THAT WE ARE WORKING ON
SELECT current_database()

-- COMMAND ----------

-- THIS WILL SHOW ALL THE TABLES IN THE CURRENT DATABASE
-- SHOW TABLES;

-- TO SHOW ALL THE TABLES IN THE DEMO DATABASE, USE THE COMMAND BELOW
-- SHOW TABLES IN DEMO;

-- OR WE CAN CHANGE THE CURRENT DATABASE AS SHOWN BELOW
USE DEMO;

-- COMMAND ----------

-- This shows that the current database has been changed from default to demo
SELECT current_database();

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creating managed tables
-- MAGIC
-- MAGIC #### Learning Objectives:
-- MAGIC
-- MAGIC 1. Create managed table using Python
-- MAGIC 2. Create managed table using SQL
-- MAGIC 3. Effect of dropping a managed table
-- MAGIC 4. Describe table

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # We read the race_results from the presentation folder
-- MAGIC race_results_df = spark.read.parquet(f'{presentation_folder_path}/race_results')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # This statement writes the dataframe as a parquet file but we save the information from the parquet file as a table called "race_results_python" and this table is stored in the demo database
-- MAGIC # This is a managed table 
-- MAGIC race_results_df.write.format('parquet').saveAsTable('demo.race_results_python')

-- COMMAND ----------

-- THIS SHOWS THAT THE NEW TABLE HAS BEEN WRITTEN INTO THE HIVE METASTORE
SHOW TABLES;

-- COMMAND ----------

-- THIS COMMAND DESCRIBES THE TABLE RACE_RESULTS_PYTHON and the type of the table is managed
DESCRIBE EXTENDED RACE_RESULTS_PYTHON;

-- COMMAND ----------

SELECT *
FROM demo.RACE_RESULTS_PYTHON
WHERE race_year = 2020;

-- COMMAND ----------

-- CREATING TABLE USING SQL, WHERE THE TABLE RESULTS COME FROM THE SELECT STATEMENT
-- Since we did not specify a location, the default is to store it in the current_database() or in this case, it is specified to be in the demo database
-- Thus, this will become a managed table
CREATE TABLE demo.RACE_RESULTS_SQL
AS
-- THE TABLE IS POPULATED USING THE SELECT STATEMENT BELOW
SELECT *
FROM demo.RACE_RESULTS_PYTHON
WHERE race_year = 2020;

-- COMMAND ----------

DESCRIBE EXTENDED DEMO.RACE_RESULTS_SQL

-- COMMAND ----------

-- DROP TABLE USING SQL
DROP TABLE DEMO.RACE_RESULTS_SQL

-- COMMAND ----------

-- THIS SHOWS THE DEMO.RACE_RESULTS_SQL IS DROPPED AND IS NOT PRESENT IN HIVE METASTORE ANYMORE
-- All the raw data is lost as well
SHOW TABLES IN DEMO

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creating External Tables
-- MAGIC
-- MAGIC #### Learning Objectives:
-- MAGIC
-- MAGIC 1. Create external table using Python
-- MAGIC 2. Create external table using SQL
-- MAGIC 3. Effect of dropping an external table
-- MAGIC
-- MAGIC External tables are also known as unmanaged tables
-- MAGIC
-- MAGIC * Managed tables means that the metadata + data is stored in the hive meta storage <br>
-- MAGIC * Unmanaged tables means that only the metadata is stored in the hive meta storage
-- MAGIC
-- MAGIC * When dropping managed tables, you lose the original data as well <br>
-- MAGIC * But when dropping unmanged tables, you do not lose the original data

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # To break this statement down, we are writing race_results_df as a parquet file into the demo folder
-- MAGIC # If you specify the path when saving the table, then it will automatically treat it as an external table else it will save it into the hive meta store to treat it as a managed table
-- MAGIC # This will have an error if a folder already exists
-- MAGIC
-- MAGIC race_results_df.write.format('parquet').option('path', f'{demo_folder_path}/race_results_ext_py').saveAsTable('demo.race_results_ext_py')

-- COMMAND ----------

DESCRIBE EXTENDED DEMO.RACE_RESULTS_EXT_PY

-- COMMAND ----------

-- In this part, we will create a table in the database using SQL, this only creates a table in the database, there is no data added yet

CREATE TABLE demo.race_results_ext_sql (
  race_year INT,
  race_name STRING,
  race_data TIMESTAMP,
  circuit_location STRING,
  driver_name STRING,
  driver_number INT,
  driver_nationality STRING,
  team STRING,
  grid INT,
  fastest_lap INT,
  race_time STRING,
  points FLOAT,
  position INT,
  created_data TIMESTAMP
)
USING PARQUET -- If using a managed table, we can stop here but an external table needs to specify a location to put the data into which is shown in the next line
LOCATION "abfss://demo@newdatabrickscoursedl.dfs.core.windows.net/race_results_ext_sql" -- When we insert data into this table, the data will be stored as a parquet file in this specified location, if location exists, data will be read into the table

-- COMMAND ----------

SHOW TABLES IN DEMO

-- COMMAND ----------

-- To add data into the table defined above, we can use the INSERT INTO statement
INSERT INTO demo.race_results_ext_sql
SELECT * FROM DEMO.RACE_RESULTS_EXT_PY WHERE race_year = 2020;

-- This new information will be stored as a parquet file in the presentation folder

-- COMMAND ----------

SHOW TABLES IN DEMO

-- COMMAND ----------

DROP TABLE DEMO.RACE_RESULTS_EXT_SQL
-- Although table has been dropped from the hive metastore, the original data can be found in our azure blob storage containers

-- COMMAND ----------

SHOW TABLES IN DEMO

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Views on tables
-- MAGIC
-- MAGIC #### Learning Objectives:
-- MAGIC 1. Creating Temp view using SQL (only available in the spark session)
-- MAGIC 2. Creating Global Temp view using SQL (only availabe in the spark cluster)
-- MAGIC 3. Creating Permanent view using SQL
-- MAGIC
-- MAGIC Tables vs Views:
-- MAGIC * Tables have underlying data stored either in ABFS or ADLS
-- MAGIC * Views are just a visual representation of the data, it has no underlying data. The underlying function is to query the data

-- COMMAND ----------

-- Creating a Temp view
-- If using CREATE, command will fail when the view already exists
-- By using CREATE OR REPLACE, it will always replace the view with newer data

CREATE OR REPLACE TEMP VIEW v_race_results
AS
SELECT * 
FROM DEMO.race_results_python
WHERE RACE_YEAR = 2018;

-- COMMAND ----------

SELECT * FROM v_race_results

-- COMMAND ----------

-- Creating Global Temp Views requires just the addition of the GLOBAL keyword

CREATE OR REPLACE GLOBAL TEMP VIEW gv_race_results
AS
SELECT * 
FROM DEMO.race_results_python
WHERE RACE_YEAR = 2012;

-- COMMAND ----------

-- To view any global temp views, we have to select it from the global_temp database

SELECT * FROM GLOBAL_TEMP.gv_race_results

-- COMMAND ----------

-- This creates a permanent view

CREATE OR REPLACE VIEW demo.pv_race_results
AS
SELECT * 
FROM DEMO.race_results_python
WHERE RACE_YEAR = 2000;

-- COMMAND ----------


