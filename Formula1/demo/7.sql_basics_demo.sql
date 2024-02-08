-- Databricks notebook source
SHOW DATABASES

-- COMMAND ----------

SELECT CURRENT_DATABASE()

-- COMMAND ----------

-- To switch the current_database, we can use the "USE" keyword
USE f1_processed

-- COMMAND ----------

-- In this example, we will want to see all the data that has been ingested which is in the f1_processed database
-- We will need to list all the tables in that database in order to know what we can query
SHOW TABLES

-- COMMAND ----------

SELECT * 
FROM f1_processed.drivers
-- "LIMIT X" will show the first X rows
LIMIT 10;


-- COMMAND ----------

-- We can use describe to show the data_types
DESC f1_processed.drivers

-- COMMAND ----------

-- If we like all the drivers whose nationality is British
SELECT NAME, DOB DATE_OF_BIRTH
FROM f1_processed.drivers
-- Multiple WHERE clause
WHERE f1_processed.drivers.nationality = 'British'
AND DOB >= '1990-01-01'
-- ORDER BY clause, default is ascending
ORDER BY DATE_OF_BIRTH

-- COMMAND ----------

SELECT * 
FROM DRIVERS
-- Multiple ORDER BY clause
ORDER BY nationality ASC, dob DESC

-- COMMAND ----------

SELECT NAME, nationality, DOB DATE_OF_BIRTH
FROM f1_processed.drivers
WHERE (f1_processed.drivers.nationality = 'British'
AND DOB >= '1990-01-01') OR 
nationality = "Indian"
ORDER BY DATE_OF_BIRTH DESC

-- COMMAND ----------


