-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_processed
-- The location keyword here will suggest where should the data should be stored else when created else it will be stored under databrick's default location
-- Cause when we write the ingested dataframe into the database, we will need to write the raw data somewhere which is specified in the LOCATION heree
-- Then we can save the raw data as a table into the f1_processed database for analyst to extract the data
LOCATION 'abfss://processed@newdatabrickscoursedl.dfs.core.windows.net/'

/* Creating a database called f1_processed
During ingestion, when we save as a table under the database "f1_processed",
a folder with the table_name will be created to store the underlying information, so we can still think of
the writing portion as actually writing to the same storage location just that the database can store it as a table
*/

-- COMMAND ----------


