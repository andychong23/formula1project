-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ### Since LOCATION is specified in all of the table creation, they are considered to be external tables

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Create Circuits Table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;

CREATE TABLE IF NOT EXISTS f1_raw.circuits (
  circuitId INT,
  circuitRef STRING,
  name STRING,
  location STRING,
  country STRING,
  lat DOUBLE,
  lng DOUBLE,
  alt INT,
  url STRING
)
USING csv
-- specifying the file path/folder path which Spark SQL can expect the file/folder to be at
LOCATION 'abfss://raw@newdatabrickscoursedl.dfs.core.windows.net/circuits.csv'
OPTIONS (header=true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Create Races table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;

CREATE TABLE IF NOT EXISTS f1_raw.races (
  raceId INT,
  year INT,
  round INT,
  circuitId INT,
  name STRING,
  date STRING,
  time STRING,
  url STRING
)
USING csv
LOCATION 'abfss://raw@newdatabrickscoursedl.dfs.core.windows.net/races.csv'
OPTIONS (header=true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Create tables for JSON files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Create constructors table
-- MAGIC * Single Line JSON
-- MAGIC * Simple Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;

-- If using JSON file, do not need to specify header is true, i think it is because it is a key-value pair so the header is implicit
CREATE TABLE IF NOT EXISTS f1_raw.constructors (
  constructorId INT, 
  constructorRef STRING, 
  name STRING, 
  nationality STRING, 
  url STRING
)
USING JSON
LOCATION 'abfss://raw@newdatabrickscoursedl.dfs.core.windows.net/constructors.json'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Create drivers table
-- MAGIC * Single Line JSON
-- MAGIC * Complex Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;

CREATE TABLE IF NOT EXISTS f1_raw.drivers (
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT<forename: STRING, surname: STRING>,
  nationality STRING,
  url STRING
)
USING JSON
LOCATION 'abfss://raw@newdatabrickscoursedl.dfs.core.windows.net/drivers.json'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Create results table
-- MAGIC * Single Line JSON
-- MAGIC * Simple Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;

CREATE TABLE IF NOT EXISTS f1_raw.results (
  resultId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  grid INT,
  position INT,
  positionText STRING,
  positionOrder INT,
  points FLOAT,
  laps INT,
  time STRING,
  milliseconds INT,
  fastestLap INT,
  rank INT,
  fastestLapTime STRING,
  fastestLapSpeed STRING,
  statusId INT
)
USING JSON
LOCATION 'abfss://raw@newdatabrickscoursedl.dfs.core.windows.net/results.json'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Create pitstops table
-- MAGIC * Multi-line JSON
-- MAGIC * Simple Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pitstops;

CREATE TABLE IF NOT EXISTS f1_raw.pitstops (
  raceId INT,
  driverId INT,
  stop STRING,
  lap INT,
  time STRING,
  duration STRING,
  milliseconds INT
)
USING JSON
LOCATION 'abfss://raw@newdatabrickscoursedl.dfs.core.windows.net/pit_stops.json'
OPTIONS (multiLine=true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Create tables for list of files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Create Lap Times files
-- MAGIC * CSV files
-- MAGIC * Multiple files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;

CREATE TABLE IF NOT EXISTS f1_raw.lap_times (
  raceId INT,
  driverId INT,
  lap INT,
  position INT,
  time STRING,
  milliseconds INT
)
USING CSV
LOCATION 'abfss://raw@newdatabrickscoursedl.dfs.core.windows.net/lap_times'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Create Qualifying Table
-- MAGIC * JSON file
-- MAGIC * MultiLine JSON 
-- MAGIC * Multiple files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;

CREATE TABLE IF NOT EXISTS f1_raw.qualifying (
  qualifyId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  position INT,
  q1 STRING,
  q2 STRING,
  q3 STRING
)
USING JSON
LOCATION 'abfss://raw@newdatabrickscoursedl.dfs.core.windows.net/qualifying'
OPTIONS (multiLine=true)

-- COMMAND ----------


