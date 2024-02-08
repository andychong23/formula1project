-- Databricks notebook source
USE f1_presentation

-- COMMAND ----------

DESC driver_standings

-- COMMAND ----------

-- Temp views will be created in the database that we are working in
CREATE OR REPLACE TEMP VIEW v_driver_standings_2018
AS
SELECT race_year, driver_name, team, total_points, wins, rank
FROM driver_standings
WHERE race_year = 2018

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_driver_standings_2020
AS
SELECT race_year, driver_name, team, total_points, wins, rank
FROM driver_standings
WHERE race_year = 2020

-- COMMAND ----------

SELECT * FROM v_driver_standings_2018

-- COMMAND ----------

SELECT * FROM v_driver_standings_2020

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Inner Join

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2018 d_2018
INNER JOin v_driver_standings_2020 d_2020 ON d_2018.driver_name = d_2020.driver_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Left Join

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2018 d_2018
LEFT JOin v_driver_standings_2020 d_2020 ON d_2018.driver_name = d_2020.driver_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Right Join

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2018 d_2018
RIGHT JOIN v_driver_standings_2020 d_2020 ON d_2018.driver_name = d_2020.driver_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Full Join

-- COMMAND ----------

-- Do a Left Join, Right Join, Remove all duplicates
SELECT *
FROM v_driver_standings_2018 d_2018
FULL JOIN v_driver_standings_2020 d_2020 ON d_2018.driver_name = d_2020.driver_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Semi Join
-- MAGIC
-- MAGIC Inner join with only the data from the left table

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2018 d_2018
SEMI JOIN v_driver_standings_2020 d_2020 ON d_2018.driver_name = d_2020.driver_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Anti Join
-- MAGIC
-- MAGIC Complement of the Semi Join

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2018 d_2018
ANTI JOIN v_driver_standings_2020 d_2020 ON d_2018.driver_name = d_2020.driver_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Cross Join
-- MAGIC
-- MAGIC For each entry on the left table, join with each entry in the right table

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2018 d_2018
CROSS JOIN v_driver_standings_2020 d_2020

-- COMMAND ----------


