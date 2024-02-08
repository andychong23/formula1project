-- Databricks notebook source
-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### In this section, we will be creating the table to aid in the analysis of the most dominant driver and team in the past years

-- COMMAND ----------

USE f1_processed;

-- COMMAND ----------

CREATE TABLE f1_presentation.calculated_race_results
-- this next line determines the how the data will be saved
USING parquet
AS
SELECT 
  races.race_year, 
  constructors.name team_name, 
  drivers.name driver_name, 
  results.position, 
  results.points,
  11 - results.position calculated_points
FROM results
INNER JOIN drivers on (results.driver_id = drivers.driver_id)
INNER JOIN constructors on (results.constructor_id = constructors.constructor_id)
INNER JOIN races on (results.race_id = races.race_id)
WHERE results.position <= 10;

 
-- From exploratory data analysis, we see that over the years, the number of points given for each race win is different
-- E.g In 1954, winning the race gave 8 points but in 2023, winning the race gave 25 points

-- Thus, to overcome this issue, we will recalculate the points ourselves where being in first place means that they have 10 points all the way down to the 10th position having 1 point. Although using 11 - race_position will generate either null values (if they don't complete the race) or negative points, we can ignore them by filtering them out

-- This works because at the end of the day, our purpose is to find the most dominant drivers

