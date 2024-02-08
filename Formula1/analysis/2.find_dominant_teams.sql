-- Databricks notebook source
USE f1_presentation;

-- COMMAND ----------

SELECT * FROM calculated_race_results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Find the most dominant teams, we want to group by the team_name and then get the total points, total races and average points

-- COMMAND ----------

SELECT
  team_name,
  count(1) total_races,
  sum(calculated_points) total_points,
  avg(calculated_points) avg_points
FROM calculated_race_results
GROUP BY team_name
-- exclude teams where there are too little races to count properly
HAVING COUNT(1) >= 100
ORDER BY avg_points DESC

-- COMMAND ----------

SELECT
  team_name,
  count(1) total_races,
  sum(calculated_points) total_points,
  avg(calculated_points) avg_points
FROM calculated_race_results
WHERE race_year BETWEEN 2011 AND 2020
GROUP BY team_name
-- exclude teams where there are too little races to count properly
HAVING COUNT(1) >= 100
ORDER BY avg_points DESC

-- COMMAND ----------

SELECT
  team_name,
  count(1) total_races,
  sum(calculated_points) total_points,
  avg(calculated_points) avg_points
FROM calculated_race_results
WHERE race_year BETWEEN 2001 AND 2011
GROUP BY team_name
-- exclude teams where there are too little races to count properly
HAVING COUNT(1) >= 100
ORDER BY avg_points DESC

-- COMMAND ----------


