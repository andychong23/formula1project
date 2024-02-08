-- Databricks notebook source
-- MAGIC %python
-- MAGIC
-- MAGIC html = """<h1 style="color:black;text-align:center;font:Arial"> Report on F1 Dominant Drivers</h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_drivers 
AS
SELECT 
driver_name,
COUNT(1) total_races, 
SUM(calculated_points) total_points,
AVG(calculated_points) avg_points,
-- You cannot use a new column in the window function, window function operates on the original table
RANK() OVER(ORDER BY avg(calculated_points) DESC) driver_rank
FROM f1_presentation.calculated_race_results
GROUP BY driver_name
HAVING COUNT(1) >= 50
ORDER BY avg_points DESC

-- COMMAND ----------

SELECT 
race_year,
driver_name,
COUNT(1) total_races, 
SUM(calculated_points) total_points,
AVG(calculated_points) avg_points
FROM f1_presentation.calculated_race_results
-- selecting the top 10 dominant drivers and then filtering it in this new table to get the results for them only
WHERE driver_name IN (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank <= 10)
GROUP BY race_year, driver_name
ORDER BY race_year, avg_points DESC

-- COMMAND ----------

SELECT 
race_year,
driver_name,
COUNT(1) total_races, 
SUM(calculated_points) total_points,
AVG(calculated_points) avg_points
FROM f1_presentation.calculated_race_results
-- selecting the top 10 dominant drivers and then filtering it in this new table to get the results for them only
WHERE driver_name IN (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank <= 10)
GROUP BY race_year, driver_name
ORDER BY race_year, avg_points DESC

-- COMMAND ----------

SELECT 
race_year,
driver_name,
COUNT(1) total_races, 
SUM(calculated_points) total_points,
AVG(calculated_points) avg_points
FROM f1_presentation.calculated_race_results
-- selecting the top 10 dominant drivers and then filtering it in this new table to get the results for them only
WHERE driver_name IN (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank <= 10)
GROUP BY race_year, driver_name
ORDER BY race_year, avg_points DESC

-- COMMAND ----------


