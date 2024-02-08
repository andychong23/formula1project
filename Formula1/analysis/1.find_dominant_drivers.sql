-- Databricks notebook source
SELECT 
driver_name,
COUNT(1) total_races, 
SUM(calculated_points) total_points,
AVG(calculated_points) avg_points
FROM f1_presentation.calculated_race_results
-- Since we used an aggregation function, we need a group by, and since we are finding the most dominant drivers through the number of modified points they have, we can group by the driver_name
GROUP BY driver_name
-- HAVING clause works on aggregated data, while the WHERE clause works on row data
HAVING COUNT(1) >= 50
-- We then order the total_points to see who has the most points
-- However, if you race more, it is more likely that you get the most number of points
ORDER BY avg_points DESC

-- COMMAND ----------

SELECT 
driver_name,
COUNT(1) total_races, 
SUM(calculated_points) total_points,
AVG(calculated_points) avg_points
FROM f1_presentation.calculated_race_results
-- We filter row data based on the race_year first (prefilter)
WHERE race_year BETWEEN 2011 AND 2020
-- Since we used an aggregation function, we need a group by, and since we are finding the most dominant drivers through the number of modified points they have, we can group by the driver_name
GROUP BY driver_name
-- HAVING clause works on aggregated data (postfilter), filter on the aggregated results
HAVING COUNT(1) >= 50
-- We then order the total_points to see who has the most points
-- However, if you race more, it is more likely that you get the most number of points
ORDER BY avg_points DESC

-- COMMAND ----------

SELECT 
driver_name,
COUNT(1) total_races, 
SUM(calculated_points) total_points,
AVG(calculated_points) avg_points
FROM f1_presentation.calculated_race_results
-- We filter row data based on the race_year first (prefilter)
WHERE race_year BETWEEN 2001 AND 2010
-- Since we used an aggregation function, we need a group by, and since we are finding the most dominant drivers through the number of modified points they have, we can group by the driver_name
GROUP BY driver_name
-- HAVING clause works on aggregated data (postfilter), filter on the aggregated results
HAVING COUNT(1) >= 50
-- We then order the total_points to see who has the most points
-- However, if you race more, it is more likely that you get the most number of points
ORDER BY avg_points DESC

-- COMMAND ----------


