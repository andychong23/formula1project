-- Databricks notebook source
-- MAGIC %python
-- MAGIC
-- MAGIC # We can escape from all quotations by using triple quotes
-- MAGIC html = """<h1 style="color:Black;text-align:center;font-family:Arial">Report on Dominant Formula 1 Teams </h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

-- Only select the table where teams has more than 100 entries recorded and we rank them accordingly
CREATE OR REPLACE TEMP VIEW v_dominant_teams
AS
SELECT
  team_name,
  count(1) total_races,
  sum(calculated_points) total_points,
  avg(calculated_points) avg_points,
  RANK() OVER (ORDER BY avg(calculated_points) DESC) team_rank
FROM f1_presentation.calculated_race_results
GROUP BY team_name
-- exclude teams where there are too little races to count properly
HAVING COUNT(1) >= 100
ORDER BY avg_points DESC

-- COMMAND ----------

-- Visualize using line chart
SELECT
  race_year,
  team_name,
  count(1) total_races,
  sum(calculated_points) total_points,
  avg(calculated_points) avg_points
FROM f1_presentation.calculated_race_results
WHERE team_name in (SELECT team_name FROM v_dominant_teams WHERE team_rank <= 5)
GROUP BY race_year, team_name
ORDER BY race_year, avg_points DESC

-- COMMAND ----------

SELECT
  race_year,
  team_name,
  count(1) total_races,
  sum(calculated_points) total_points,
  avg(calculated_points) avg_points
FROM f1_presentation.calculated_race_results
WHERE team_name in (SELECT team_name FROM v_dominant_teams WHERE team_rank <= 5)
GROUP BY race_year, team_name
ORDER BY race_year, avg_points DESC

-- COMMAND ----------


