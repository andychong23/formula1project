# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### In this section, we will be coming up with the constructors standings
# MAGIC
# MAGIC We will require
# MAGIC 1. Team name
# MAGIC 2. Number of wins
# MAGIC 3. Points
# MAGIC 4. Rank
# MAGIC
# MAGIC We can do this in 3 main steps:
# MAGIC 1. We can achieve this from the race_results by grouping by team, race_year (because we want an entry for each team in each year)
# MAGIC 2. We can then apply aggregate functions to count the number of wins (apply when position == 1), and sum the number of points
# MAGIC 3. We can then aply the window function to get the rank

# COMMAND ----------

race_results_df = spark.read.parquet(f'{presentation_folder_path}/race_results')

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 1: Group race_results by team and race_year

# COMMAND ----------

grouped_df_object = race_results_df.groupBy('team', 'race_year')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 2: Apply aggregate functions

# COMMAND ----------

from pyspark.sql.functions import col, sum, count, when

aggregated_df = grouped_df_object.agg(
    sum('points').alias('total_points'),
    count(when(col('position') == 1, True)).alias('wins')
)

# COMMAND ----------

display(aggregated_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 3: Apply Window functions

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank

# Partition by race_year then sort by total_points and tiebreak by wins
# This is the window function
constructor_rank_spec = Window.partitionBy('race_year').orderBy(col('total_points').desc(), col('wins').desc())

# Get the final_df
final_df = aggregated_df.withColumn('rank', rank().over(constructor_rank_spec))

# COMMAND ----------

display(final_df.filter('race_year = 2020'))

# COMMAND ----------

final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_presentation.constructor_standings')
