# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #### <b> Access DataFrames using SQL </b>
# MAGIC
# MAGIC <b> Objectives </b>
# MAGIC 1. Create temporary views on dataframes
# MAGIC 2. Access the view from SQL cell
# MAGIC 3. Access the view from Python cell
# MAGIC
# MAGIC Note: This view is only accessible in this notebook and is only stored in this specific spark session

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f'{presentation_folder_path}/race_results')

# COMMAND ----------

# To create a temporary view, spark provides the method .createTempView(view_name) for all dataframes
# We use createOrReplaceTempView(view_name) so that no errors occur when the view_name is already being used
race_results_df.createOrReplaceTempView('v_race_results')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * 
# MAGIC FROM V_RACE_RESULTS
# MAGIC WHERE RACE_YEAR = 2020

# COMMAND ----------

# Running SQL statements in a Python cell
# spark.sql allows us to read it into a dataframe
# If we want to use dynamic SQL statements, then we do it this way where we can utilize f-strings in python to achieve that
race_results_2019_df = spark.sql('SELECT * FROM V_RACE_RESULTS WHERE RACE_YEAR = 2019')

# COMMAND ----------

display(race_results_2019_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### Global Temporary Views
# MAGIC
# MAGIC 1. Create global temporary views on dataframes
# MAGIC 2. Access the view from SQL cell
# MAGIC 3. Access the view from Python cell
# MAGIC 4. Access the view from another notebook
# MAGIC
# MAGIC Note: Global Temporary views are attached to the cluster

# COMMAND ----------

# This method will create a global temporary view that is attached to the cluster instead of the specific spark session

race_results_df.createOrReplaceGlobalTempView('gv_race_results')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW TABLES IN global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- THIS WILL NOT WORK AS THE GLOBAL TEMP VIEW IS REGISTERED IN SPARK'S DATABASE CALLED GLOBAL_TEMP
# MAGIC -- SELECT * 
# MAGIC -- FROM GV_RACE_RESULTS;
# MAGIC
# MAGIC -- TO ACCESS THE GLOBAL TEMP VIEW, WE NEED TO APPEND A global_temp. prefix to the table name like:
# MAGIC SELECT * 
# MAGIC FROM global_temp.GV_RACE_RESULTS;

# COMMAND ----------

spark.sql('SELECT * FROM global_temp.GV_RACE_RESULTS').show()

# COMMAND ----------


