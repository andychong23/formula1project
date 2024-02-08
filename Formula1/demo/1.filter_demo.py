# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #### Trying out the filter condition

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f'{processed_folder_path}/races')

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Using the SQL way to write filters

# COMMAND ----------

filtered_df = races_df.filter('race_year = 2019 and round <=5')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Using the pythonic way to write filters

# COMMAND ----------

'''
We use the & operator here as it signals to python to do an element wise boolean check
Underlying this, we cannot use the "and" operator here as python will be unable to properly evaluate what is true, does it evaluate the content or does it evaluate the array

To use or, have to use '|'
'''
filtered_df = races_df.filter((races_df['race_year'] == 2019) & (races_df['round'] <= 5))

# COMMAND ----------

display(filtered_df)

# COMMAND ----------


