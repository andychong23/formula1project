# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Aggregation Functions demo

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ##### Built-in aggregation functions
# MAGIC Aggregation functions will aggregate on the whole column unless Group By is used

# COMMAND ----------


race_results_df = spark.read.parquet(f'{presentation_folder_path}/race_results')

# COMMAND ----------

demo_df = race_results_df.filter(race_results_df['race_year'] == 2020)

# COMMAND ----------

display(demo_df)

# COMMAND ----------

from pyspark.sql.functions import count, count_distinct, sum

# COMMAND ----------

# Aggregation functions should be used with select since it will return a Column object which will be used in select
count('race_name')

# COMMAND ----------

# Using the aggregation function to count the number of entries
demo_df.select(count('race_name')).show()

# COMMAND ----------

# To get the number of unique races, we can use count_distinct
# And similar to count(), it will return a column object which can be used in other dataframes as well
count_distinct('race_name')

# COMMAND ----------

demo_df.select(count_distinct('race_name')).show()

# COMMAND ----------

# To get the sum of values in a certain column, we can use the sum aggregate function from pyspark.sql.functions
# if column is a string, sum will return null
sum('points')

# COMMAND ----------

demo_df.select(sum('points')).show()

# COMMAND ----------

# If we want to get the number of points of a specific individual, we can apply the filter function first
# In this example, we will check the number of points Lewis Hamilton scored
demo_df.filter(demo_df['driver_name'] == 'Lewis Hamilton').select(sum('points')).show()

# COMMAND ----------

# Under the select method, if we want to include more aggregated columns, we can do so as well by just passing in more parameters that either correspond to a Column object or a column_name in string
# We can use the .withColumnRenamed to rename the ugly columns
demo_df.filter(demo_df['driver_name'] == 'Lewis Hamilton').select(sum('points'), count_distinct('race_name')) \
    .withColumnRenamed('sum(points)', 'total_points') \
    .withColumnRenamed('count(DISTINCT race_name)', 'number_of_races') \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### GroupBy aggregations
# MAGIC
# MAGIC When using group by on a dataframe, spark returns a Grouped Data object which we can apply aggregations to

# COMMAND ----------

# Example of return object of the groupby method
demo_df.groupBy('driver_name')

# COMMAND ----------

# Getting sum of points on our Grouped Data object
# You cannot chain aggregation methods together becuase .sum on a GroupedData object returns a DataFrame and DataFrames cannot be aggregated on 
demo_df.groupBy('driver_name') \
    .sum('points').show()

# COMMAND ----------

# To get more than 1 aggregation function, we can use the agg(*exprs) method in order to provide all the different aggregations that we want to do on the GroupedData object\
# You can provide alias on Col objects, in this case, aggregation functions will return Col objects, thus, we can assign an alias to them
demo_df.groupBy('driver_name') \
    .agg(sum('points').alias('total_points'), count_distinct('race_name').alias('number_of_races')) \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Window Functions
# MAGIC
# MAGIC There are 3 conditions to look out for:
# MAGIC 1. How do we want to partition the data
# MAGIC 2. How do we want to order each partition of data
# MAGIC 3. What function do we want to apply to product an output

# COMMAND ----------

# Getting a new_df where window functions makes sense to be used
# We use Spark SQL statements here as they are easier to write than the pythonic way
demo_df = race_results_df.filter("race_year in ('2019', '2020')")

# COMMAND ----------

display(demo_df)

# COMMAND ----------

# Using groupBy with more than 1 column
# We do not have to specify that we have to select the group by columns as it will be automatically selected
demo_grouped_df = demo_df.groupBy('race_year', 'driver_name') \
    .agg(sum('points').alias('total_points'), count_distinct('race_name').alias('number_of_races'))

# COMMAND ----------

display(demo_grouped_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col

# we want to partition by race_year, order by points 
driver_rank_spec = Window.partitionBy(col('race_year')).orderBy(col('total_points').desc())

# This statement says that, I will create a new column 'rank', where we apply the function rank over the window specification
demo_grouped_df.withColumn('rank', rank().over(driver_rank_spec)).show(100)

# COMMAND ----------


