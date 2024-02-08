# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### In this section, we will be ingesting the results.json file which is a single line json file
# MAGIC
# MAGIC Requirements:
# MAGIC 1. Rename resultId to result_id
# MAGIC 2. Rename raceId to race_id
# MAGIC 3. Rename driverId to driver_id
# MAGIC 4. Rename constructorId to constructor_id
# MAGIC 5. Rename positionText to position_text
# MAGIC 6. Rename positionOrder to position_order
# MAGIC 7. Rename fastestLap to fastest_lap
# MAGIC 8. Rename fastestLapTime to fastest_lap_time
# MAGIC 9. Rename fastestLapSpeed to fastest_lap_speed
# MAGIC 10. Add ingestion_date
# MAGIC 11. Drop statusId
# MAGIC 12. Save the file in parquet format and partition by race_id
# MAGIC
# MAGIC ### This file also showcases the different methods to do incremental data loading
# MAGIC Actually, in order to do incremental data loading, all data must by partitioned
# MAGIC * In method 1, we are actively dropping the partitions created when the condition is met
# MAGIC * In method 2, we are overwriting the data present by checking using the partition column (its implemented by an update clause probably)
# MAGIC
# MAGIC

# COMMAND ----------

dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text('p_file_date', '')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 1: Read results.json using pyspark dataframe reader API with the correct schema

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, FloatType

# COMMAND ----------

results_schema = StructType(fields=[
    StructField('resultId', IntegerType(), False),
    StructField('raceId', IntegerType(), False),
    StructField('driverId', IntegerType(), False),
    StructField('constructorId', IntegerType(), False),
    StructField('number', IntegerType(), True),
    StructField('grid', IntegerType(), False),
    StructField('position', IntegerType(), True),
    StructField('positionText', StringType(), False),
    StructField('positionOrder', IntegerType(), False),
    StructField('points', FloatType(), False),
    StructField('laps', IntegerType(), False),
    StructField('time', StringType(), True),
    StructField('milliseconds', IntegerType(), True),
    StructField('fastestLap', IntegerType(), True),
    StructField('rank', IntegerType(), True),
    StructField('fastestLapTime', StringType(), True),
    StructField('fastestLapSpeed', StringType(), True),
    StructField('statusId', IntegerType(), False)
])

# COMMAND ----------

results_df = spark.read.schema(results_schema).json(f'{raw_folder_path}/{v_file_date}/results.json')

# COMMAND ----------

results_df.columns

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 2: Rename columns

# COMMAND ----------

results_renamed_df = results_df.withColumnRenamed('resultId', 'result_id') \
    .withColumnRenamed('raceId', 'race_id') \
    .withColumnRenamed('driverId', 'driver_id') \
    .withColumnRenamed('constructorId', 'constructor_id') \
    .withColumnRenamed('positionText', 'position_text') \
    .withColumnRenamed('positionOrder', 'position_order') \
    .withColumnRenamed('fastestLap', 'fastest_lap') \
    .withColumnRenamed('fastestLapTime', 'fastest_lap_time') \
    .withColumnRenamed('fastestLapSpeed', 'fastest_lap_speed')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 3: Add ingestion_date

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

results_ingestion_df = add_ingestion_date(results_renamed_df).withColumn('data_source', lit(v_data_source)) \
    .withColumn('file_date', lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 4: Drop status id

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

results_final_df = results_ingestion_df.drop(col('statusId'))

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### Step 5: Save the file in parquet format and partition by race_id

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Method 1 of incremental data loading
# MAGIC Idea for this is quite simple where we drop what we had if it exists, then append the information back
# MAGIC
# MAGIC Steps:
# MAGIC 1. Ingest file in the folder with the given dates
# MAGIC 2. Loop through the race_id associated in the processed dataframe
# MAGIC 3. Drop all the partitions with the race_id that is in the processed dataframe
# MAGIC 4. Append the "new" data into the table so that we will not overwrite the whole table

# COMMAND ----------

# for race_id_list in results_final_df.select('race_id').distinct().collect():
#     # this if statement checks if the table exists, if table does not exists, the ALTER TABLE WILL FAIL
#     if (spark._jsparkSession.catalog().tableExists('f1_processed.results')):
#         # spark sql statement to drop the partition if it exists
#         spark.sql(f'ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})')

# COMMAND ----------

# results_final_df.write.mode('append').partitionBy('race_id').format('parquet').saveAsTable('f1_processed.results')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Method 2 of incremental data loading
# MAGIC
# MAGIC Steps:
# MAGIC 1. Ingest file in the folder with the given dates
# MAGIC 2. Make the partition column to be the last column (have to reshift the data around)
# MAGIC 3. Set the spark config so that the partitionOverwriteMode is dynamic else it wont rewrite
# MAGIC 4. Allow the writer to overwrite and insertInto the table

# COMMAND ----------

# spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')

# COMMAND ----------

# results_final_df = results_final_df.select('result_id', 'driver_id', 'constructor_id', 'number', 'grid', 'position', 'position_text', 'position_order', 'points', 'laps', 'time', 'milliseconds', 'fastest_lap', 'rank', 'fastest_lap_time', 'fastest_lap_speed', 'data_source', 'file_date', 'ingestion_date', 'race_id')

# COMMAND ----------

# if (spark._jsparkSession.catalog().tableExists('f1_processed.results')):
#     # Spark assumes that the last column is the partition column if we use insertInto method
#     # this will append new data
#     results_final_df.write.mode('overwrite').insertInto('f1_processed.results')
# else:
#     # this creates the table the first time
#     results_final_df.write.mode('overwrite').partitionBy('race_id').format('parquet').saveAsTable('f1_processed.results')

# COMMAND ----------

second_incremental_data_loading(results_final_df, 'f1_processed.results', 'race_id')

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------


