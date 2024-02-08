# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Ingest lap times folder

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
# MAGIC #### Step 1: Read the set of CSV files in the folder using the pyspark dataframe reader API

# COMMAND ----------

display(dbutils.fs.ls(f'{raw_folder_path}'))

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, IntegerType

# COMMAND ----------

lap_times_schema = StructType(fields=[
    StructField('raceId', IntegerType(), False),
    StructField('driverId', IntegerType(), True),
    StructField('lap', IntegerType(), True),
    StructField('position', IntegerType(), True),
    StructField('time', StringType(), True),
    StructField('milliseconds', IntegerType(), True)
])

# COMMAND ----------

# can just pass in the folder as the input and it will loop through the folder itself and combine everything in to a dataframe for me
# can use "*" as a wildcard character similar to what is available in linux since it follows the file system operations
lap_times_df = spark.read.schema(lap_times_schema) \
    .csv(f'{raw_folder_path}/{v_file_date}/lap_times/')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 2: Renamed columns and add new columns
# MAGIC 1. Rename driverId to driver_id
# MAGIC 2. Rename raceId to race_id
# MAGIC 3. Add ingestion_date column

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

final_df = add_ingestion_date(lap_times_df.withColumnRenamed('driverId', 'driver_id') \
    .withColumnRenamed('raceId', 'race_id')) \
    .withColumn('data_source', lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 3: Write the output to processed containeer in parquet format

# COMMAND ----------

second_incremental_data_loading(final_df, 'f1_processed.lap_times', 'race_id')

# COMMAND ----------

dbutils.notebook.exit('Success')
