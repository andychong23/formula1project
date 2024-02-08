# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Ingest pitstops.json file

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
# MAGIC #### Step 1: Read the JSON file using the pyspark dataframe reader API

# COMMAND ----------

display(dbutils.fs.ls(f'{raw_folder_path}'))

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, IntegerType

# COMMAND ----------

pit_stops_schema = StructType(fields=[
    StructField('raceId', IntegerType(), False),
    StructField('driverId', IntegerType(), True),
    StructField('stop', StringType(), True),
    StructField('lap', IntegerType(), True),
    StructField('time', StringType(), True),
    StructField('duration', StringType(), True),
    StructField('milliseconds', IntegerType(), True)
])

# COMMAND ----------

# need to specify multiLine as True if we need to process deeper into the JSON file
pit_stops_df = spark.read.schema(pit_stops_schema).option('multiLine', True) \
    .json(f'{raw_folder_path}/{v_file_date}/pit_stops.json')

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

pit_stops_final_df = add_ingestion_date(pit_stops_df.withColumnRenamed('driverId', 'driver_id') \
    .withColumnRenamed('raceId', 'race_id')) \
    .withColumn('data_source', lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 3: Write the output to processed containeer in parquet format

# COMMAND ----------

second_incremental_data_loading(pit_stops_final_df, 'f1_processed.pit_stops', 'race_id')

# COMMAND ----------

dbutils.notebook.exit('Success')
