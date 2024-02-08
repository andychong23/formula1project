# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Ingest qualifying folder

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

display(dbutils.fs.ls(f'{raw_folder_path}'))

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### Step 1: Create the schema for the json file and ingest the json file

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType

# COMMAND ----------

# you will only need to include nested json schemas if there are objects within the object
qualifying_schema = StructType(fields=[
    StructField('qualifyId', IntegerType(), False),
    StructField('raceId', IntegerType(), False),
    StructField('driverId', IntegerType(), False),
    StructField('constructorId', IntegerType(), False),
    StructField('number', IntegerType(), False),
    StructField('position', IntegerType(), True),
    StructField('q1', StringType(), True),
    StructField('q2', StringType(), True),
    StructField('q3', StringType(), True)
])

# COMMAND ----------

# Multiline will read the entire json record together instead of a line by itself
qualifying_df = spark.read.option('multiLine', True) \
    .schema(qualifying_schema).json(f'{raw_folder_path}/{v_file_date}/qualifying/') 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 2: Add ingestion date and renamed certain columns

# COMMAND ----------

from pyspark.sql.functions import col, lit

# COMMAND ----------

final_df = add_ingestion_date(qualifying_df.withColumnRenamed('qualifyId', 'qualify_id') \
    .withColumnRenamed('raceId', 'race_id') \
    .withColumnRenamed('driverId', 'driver_id') \
    .withColumnRenamed('constructorId', 'constructor_id')) \
    .withColumn('data_source', lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 3: Write data as parquet format

# COMMAND ----------

second_incremental_data_loading(final_df, 'f1_processed.qualifying', 'race_id')

# COMMAND ----------

dbutils.notebook.exit('Success')
