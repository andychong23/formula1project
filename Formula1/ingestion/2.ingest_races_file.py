# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #### In this section, we will be ingesting the races file and transforming it to the data we require and then upload it onto storage as a parquet file
# MAGIC There are 2 things that we need to do in this notebook
# MAGIC 1. Transform the dataframe into the requirements
# MAGIC 2. Write the file onto storage

# COMMAND ----------

dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text('p_file_date', '')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 1: Ingest data from storage
# MAGIC * Define the Schema for the columns
# MAGIC * Promote the headers

# COMMAND ----------

display(dbutils.fs.ls(f'{raw_folder_path}'))

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

# COMMAND ----------

races_schema = StructType(fields=[
    StructField('raceId', IntegerType(), False),
    StructField('year', IntegerType(), False),
    StructField('round', IntegerType(), False),
    StructField('circuitId', IntegerType(), False),
    StructField('name', StringType(), False),
    StructField('date', StringType(), False),
    StructField('time', StringType(), False),
    StructField('url', StringType(), False),
])

# COMMAND ----------

races_df = spark.read.schema(races_schema) \
    .option('header', True) \
    .csv(f'{raw_folder_path}/{v_file_date}/races.csv')

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ##### Step 2: Rename columns
# MAGIC * Create mapping for each column

# COMMAND ----------

original_column_names = races_df.columns
new_column_names = ['race_id', 'race_year', 'round', 'circuit_id', 'name', 'date', 'time', 'url']

races_renamed_df = races_df

for original_col, new_col in zip(original_column_names, new_column_names):
    races_renamed_df = races_renamed_df.withColumnRenamed(original_col, new_col)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 3: Create additional column for race time stamp and ingestion time

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, concat_ws, lit

# COMMAND ----------

races_new_columns_df = add_ingestion_date(races_renamed_df.withColumn('race_timestamp', to_timestamp(concat_ws(' ', races_renamed_df['date'], races_renamed_df['time']), 'yyyy-MM-dd HH:mm:ss'))) \
    .withColumn('data_source', lit(v_data_source)) \
    .withColumn('file_date', lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 4: Select/drop the columns required

# COMMAND ----------

races_final_df = races_new_columns_df.drop('date', 'time', 'url')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 5: Write file into storage as a parquet file

# COMMAND ----------

# paritionBy will help to create folders for that column
races_final_df.write.mode('overwrite').partitionBy('race_year').format('parquet').saveAsTable('f1_processed.races')

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------


