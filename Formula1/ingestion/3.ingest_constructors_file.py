# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Ingest Constructors.json file

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### Step 1: Read the JSON file using the spark dataframe reader

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

constructor_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read.schema(constructor_schema) \
    .json(path=f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 2: Drop unwanted column from the dataframe

# COMMAND ----------

from pyspark.sql.functions import col, lit

# COMMAND ----------

constructor_dropped_df = constructor_df.drop('url')

# or

constructor_dropped_df = constructor_df.drop(constructor_df['url'])

# or 
constructor_dropped_df = constructor_df.drop(col('url'))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 3: Rename columns and add ingestion date

# COMMAND ----------

constructor_final_df = add_ingestion_date(constructor_dropped_df.withColumnRenamed('constructorId', 'constructor_id') \
    .withColumnRenamed('constructorRef', 'constructor_ref')) \
    .withColumn('data_source', lit(v_data_source)) \
    .withColumn('file_date', lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 4: Write to parquet file

# COMMAND ----------

constructor_final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.constructors')

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.constructors

# COMMAND ----------


