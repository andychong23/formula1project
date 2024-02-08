# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Ingest drivers.json file

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 1: Read the json file using the spark dataframe reader API

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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

# to deal with a multi level json object, define schemas for each inner level and combine with the outer level
# the field will consists of objects and then you can use field_name.attribute_name to access the object's attribute

name_schema = StructType(fields=[
    StructField('forename', StringType(), True),
    StructField('surname', StringType(), True)
])

drivers_schema = StructType(fields=[
    StructField('driverId', IntegerType(), False),
    StructField('driverRef', StringType(), True),
    StructField('number', IntegerType(), True),
    StructField('code', StringType(), True),
    StructField('name', name_schema),
    StructField('dob', StringType(), False),
    StructField('nationality', StringType(), True),
    StructField('url', StringType(), True)
])

# COMMAND ----------

drivers_df = spark.read.schema(drivers_schema).json(f'{raw_folder_path}/{v_file_date}/drivers.json')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 2: Rename columns and add new columns
# MAGIC 1. driverId renamed to driver_id
# MAGIC 2. driverRef renamed to driver_ref
# MAGIC 3. ingestion_date added
# MAGIC 4. name added with concatenation of forename and surname

# COMMAND ----------

from pyspark.sql.functions import col, concat_ws, lit

# COMMAND ----------

# if you use the same name again, it will just rewrite the column
drivers_with_columns_df = add_ingestion_date(drivers_df) \
    .withColumnRenamed('driverId', 'driver_id') \
    .withColumnRenamed('driverRef', 'driver_ref') \
    .withColumn('name', concat_ws(' ', col('name.forename'), col('name.surname'))) \
    .withColumn('data_source', lit(v_data_source)) \
    .withColumn('file_date', lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 3: Drop the unwanted columns
# MAGIC 1. name.forename (replaced)
# MAGIC 2. name.surname (replaced)
# MAGIC 3. url

# COMMAND ----------

drivers_final_df = drivers_with_columns_df.drop(col('url'))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 4: Write output as parquet file

# COMMAND ----------

drivers_final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.drivers')

# COMMAND ----------

display(spark.read.parquet(f'{processed_folder_path}/drivers'))

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------


