# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Ingest circuits.csv File
# MAGIC
# MAGIC Since we have already created the table from the raw data file, technically, we can carry out all these different ingestions through SQL
# MAGIC
# MAGIC However, it might be easier to do it through PySpark and then write the ingested data like how we are doing currently

# COMMAND ----------

dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

# We implement the full load processing here by indicating which folder path to go to, through the use of a widget
# this is becaues you can pass in a key-value pair when running from the main notebook and it will do the full processing instead
dbutils.widgets.text('p_file_date', '')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 1: Read the csv file using the Spark Dataframe reader

# COMMAND ----------

display(dbutils.fs.ls(f'{raw_folder_path}'))

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

# StructType can be thought of as the row and StructField can be thought of as the field/column
# If there are nested items, you will need to define StructType within StructField
circuits_schema = StructType(fields=[
    StructField('circuitId', IntegerType(), False),
    StructField('circuitRef', StringType(), False),
    StructField('name', StringType(), False),
    StructField('location', StringType(), False),
    StructField('country', StringType(), False),
    StructField('lat', DoubleType(), False),
    StructField('lng', DoubleType(), False),
    StructField('alt', IntegerType(), False),
    StructField('url', StringType(), False)
])

# COMMAND ----------

circuits_df = spark.read \
    .option('header', True) \
    .schema(circuits_schema) \
    .csv(f'{raw_folder_path}/{v_file_date}/circuits.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 2: Select only the required columns

# COMMAND ----------

# Selecting based on column names
circuits_selected_df = circuits_df.select('circuitId', 'circuitRef', 'name', 'location', 'country', 'lat', 'lng', 'alt')

# Can also select based on a list like a Pandas DataFrame if it makes it more readable

# COMMAND ----------

# Selecting by passing in the entire column using the "." attribute
circuits_selected_df = circuits_df.select(circuits_df.circuitId, circuits_df.circuitRef, circuits_df.name, circuits_df.location, circuits_df.country, circuits_df.lat, circuits_df.lng, circuits_df.alt)

# COMMAND ----------

# Selecting by passing in the entire column using keys, which will return col object similar to the run below
circuits_selected_df = circuits_df.select(circuits_df['circuitId'], circuits_df['circuitRef'], circuits_df['name'], circuits_df['location'], \
    circuits_df['country'], circuits_df['lat'], circuits_df['lng'], circuits_df['alt'])

# COMMAND ----------

from pyspark.sql.functions import col, lit

# COMMAND ----------

# Selecting based on column names
circuits_selected_df = circuits_df.select(col('circuitId'), col('circuitRef'), col('name'), col('location'), col('country'), col('lat'), col('lng'), col('alt'))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 3: Rename the columns as required

# COMMAND ----------

# Renaming through chaining of methods since it always returns a dataframe
circuits_renamed_df = circuits_selected_df.withColumnRenamed('circuitId', 'circuit_id') \
    .withColumnRenamed('circuitRef', 'circuit_ref') \
    .withColumnRenamed('lat', 'latitude') \
    .withColumnRenamed('lng', 'longitude') \
    .withColumnRenamed('alt', 'altitude') 

# COMMAND ----------

# Renaming through iteration
circuits_renamed_df = circuits_selected_df

original_column_names = circuits_selected_df.columns
new_column_names = ['circuit_id', 'circuit_ref', 'name', 'location', 'country', 'latitude', 'longitude', 'altitude']

for original_name, new_name in zip(original_column_names, new_column_names):
    circuits_renamed_df = circuits_renamed_df.withColumnRenamed(original_name, new_name)

circuits_renamed_df = circuits_renamed_df.withColumn('data_source', lit(v_data_source)) \
    .withColumn('file_date', lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 4: Add ingestion date into the dataframe

# COMMAND ----------

# You can run pandas manipulation through and add it as a new column such as this one that is commented
# test_df = circuits_renamed_df.withColumn('is_even', circuits_renamed_df['circuit_id'] % 2)
circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 5: Write data to data lake as parquet

# COMMAND ----------

# Data will be saved into the f1_processed database as the circuits table
# But the data will be stored as parquet
# Gives flexibility to the user, where you can still read via the parquet file but an analyst who knows SQL can just read via SQL
circuits_final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.circuits')

# COMMAND ----------

dbutils.notebook.exit('Success')
