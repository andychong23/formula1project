# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #### In this section, we will be recreating the table required for the assignment
# MAGIC
# MAGIC The table we require is shown below:
# MAGIC <table>
# MAGIC   <tr>
# MAGIC     <th> Driver </th>
# MAGIC     <th> Number </th>
# MAGIC     <th> Team </th>
# MAGIC     <th> Grid </th>
# MAGIC     <th> Pits </th>
# MAGIC     <th> Fastest Lap </th>
# MAGIC     <th> Race time </th>
# MAGIC     <th> Points </th>
# MAGIC   </tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text('p_file_date', '')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 1: Get all the tables required first

# COMMAND ----------

drivers_df = spark.read.parquet(f'{processed_folder_path}/drivers') \
    .withColumnRenamed('driver_id', 'drivers_driver_id') \
    .withColumnRenamed('number', 'drivers_number') \
    .withColumnRenamed('name', 'drivers_name') \
    .withColumnRenamed('driver_ref', 'drivers_driver_ref') \
    .withColumnRenamed('nationality', 'drivers_nationality') \
    .drop('ingestion_date', 'data_source')

# COMMAND ----------

races_df = spark.read.parquet(f'{processed_folder_path}/races') \
    .withColumnRenamed('race_id', 'races_race_id') \
    .withColumnRenamed('name', 'races_name') \
    .withColumnRenamed('round', 'races_round') \
    .withColumnRenamed('circuit_id', 'races_circuit_id') \
    .withColumnRenamed('race_timestamp', 'races_race_timestamp') \
    .withColumnRenamed('race_year', 'races_race_year') \
    .drop('data_source', 'ingestion_date')

# COMMAND ----------

constructors_df = spark.read.parquet(f'{processed_folder_path}/constructors') \
    .withColumnRenamed('name', 'constructors_name') \
    .withColumnRenamed('constructor_id', 'constructors_constructor_id') \
    .withColumnRenamed('constructor_ref', 'constructors_constructor_ref') \
    .withColumnRenamed('nationality', 'constructors_nationality') \
    .drop('ingestion_date', 'data_source')

# COMMAND ----------

results_df = spark.read.parquet(f'{processed_folder_path}/results').filter(f'file_date = """{v_file_date}"""')

for col_name in results_df.columns:
    if col_name != 'data_source' and col_name != 'ingestion_date':
        results_df = results_df.withColumnRenamed(col_name, f'results_{col_name}')
    else:
        results_df = results_df.drop(col_name)

# COMMAND ----------

circuits_df = spark.read.parquet(f'{processed_folder_path}/circuits')

for col_name in circuits_df.columns:
    if col_name != 'data_source' and col_name != 'ingestion_date':
        circuits_df = circuits_df.withColumnRenamed(col_name, f'circuits_{col_name}')
    else:
        circuits_df = circuits_df.drop(col_name)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 2: Join all the relevant tables together

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

results_races_df = results_df.join(races_df, results_df['results_race_id'] == races_df['races_race_id'], 'inner')

# COMMAND ----------

results_races_drivers_df = results_races_df.join(drivers_df, results_races_df['results_driver_id'] == drivers_df['drivers_driver_id'], 'inner')

# COMMAND ----------

results_races_drivers_constructors_df = results_races_drivers_df.join(constructors_df, results_races_drivers_df['results_constructor_id'] == constructors_df['constructors_constructor_id'], 'inner')

# COMMAND ----------

joined_df = results_races_drivers_constructors_df.join(circuits_df, results_races_drivers_constructors_df['races_circuit_id'] == circuits_df['circuits_circuit_id'], 'inner')

# COMMAND ----------

columns_to_keep = [
    'races_race_year',
    'races_name',
    'races_race_timestamp',
    'circuits_location',
    'drivers_name',
    'drivers_number',
    'drivers_nationality',
    'constructors_name',
    'results_grid',
    'results_fastest_lap',
    'results_time',
    'results_points',
    'results_position',
    'races_race_id',
    'results_file_date'
]

# COMMAND ----------

final_df = add_ingestion_date(joined_df.select(columns_to_keep)).withColumnRenamed('ingestion_date', 'created_date')

target_names = ['race_year', 'race_name', 'race_date', 'circuit_location', 'driver_name', 'driver_number', 'driver_nationality', 'team', 'grid', 'fastest_lap', 'race_time', 'points', 'position', 'race_id', 'file_date', 'created_date']

for initial_name, target_name in zip(columns_to_keep, target_names):
    final_df = final_df.withColumnRenamed(initial_name, target_name)

# COMMAND ----------

display(final_df.filter('race_year == 2020 and race_name == "Abu Dhabi Grand Prix"').orderBy(final_df['points'].desc()))

# COMMAND ----------

# Because the path was not stated here during this write phase, this is considered as a managed table
# Although it is a managed table, the data written is not stored on databricks but on our ABFS storage instead
# Because we have indicated when creating the database that whatever data that is saved as a table in f1_presentation, will be saved into the ABFS storage location specified
# The folder name will have the same name as the table_name

# final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_presentation.race_results')

second_incremental_data_loading(final_df, 'f1_presentation.race_results', 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct(file_date) from f1_presentation.race_results

# COMMAND ----------


