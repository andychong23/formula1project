# Databricks notebook source
notebook_names = [
    '1.ingest_circuits_file',
    '2.ingest_races_file',
    '3.ingest_constructors_file',
    '4.ingest_drivers_file',
    '5.ingest_results_file',
    '6.ingest_pitstops_file',
    '7.ingest_lap_times_file',
    '8.ingest_qualifying_file'
]

# COMMAND ----------

# Hypothesis: You probably loop through the dates that you want from the full load processing
# Not sure how does the actual incremental load work though

for notebook_name in notebook_names:
    status = dbutils.notebook.run(notebook_name, 0, {'p_data_source': 'Ergast API', 'p_file_date': '2021-04-18'})
    if status != 'Success':
        dbutils.notebook.exit('ERROR')
        break

# COMMAND ----------


