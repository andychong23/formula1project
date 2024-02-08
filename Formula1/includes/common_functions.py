# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_df):
    output_df = input_df.withColumn('ingestion_date', current_timestamp())
    return output_df

# COMMAND ----------

def table_exists(db_table_name):
    '''
    Purpose: checks if the required table exists
    Inputs: db_table_name
    Output: Boolean
    '''
    if spark._jsparkSession.catalog().tableExists(db_table_name):
        return True
    return False

# COMMAND ----------

# can consider using this to do the checks if table exists instead, since it does not require additional knowledge
# 'demo' in list(map(lambda x: x.databaseName, spark.sql('show databases').collect()))

# COMMAND ----------

def first_incremental_data_loading(input_df, db_table_name, column_partition):
    '''
    Purpose: To carry out the first method of incremental data loading
    Inputs: input_dataframe, db_table_name, column_partition
    Outputs: None
    '''
    # making the saved table consistent among the 2 methods
    output_df = reorder_df(input_df, column_partition)

    # if the table does not exists, create the table
    if not(table_exists(db_table_name)):
        # only the dataframe can invoke the writer
        output_df.write.mode('overwrite').partitionBy(column_partition).format('parquet').saveAsTable(db_table_name)
    
    # table exists
    else:
        # collect will store the column as a list, but will be stored in the ram, so dont do it with large files
        partition_list = output_df.select(column_partition).distinct().collect()
        
        for partition_id_obj in partition_list:
            # we first drop all the partitions
            # althought included, cannot use this
            spark.sql(f'ALTER TABLE {db_table_name} DROP IF EXISTS PARTITION ({column_partition} = {partition_id_obj[column_partition]})')
        
        # we append the new data to the table
        output_df.write.mode('append').partitionBy(column_partition).format('parquet').saveAsTable(db_table_name)

# COMMAND ----------

def set_dynamic_partition_overwrite():
    spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')

# COMMAND ----------

def reorder_df(input_df, column_partition):
    columns = input_df.columns
    partition_idx = columns.index(column_partition)
    columns.append(columns.pop(partition_idx))
    return input_df.select(columns)

# COMMAND ----------

def second_incremental_data_loading(input_df, db_table_name, column_partition):
    '''
    Purpose: To carry out the second method of incremental data loading
    Inputs: input_dataframe, db_table_name, column_partition
    Output: None
    '''
    # have to reorder to ensure that insert to will work
    output_df = reorder_df(input_df, column_partition)

    # if table does not exists
    if not(table_exists(db_table_name)):
        # write into a new table
        output_df.write.mode('overwrite').partitionBy(column_partition).format('parquet').saveAsTable(db_table_name)
    else:
        set_dynamic_partition_overwrite()   
        output_df.write.mode('overwrite').insertInto(db_table_name)

# COMMAND ----------


