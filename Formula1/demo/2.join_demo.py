# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #### Spark Join Transformation

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Inner joins
# MAGIC
# MAGIC Will only keep records that exists in both the left and right dataframe
# MAGIC If there is no match, the record will be thrown away
# MAGIC E.e
# MAGIC Dataframe A
# MAGIC <table>
# MAGIC   <tr> 
# MAGIC     <th> id </th>
# MAGIC     <th> name </th>
# MAGIC   </tr> 
# MAGIC   <tr>
# MAGIC     <td> 1 </td>
# MAGIC     <td> A </td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td> 2 </td>
# MAGIC     <td> B </td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td> 3 </td>
# MAGIC     <td> C </td>
# MAGIC   </tr>
# MAGIC </table>
# MAGIC
# MAGIC Dataframe B
# MAGIC <table>
# MAGIC   <tr> 
# MAGIC     <th> id </th>
# MAGIC     <th> occupation </th>
# MAGIC   </tr> 
# MAGIC   <tr>
# MAGIC     <td> 1 </td>
# MAGIC     <td> student </td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td> 2 </td>
# MAGIC     <td> teacher </td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td> 4 </td>
# MAGIC     <td> policeman </td>
# MAGIC   </tr>
# MAGIC </table>
# MAGIC
# MAGIC When we inner join Dataframe A and B, we get Dataframe C
# MAGIC
# MAGIC Dataframe C:
# MAGIC <table>
# MAGIC   <tr> 
# MAGIC     <th> id </th>
# MAGIC     <th> name </th>
# MAGIC     <th> occupation </th 
# MAGIC   </tr> 
# MAGIC   <tr>
# MAGIC     <td> 1 </td>
# MAGIC     <td> A </td>
# MAGIC     <td> student </td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td> 2 </td>
# MAGIC     <td> B </td>
# MAGIC     <td> teacher </td>
# MAGIC   </tr>
# MAGIC </table>

# COMMAND ----------

circuits_df = spark.read.parquet(f'{processed_folder_path}/circuits') \
    .filter('circuit_id < 70') \
    .withColumnRenamed('name', 'circuit_name')

# COMMAND ----------

races_df = spark.read.parquet(f'{processed_folder_path}/races').filter('race_year = 2019') \
    .withColumnRenamed('name', 'race_name')

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

display(races_df)

# COMMAND ----------

# If we are joining many tables and there are overlapping column names, we should rename the columns first so that we can select the correct columns in the future

race_circuits_df = circuits_df.join(races_df, circuits_df['circuit_id'] == races_df['circuit_id'], "inner") \
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Outer Joins

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###### Left Outer Joins
# MAGIC Idea: Keep everything from the left and only take entries from the right if it matches, its kinda like a double for loop <br> <br>
# MAGIC for row in left_data_frame: <br>
# MAGIC   if primary_key_left == foreign_key_right: <br> 
# MAGIC     join_rows_together

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df['circuit_id'] == races_df['circuit_id'], "left") \
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Right outer join
# MAGIC Honestly, the same idea as the left outer join, can be thought of as left outer join if you invert the dataframes

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df['circuit_id'] == races_df['circuit_id'], "right") \
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Full outer joins
# MAGIC Idea: Do a left join, do a right join, combine both outputs together and remove duplicates
# MAGIC Similar to a A Union B

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df['circuit_id'] == races_df['circuit_id'], "full") \
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Semi joins

# COMMAND ----------

# Very similar to inner joins where you only get records that satisfy the condition from both tables but resultant table only has columns from the LEFT table

race_circuits_df = circuits_df.join(races_df, circuits_df['circuit_id'] == races_df['circuit_id'], "semi") \
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###### Anti joins

# COMMAND ----------

# It gives you the complement of the semi join
race_circuits_df = circuits_df.join(races_df, circuits_df['circuit_id'] == races_df['circuit_id'], "anti")

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Cross joins
# MAGIC Idea: Will give the cartesian product, for everything on the left, join everything on the right

# COMMAND ----------

race_circuits_df = races_df.crossJoin(circuits_df)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

races_df.count() * circuits_df.count()
