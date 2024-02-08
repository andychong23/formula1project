-- Databricks notebook source
USE f1_processed

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### WHERE vs HAVING clause
-- MAGIC
-- MAGIC Both WHERE and HAVING does very similar things
-- MAGIC
-- MAGIC Differences lies here: <br>
-- MAGIC The WHERE clause evaluates on the entire dataset (pre-filter) <br>
-- MAGIC The HAVING clause evaluates on the aggregated dataset (post-filter)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Scalar Functions
-- MAGIC There functions will just create a new column for the table
-- MAGIC Examples include:
-- MAGIC 1. Concat
-- MAGIC 2. Split
-- MAGIC 3. date_add
-- MAGIC 4. current_timestamp
-- MAGIC 5. date_format

-- COMMAND ----------

-- Scalar Function such as concatentation
SELECT * , CONCAT(driver_ref, '-', code) new_driver_ref
FROM drivers

-- COMMAND ----------

SELECT * , SPLIT(name, ' ')[0] forename, split(name, ' ')[1] surname
FROM drivers

-- COMMAND ----------

SELECT * , current_timestamp()
FROM drivers

-- COMMAND ----------

SELECT *, date_format(dob, 'dd-MM-yyyy')
FROM drivers

-- COMMAND ----------

SELECT *, date_add(dob, 1)
FROM drivers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Aggregate Functions
-- MAGIC Usually these will be used with a GROUP BY clause and it will then to produce a table with less rows than the original table
-- MAGIC
-- MAGIC Examples include:
-- MAGIC 1. Count
-- MAGIC 2. Max
-- MAGIC 3. Min

-- COMMAND ----------

SELECT COUNT(*)
FROM drivers

-- COMMAND ----------

SELECT MAX(DOB) FROM drivers

-- COMMAND ----------

SELECT * FROM drivers WHERE DOB = '2000-05-11'

-- COMMAND ----------

SELECT COUNT(*)
FROM drivers
WHERE NATIONALITY = 'British'

-- COMMAND ----------

SELECT nationality, COUNT(*)
FROM drivers
GROUP BY nationality
ORDER BY nationality

-- COMMAND ----------

SELECT nationality, COUNT(*)
FROM drivers
GROUP BY nationality
-- HAVING clause lets us restricts the data based on the aggregated function
HAVING COUNT(*) > 100
ORDER BY nationality

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Window Functions
-- MAGIC
-- MAGIC Window functions allows us to partition data by certain columns and order by the resultant columns, then the window function will be applied on each partition of data
-- MAGIC Window functions are more flexible than GROUP BY aggregate functions as it allows us to apply the aggregate function on each row of the table
-- MAGIC
-- MAGIC Examples include:
-- MAGIC 1. Rank

-- COMMAND ----------

SELECT nationality, name, dob, driver_id,
-- applying the RANK function on the result of each partition
rank() OVER (PARTITION BY nationality ORDER BY dob DESC) age_rank,
-- this aggregate function will be evaluated 1 by 1 on the entire partition, so min(driver_id) will be weird since for the first row, the min is 834, to get a consistent value, we will want to order this evaluation by driver_id
-- if we order by dob, then the values are weird
min(driver_id) OVER (PARTITION BY nationality ORDER BY dob) weird_min_id,
-- if we order by driver_id then the values are correct
min(driver_id) OVER (PARTITION BY nationality ORDER BY driver_id DESC) actual_min_id,
-- snce this can be thought as a running average, thats why the average looks so weird here
avg(driver_id) OVER (PARTITION BY nationality ORDER BY dob DESC) avg_id
FROM drivers
ORDER BY nationality

-- COMMAND ----------


