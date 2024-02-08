# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## In this section, we will cover how to store access key/ SAS tokens in the key vault
# MAGIC There are 2 methods to actually doing this:
# MAGIC 1. You can use Azure Key Vault and connect to Databricks Secret Scope (preferred method)
# MAGIC 2. You can just directly use Databricks Secrete Scope
# MAGIC
# MAGIC ### We will be covering the steps to set up the Azure Key Vault and connecting Databricks secret scope here
# MAGIC Steps: 
# MAGIC 1. We will need to create a Key Vault Resource in Azure and deploy it with the necessary settings
# MAGIC 2. We then create Secrets under the "Secret" tab in the Key Vault Resource
# MAGIC 3. On Databricks, go to the homepage by clicking the "databricks" logo on the top left
# MAGIC 4. At the URI, include secrets/createScope to navigate to the page to create a secret scope
# MAGIC 5. The DNS name: Key Vault URI on Azure and Resouce ID: Resource ID on Azure
# MAGIC <br><b>Note: If you are lost on what kind of methods are there or what kind of utility is there, you can always invoke the help method on dbutils</b>
# MAGIC <br><b>Invoking the help function will provide the docs for the class instead </b>

# COMMAND ----------

# Getting help on the dbutils.secrets package
dbutils.secrets.help()

# COMMAND ----------

# Getting help on the dbutils package
dbutils.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope = 'formula1-scope')

# COMMAND ----------

dbutils.secrets.get(scope='formula1-scope', key='newdatabrickscoursedl-account-key')

# COMMAND ----------


