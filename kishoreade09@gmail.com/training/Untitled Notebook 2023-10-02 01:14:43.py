# Databricks notebook source
dbutils.fs.ls("/FileStore/tables")

# COMMAND ----------

import platform
print(platform.system())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/mnt

# COMMAND ----------


