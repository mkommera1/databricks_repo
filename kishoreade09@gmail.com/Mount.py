# Databricks notebook source
from pyspark.sql.functions import input_file_name

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "eb9b053d-5800-4c63-952f-350c8662283b",
"fs.azure.account.oauth2.client.secret":dbutils.secrets.get('secretscope_keyvault','serviceprinciplesecret'),
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/5bf83608-f9c6-4cd1-aa4e-6eab2e4f1cc3/oauth2/token"}

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt

# COMMAND ----------

dbutils.fs.mount(source= "abfss://data@summnertraining630.dfs.core.windows.net/",
				mount_point = "/mnt/data",
				extra_configs = configs
				)

# COMMAND ----------



# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

df = spark.read.format("csv").load("/mnt/data/Delta_Customers/")

# COMMAND ----------

df.createOrReplaceTempView("customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customers
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select *,input_file_name() from customers

# COMMAND ----------

df = df.withColumn("file_name",input_file_name())

# COMMAND ----------

df.write.format("parquet").save("/mnt/silver/customers_parquet")

# COMMAND ----------

df = spark.read.format("parquet").load("/mnt/silver/customers_parquet")

# COMMAND ----------

df.show(10,truncate=False)

# COMMAND ----------

df.write.format("parquet").save("/mnt/silver/customers_parquet")

# COMMAND ----------

df = spark.read.format("parquet").load("/mnt/silver/customers_parquet")

# COMMAND ----------

df.write.format("delta").save("/mnt/silver/customers_delta")

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table default.customers

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/silver

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE customers USING delta LOCATION '/mnt/silver/customers_delta'
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe formatted customers

# COMMAND ----------

# MAGIC %sql
# MAGIC select *  from delta.`/mnt/silver/customers_delta`

# COMMAND ----------

# MAGIC %fs
# MAGIC mkdirs /mnt/data

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/data

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.unmount("/mnt/data")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/data

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list('secretscope_keyvault')

# COMMAND ----------

dbutils.secrets.get('secretscope_keyvault','serviceprinciplesecret')
