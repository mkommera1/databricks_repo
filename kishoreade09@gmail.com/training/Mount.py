# Databricks notebook source


# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "eb9b053d-5800-4c63-952f-350c8662283b",
"fs.azure.account.oauth2.client.secret":"Y.k8Q~bMRIkCrD5mtIv4-Kc5Nt7IQAOywtseib-i",
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/5bf83608-f9c6-4cd1-aa4e-6eab2e4f1cc3/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(source= "abfss://data@summnertraining630.dfs.core.windows.net/",
				mount_point = "/mnt/data",
				extra_configs = configs
				)

# #abfss  wasbs

# COMMAND ----------

dbutils.fs.ls("/mnt/data")

# COMMAND ----------

dbutils.fs.mkdirs("/mnt/data")

# COMMAND ----------

# Databricks notebook source
storage_account_name='sasaildeveastus2'
sas='test'

# COMMAND ----------

def mount_adls(container_name):
  
    result = dbutils.fs.mount(
          source = "wasbs://{0}@{1}.blob.core.windows.net".format(container_name,storage_account_name),
          mount_point = "/mnt/sail/{0}".format(container_name),
          extra_configs = {"fs.azure.sas.{0}.{1}.blob.core.windows.net".format(container_name,storage_account_name):sas}
                        )
    if result:
      print("!! mount point:/mnt/sail/{0} is created ".format(container_name))

# COMMAND ----------

blob_containers = ["metadata", "logs", "bronze", "silver", "gold"]
try:
  for container in blob_containers:
    mount_adls(container_name=container)
except:
  raise


# COMMAND ----------

#Creation of sample RDD

rdd = spark.spark
