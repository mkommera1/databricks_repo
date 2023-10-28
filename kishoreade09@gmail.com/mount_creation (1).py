# Databricks notebook source
storage_account_name='summnertraining630'
configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "eb9b053d-5800-4c63-952f-350c8662283b",
"fs.azure.account.oauth2.client.secret":"Y.k8Q~bMRIkCrD5mtIv4-Kc5Nt7IQAOywtseib-i",
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/5bf83608-f9c6-4cd1-aa4e-6eab2e4f1cc3/oauth2/token"}


# COMMAND ----------

def mount_adls(container_name):
  
    result = dbutils.fs.mount(
          source = "abfss://{0}@{1}.dfs.core.windows.net".format(container_name,storage_account_name),
          mount_point = "/mnt/{0}".format(container_name),
          extra_configs = configs
                        )
    if result:
      print("!! mount point:/mnt/{0} is created ".format(container_name))


# COMMAND ----------

blob_containers = ["data", "silver", "gold","logs"]


# COMMAND ----------

try:
  for container in blob_containers:
    mount_adls(container_name=container)
except:
  raise

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt"

# COMMAND ----------

my_secret = dbutils.secrets.get(scope="my_scope", key="my_secret_key")
print(my_secret)

# COMMAND ----------

spClientId=dbutils.secrets.get(scope=scopeName,key=f"sp-client-id")
spClientSecret=dbutils.secrets.get(scope=scopeName,key=f"sp-client-secret")
