# Databricks notebook source
data = [1,2,3,4,5,6,7,8,9,10]
rdd = spark.parallelize(data)

# COMMAND ----------



# COMMAND ----------

for row in first_10_rows:
    print(row)
