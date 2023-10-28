# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE silver.Customers (
# MAGIC     CustomerID INT ,
# MAGIC     FirstName VARCHAR(50),
# MAGIC     LastName VARCHAR(50),
# MAGIC     Email VARCHAR(100),
# MAGIC     PhoneNumber VARCHAR(15),
# MAGIC     lastmodifieddatetime varchar(50)
# MAGIC );

# COMMAND ----------

# MAGIC %fs 
# MAGIC rm -r 'dbfs:/user/hive/warehouse/silver.db/customers'

# COMMAND ----------

# MAGIC %sql
# MAGIC describe formatted silver.customers;

# COMMAND ----------


