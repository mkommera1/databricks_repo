# Databricks notebook source
jdbc_url = "jdbc:mysql://localhost:3306/training"
connection_properties = {
    "user": "root",
    "password": "root",
    "driver": "com.mysql.jdbc.Driver"  # MySQL JDBC driver class
}

# Define the SQL query to retrieve data
query = "SELECT * FROM emp"



# COMMAND ----------

# Read data from MySQL into a DataFrame
df = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)

# Show the DataFrame
df.show()
