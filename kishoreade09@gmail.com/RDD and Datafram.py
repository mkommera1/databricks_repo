# Databricks notebook source
display(spark)
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


# COMMAND ----------

print(spark)

# COMMAND ----------

print(spark)
print(spark.sparkContext)

# COMMAND ----------

data = [1,2,3,4,5,6,7,8,9,10,11,12]
print(max(data))
display(max(data))

# COMMAND ----------

print(data)

# COMMAND ----------

rdd = spark.sparkContext.parallelize(data)

# COMMAND ----------

num_partitions = rdd.getNumPartitions()

# COMMAND ----------

print(num_partitions)

# COMMAND ----------

print(rdd)

# COMMAND ----------

num_partitions = rdd.getNumPartitions()

# COMMAND ----------

first_10_rows = rdd.take(10)

# COMMAND ----------

type(first_10_rows)

# COMMAND ----------

for row in first_10_rows:
    print(row)

# COMMAND ----------

display(rdd)

max_element = rdd.max()
display(max_element)


# COMMAND ----------

x=10
y=20
z = lambda x,y :x+y
print(z(10,20))

# COMMAND ----------

rdd2=rdd.map(lambda x:x*2)


# COMMAND ----------

out=rdd2.collect()


# COMMAND ----------

rdd = spark.sparkContext.textFile("/FileStore/tables/word_count_file_txt.txt")

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/")

# COMMAND ----------



# COMMAND ----------

filerdd =rdd.collect()
print(type(filerdd))
for r in filerdd:
    print(r)
    print(type(r))

# COMMAND ----------

rdd_words = rdd.flatMap(lambda x:x.split())

# COMMAND ----------

rdd_words_collect = rdd_words.collect()
for r in rdd_words_collect:
    print(r)

# COMMAND ----------

rdd_words_tuple = rdd_words.map(lambda x : (x,1))

# COMMAND ----------

rdd_words_tuple_collect = rdd_words_tuple.take(10)

for r in rdd_words_tuple_collect:
    print(r)


# COMMAND ----------

rdd_wordcount =rdd_words_tuple.reduceByKey(lambda x,y : x+y)
rdd_wordcount=rdd_wordcount.sortByKey(ascending=True)

# COMMAND ----------

result = rdd_wordcount.collect()

# COMMAND ----------

rdd_wordcount.saveAsTextFile("/FileStore/tables/output")



# COMMAND ----------

single_partitionRDD = rdd_wordcount.coalesce(1)

# COMMAND ----------

single_partitionRDD.saveAsTextFile("/FileStore/tables/output_singlepartition")

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/output_singlepartition")

# COMMAND ----------



# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/tables/output.txt",True)

# COMMAND ----------

single_partitionRDD.saveAsTextFile("/FileStore/tables/op.txt")

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/op.txt")

# COMMAND ----------

# MAGIC %fs head dbfs:/FileStore/tables/op.txt/part-00000

# COMMAND ----------

schema = StructType([
    StructField("name", StringType(), True),
    StructField("id", IntegerType(), True)
])

print(schema)


# COMMAND ----------

df = spark.createDataFrame(rdd_wordcount, schema=schema)

# COMMAND ----------

df.printSchema()
df.show()

# COMMAND ----------

df2 =df.select("name")
df2.show()
#df.printSchema()


# COMMAND ----------

# MAGIC %sql
# MAGIC show tables;

# COMMAND ----------

# MAGIC %sql 
# MAGIC create table abc(a int,b string);

# COMMAND ----------

# MAGIC %sql
# MAGIC describe formatted abc;

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /user/hive/warehouse

# COMMAND ----------

df = spark.read.text("/FileStore/tables/emp.txt")

# COMMAND ----------

df.show(truncate=False)

# COMMAND ----------

df_csv = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter",",").load("/FileStore/tables/emp.txt")

# COMMAND ----------

df_csv.printSchema()

# COMMAND ----------

df_csv.show()
df_csv.printSchema()

# COMMAND ----------

df.show()
