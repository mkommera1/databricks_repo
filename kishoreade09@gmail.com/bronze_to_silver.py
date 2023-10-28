# Databricks notebook source
def run_append():
    run_detail = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
      
    pid_get = get_pid()
    audit_result['process_id'] = pid_get

    audit_result['process_name'] = 'autoloader_transform_bronze_to_silver'
    audit_result['process_type'] = 'DataBricks'
    audit_result['layer'] = 'Silver'
    audit_result['table_name'] = tgt_table_name
    audit_result['process_date'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d")
    audit_result['start_time'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d %H:%M:%S")

    logger.info("Autoloader stream started for {tgt_table_name} load".format(tgt_table_name=tgt_table_name))
    schema = StructType.fromJson(json.loads(src_schema))

    logger.info("Checking Source path {src_folder_path}".format(src_folder_path=src_folder_path))
    try:
        dbutils.fs.ls(src_folder_path)
        logger.info("Reading files")
        src_df = (spark.readStream.format("cloudFiles")
           .option("cloudFiles.format", "parquet")
           .schema(schema)
           .load(src_folder_path)
           )
        
        logger.info("Writing files")
        streamQuery= (src_df.writeStream
                    .format("delta")
                    .outputMode("append")
                    .foreachBatch(upsertToDelta)
                    .queryName(tgt_table_name)
                    .option("checkpointLocation",checkpoint_location) #Will replace it with event grid
                    .trigger(once=True)
                    .start()
                    .awaitTermination()
                   )
        audit_result['status'] = 'success'
        audit_result['end_time'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        logger.error("Path is not correct {src_folder_path}".format(src_folder_path=src_folder_path))
        audit_result['status'] = 'failed'
        audit_result['end_time'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d %H:%M:%S")
        audit_result['ERROR_MESSAGE'] = str(e)
        logger.info("audit_result".format(audit_result=audit_result))
        audit(audit_result)
        raise
        

# COMMAND ----------

run_append()

# COMMAND ----------

from pyspark.sql.types import *
from delta.tables import DeltaTable

# COMMAND ----------

dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()

# COMMAND ----------

src_folder_path = "/mnt/data/Delta_Customers/"
schema = StructType([
    StructField("CustomerID", IntegerType(), True),
    StructField("FirstName", StringType(), True),
    StructField("LastName", StringType(), True),
    StructField("Email", StringType(), True),
    StructField("PhoneNumber", StringType(), True),
    StructField("lastmodifieddatetime", StringType(), True)

])
print(schema)



# COMMAND ----------

checkpoint_location='/mnt/data/checkpoint_customers/'

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %sql
# MAGIC describe formatted  silver.customers;

# COMMAND ----------

 #   try:
dbutils.fs.ls(src_folder_path)
#logger.info("Reading files")
src_df = (spark.readStream.format("cloudFiles")
   .option("cloudFiles.format", "csv")
   .option("header", "true")
   .schema(schema)
   .load(src_folder_path)
   )

#logger.info("Writing files")
streamQuery= (src_df.writeStream
            .format("delta")
            .outputMode("append")
            .foreachBatch(upsertToDelta)
            .queryName("silver.Customers")
            .option("checkpointLocation",checkpoint_location) #Will replace it with event grid
            .trigger(once=True)
            .start()
            .awaitTermination()
            )
 #       audit_result['status'] = 'success'
 #       audit_result['end_time'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d %H:%M:%S")
 #   except Exception as e:
 #       logger.error("Path is not correct {src_folder_path}".format(src_folder_path=src_folder_path))
 #       audit_result['status'] = 'failed'
 #       audit_result['end_time'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d %H:%M:%S")
 #       audit_result['ERROR_MESSAGE'] = str(e)
 #       logger.info("audit_result".format(audit_result=audit_result))
 #       audit(audit_result)
 #       raise

# COMMAND ----------

# MAGIC %sql
# MAGIC select *  from delta.`dbfs:/user/hive/warehouse/customers/`
# MAGIC
# MAGIC
# MAGIC --select * from silver.customers

# COMMAND ----------

df.

# COMMAND ----------


df = spark.read.format('delta').load('dbfs:/user/hive/warehouse/customers/')

# COMMAND ----------

# MAGIC %sql
# MAGIC describe formatted silver.customers 

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls 'dbfs:/user/hive/warehouse/customers/'

# COMMAND ----------

target_folder_path ='dbfs:/user/hive/warehouse/customers/'

# COMMAND ----------

dbutils.fs.ls('dbfs:/user/hive/warehouse/customers/')

# COMMAND ----------

# MAGIC %sql
# MAGIC describe formatted silver.customers;

# COMMAND ----------

# DBTITLE 1,Upsert function
def upsertToDelta(microBatchOutputDf,batchId):
    
    try:
        cnt_file_rows = microBatchOutputDf.count()
 #       logger.info("{cnt_file_rows} rows to be processed".format(cnt_file_rows=str(cnt_file_rows)))
        if cnt_file_rows == 0:
            logger.info("Process skipped as source has no data")
  #          audit_result['status'] = 'success'
   #         audit_result['end_time'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d %H:%M:%S")
            return

        #logger.info("Running batch for loading {tgt_table_name}: {batchId}".format(tgt_table_name=tgt_table_name,batchId=batchId))
      
        #logger.debug("tgt_delta_path: " + target_folder_path)
        deltaDf = DeltaTable.forPath(spark, target_folder_path)
      
      
        columns = microBatchOutputDf.schema.fieldNames()
        print("abc")
        print(columns)
        join_condition =[]
        join_keys = ["CustomerID"]
        #logger.debug("Primary keys: {join_keys}".format(join_keys=join_keys))
      
        join_condition = ["s.{key} = t.{key}".format(key=x) for x in join_keys]
      
        #logger.info("Join condition:  {join_condition}".format(join_condition= " and ".join(join_condition)))
      
        
        columns = microBatchOutputDf.schema.fieldNames()
        
        update_columns = {"t.{col}".format(col=x) : "s.{col}".format(col=x) for x in columns if x.lower() not in set(['dl_insert_pipeline_id','dl_insert_timestamp'])}
        #logger.debug("Update columns:  {update_columns}".format(update_columns=update_columns))
        print(update_columns)
        print(join_condition)
        print("abc")
      
        insert_columns = {"t.{col}".format(col=x) : "s.{col}".format(col=x) for x in columns}
        print(insert_columns)
        #logger.debug("Insert columns:  {insert_columns}".format(insert_columns=insert_columns))
      
        #logger.info("Running upsert")
  
    
    #condition= 's.CustomerId <> t.CustomerId',
        (deltaDf.alias("t")
         .merge(
           microBatchOutputDf.alias("s"),
              " and ".join(join_condition))
         .whenMatchedUpdate(condition= '1<>2', set = {'t.CustomerID': 's.CustomerID', 't.FirstName': 's.FirstName', 't.LastName': 's.LastName', 't.Email': 's.Email', 't.PhoneNumber': 's.PhoneNumber'} )
         .whenNotMatchedInsert(values = {'t.CustomerID': 's.CustomerID', 't.FirstName': 's.FirstName', 't.LastName': 's.LastName', 't.Email': 's.Email', 't.PhoneNumber': 's.PhoneNumber'})
         .execute()
        )
    except Exception as e:
        raise
    finally:
        print("abc")

# COMMAND ----------

# MAGIC %sql
# MAGIC create database silver;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC describe formatted  silver.customers

# COMMAND ----------

spark.sql(f"CREATE TABLE silver.customers USING delta LOCATION 'dbfs:/user/hive/warehouse/customers/'")

# COMMAND ----------

# MAGIC %sql
# MAGIC select *  from silver.customers

# COMMAND ----------

# MAGIC %sql
# MAGIC create database silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC serviceprinciplesecret

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.get('secretscope2','serviceprinciplesecret')

# COMMAND ----------


dbutils.secrets.list(scope='databrickssecrets',secret='clientid')

# COMMAND ----------


