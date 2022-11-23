# Databricks notebook source
dbutils.fs.ls("s3a://databricks-seungdon-donnie-bucket/")

# COMMAND ----------

query = (spark.readStream
        .format('cloudFiles')
        .option('cloudFiles.format','json')
        .option('inferSchema','True')
        .option('cloudFiles.schemaLocation','/tmp/autoloader/schema') 
        .option('cloudFiles.useNotifications','True')
        .load("s3a://databricks-seungdon-bkt1/loader")
    
        .writeStream.format('delta')
                .option('checkpointLocation','/tmp/autoloader/_checkpoint') 
        .trigger(processingTime='10 seconds')
         .start('/tmp/sample-table')
       
        )

# COMMAND ----------

display(query)

# COMMAND ----------

# MAGIC %fs ls /tmp/sample-table

# COMMAND ----------

query2 = (spark.readStream
        .format('cloudFiles')
        .option('cloudFiles.format','json')
        .option('inferSchema','True')
        .option('cloudFiles.schemaLocation','/tmp/autoloader/schema1') 
        .option('cloudFiles.useNotifications','True')
        .option('cloudFiles.queueUrl','https://sqs.ap-northeast-2.amazonaws.com/615835089371/databricks-auto-ingest-nexon-poc')
        .load("s3a://databricks-seungdon-donnie-bucket/")
    
        .writeStream.format('delta')
                .option('checkpointLocation','/tmp/autoloader/_checkpoint1') 
        .trigger(processingTime='10 seconds')
         .start('/tmp/sample-table')
       
        )

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from delta.`/tmp/sample-table`

# COMMAND ----------


