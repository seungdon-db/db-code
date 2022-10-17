# Databricks notebook source
# MAGIC %fs ls /tmp/seoul-loan/landing 

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from json.`/tmp/seoul-loan/landing/accounts0.json`

# COMMAND ----------

account = spark.sql("select * from json.`/tmp/seoul-loan/landing/accounts0.json`")
#account.printSchema
account.schema

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType,LongType,BooleanType,DoubleType


schema = StructType([StructField('acc_fv_change_before_taxes', LongType(), True), StructField('accounting_treatment_id', LongType(), True), StructField('accrued_interest', LongType(), True), StructField('arrears_balance', LongType(), True), StructField('balance', LongType(), True), StructField('base_rate', StringType(), True), StructField('behavioral_curve_id', StringType(), True), StructField('cost_center_code', StringType(), True), StructField('count', LongType(), True), StructField('country_code', StringType(), True), StructField('currency_code', StringType(), True), StructField('customer_id', StringType(), True), StructField('date', StringType(), True), StructField('encumbrance_amount', LongType(), True), StructField('encumbrance_type', StringType(), True), StructField('end_date', StringType(), True), StructField('first_payment_date', StringType(), True), StructField('fvh_level', LongType(), True), StructField('guarantee_amount', LongType(), True), StructField('guarantee_scheme', StringType(), True), StructField('id', StringType(), True), StructField('impairment_amount', LongType(), True), StructField('insolvency_rank', LongType(), True), StructField('last_payment_date', StringType(), True), StructField('ledger_code', StringType(), True), StructField('limit_amount', LongType(), True), StructField('minimum_balance_eur', LongType(), True), StructField('next_payment_date', StringType(), True), StructField('next_repricing_date', StringType(), True), StructField('next_withdrawal_date', StringType(), True), StructField('on_balance_sheet', BooleanType(), True), StructField('prev_payment_date', StringType(), True), StructField('product_name', StringType(), True), StructField('purpose', StringType(), True), StructField('rate', DoubleType(), True), StructField('rate_type', StringType(), True), StructField('reporting_entity_name', StringType(), True), StructField('reporting_id', StringType(), True), StructField('risk_country_code', StringType(), True), StructField('risk_weight_std', DoubleType(), True), StructField('source', StringType(), True), StructField('start_date', StringType(), True), StructField('status', StringType(), True), StructField('trade_date', StringType(), True), StructField('type', StringType(), True), StructField('uk_funding_type', StringType(), True), StructField('version_id', StringType(), True), StructField('withdrawal_penalty', LongType(), True)])

# COMMAND ----------

cloudfile = {
  "cloudFiles.format":"json",
  #"cloudFiles.useNotifications":"true",
  #"cloudFiles.includeExistingFiles":"true", 
  #"cloudFiles.useIncrementalListing":"auto", # notification or this 
  "cloudFiles.maxBytesPerTrigger": 100000000,
  "cloudFiles.maxFilesPerTrigger":10000
}

# COMMAND ----------

df = (spark.readStream.format("cloudFiles")
      .options(**cloudfile)
      .schema(schema)
      .load("/tmp/seoul-loan/landing/*")
      .writeStream.format("delta")
      .trigger(once=True)
      .outputMode("append")
      .option("checkpointLocation","/tmp/seoul-loan/autoloader/_checkpoint")
      .start("/tmp/seoul-loan/loan-table")
     )


# COMMAND ----------

# DBTITLE 1,File Discovered by Autoloader 
# MAGIC %sql
# MAGIC SELECT * FROM cloud_files_state('/tmp/seoul-loan/autoloader/_checkpoint');

# COMMAND ----------

# MAGIC %fs ls /tmp/seoul-loan/loan-table

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from delta.`/tmp/seoul-loan/loan-table`

# COMMAND ----------

streamQuery.awaitTermination()

streamQuery.recentProgress

# COMMAND ----------


