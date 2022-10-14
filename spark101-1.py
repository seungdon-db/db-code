# Databricks notebook source
spark.conf.set("spark.sql.shuffle.partitions","5") # default 200

df = spark.range(10000).toDF("number")

# COMMAND ----------

display(df)

# COMMAND ----------


