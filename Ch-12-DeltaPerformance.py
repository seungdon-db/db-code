# Databricks notebook source
# MAGIC %md
# MAGIC # Utility function to generate data
# MAGIC * Requirement: pip install faker

# COMMAND ----------

# MAGIC %pip install faker

# COMMAND ----------

from faker import Faker
from faker.providers import BaseProvider

import random

cols = ["COMP_CODE", "GL_ACCOUNT", "FISC_YEAR", "BALANCE", "CURRENCY", "CURRENCY_ISO", "CODE", "MESSAGE"]
# Create a customer provider for generating random salaries and ages.
class CustomProvider(BaseProvider):
    def COMP_CODE(self):
        comp_code_range = range(1000, 1006)
        return random.choice(comp_code_range)
    
    def GL_ACCOUNT(self):
        gl_account_range = range(1000, 9999)
        return '000000' + str(random.choice(gl_account_range))
      
    def FISC_YEAR(self):
        year_range = range(2010, 2019)
        return random.choice(year_range)

    def BALANCE(self):
        balance_range = range(-10, 1000000)
        return random.choice(balance_range)

faker = Faker()
faker.add_provider(CustomProvider)


# Generate data for 4 columns: name, age, job and salary.
def gen_data_v1(num: int) -> list:
  return [(faker.COMP_CODE(), faker.GL_ACCOUNT(), int(faker.FISC_YEAR()), float(faker.BALANCE()), 'USD', '$', 'FN028', '..notes..') for _ in range(num)]

# COMMAND ----------

# MAGIC %md
# MAGIC # Clean prior run data files

# COMMAND ----------

dbutils.fs.rm('/tmp/ch-12/', True)

# Drop & recreate database
spark.sql("DROP DATABASE IF EXISTS ch_12 CASCADE")
spark.sql("CREATE DATABASE ch_12 ")
spark.sql("USE ch_12")

# Configure Path
DELTALAKE_PATH = "/tmp/ch-12/data"

# Remove table if it exists
dbutils.fs.rm(DELTALAKE_PATH, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a Delta table

# COMMAND ----------

df_0 = spark.createDataFrame(gen_data_v1(1000)).toDF(*cols)
df_0.write.format('delta').partitionBy('FISC_YEAR').save(DELTALAKE_PATH)

s_sql = "CREATE TABLE IF NOT EXISTS perf_test USING delta LOCATION '" + DELTALAKE_PATH+ "'"
spark.sql(s_sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Simulate new data coming in

# COMMAND ----------

for i in range(5):
  df = spark.createDataFrame(gen_data_v1(1000)).toDF(*cols)
  df.write.format('delta').mode('append').save(DELTALAKE_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC # Tune Table Properties & spark settings

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimize Writes
# MAGIC * combines multiple small files to reduce the number of disk I/O operations

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE perf_test SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');

# COMMAND ----------

# MAGIC %md
# MAGIC ## Randomize File Prefixes to avoid hotspots
# MAGIC   * ALTER TABLE <delta_table> SET TBLPROPERTIES 'delta.randomizeFilePrefixes' = 'true'

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE perf_test SET TBLPROPERTIES ('delta.randomizeFilePrefixes' = 'true');

# COMMAND ----------

# MAGIC %md
# MAGIC ## Other specialized settings (specific to Databricks Runtime)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dynamic File Pruning
# MAGIC SET spark.databricks.optimizer.dynamicFilePruning = true;
# MAGIC * Useful for non-partitioned tables, or for joins on non-partitioned columns. <br>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Max file size on disk
# MAGIC SET spark.databricks.delta.optimize.maxFileSize = 1610612736;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join
# MAGIC SET spark.databricks.optimizer.dynamicFilePruning = true;
# MAGIC * Number of files of the Delta table on the probe side of the join required to trigger dynamic file pruning
# MAGIC * minimum table size on the probe side of the join required to trigger (DFP) 
# MAGIC * default is 10G <br>

# COMMAND ----------

# MAGIC %md
# MAGIC ### IO Caching (Delta Cache)
# MAGIC SET spark.databricks.io.cache.enabled = true; <br>
# MAGIC SET spark.databricks.io.cache.maxDiskUsage = <> ; <br>
# MAGIC SET spark.databricks.io.cache.maxMetaDataCache = <> ; <br>
# MAGIC SET spark.databricks.io.cache.compression.enabled = true; <br>
# MAGIC CACHE SELECT * FROM perf_test;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Optimize Join performance (Range & Skew Joins using hints)
# MAGIC SET spark.databricks.optimizer.rangeJoin.binSize=5;<br>
# MAGIC  
# MAGIC SELECT /*+ RANGE_JOIN(points, 10) */ * <br>
# MAGIC FROM points JOIN ranges ON points.p >= ranges.start AND points.p < ranges.end;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enable Low Shuffle Merge
# MAGIC SET spark.databricks.delta.merge.enableLowShuffle = true;

# COMMAND ----------

# MAGIC %md
# MAGIC # Optimize (file management)
# MAGIC * OPTIMIZE <delta_table> [WHERE <partition_filter>] ZORDER BY (<column>[, …]) 
# MAGIC * Combine small filess into larger one on disk
# MAGIC * Aids file skipping
# MAGIC * Bin packing is idempotent meaning 2nd run without any new data does not have any impact on data layout

# COMMAND ----------

# MAGIC %sql
# MAGIC -- optimize entire table
# MAGIC OPTIMIZE perf_test;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- optimize only subset of table ex. recent data
# MAGIC OPTIMIZE perf_test WHERE FISC_YEAR >= 2015;

# COMMAND ----------

# MAGIC %md
# MAGIC # ZORDER
# MAGIC * Best applied on increment data 
# MAGIC * it is not idempotent

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE perf_test
# MAGIC WHERE FISC_YEAR >= 2015 
# MAGIC ZORDER BY (Comp_Code);

# COMMAND ----------

# MAGIC %md
# MAGIC # Bloom Filter
# MAGIC * Databricks specific feature

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Enable the Bloom filter index capability 
# MAGIC SET spark.databricks.io.skipping.bloomFilter.enabled = true; 
# MAGIC 
# MAGIC CREATE BLOOMFILTER INDEX 
# MAGIC ON TABLE perf_test
# MAGIC FOR COLUMNS(balance OPTIONS (fpp=0.1, numItems=50000000));

# COMMAND ----------

# MAGIC %md
# MAGIC # Use Delta APIs

# COMMAND ----------

from delta.tables import * 

deltaTable = DeltaTable.forPath(spark, DELTALAKE_PATH) 

# COMMAND ----------

deltaTable.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimize

# COMMAND ----------

deltaTable.optimize()

# COMMAND ----------

deltaTable.optimize().where("FISC_YEAR='2011'").executeCompaction()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vacuum

# COMMAND ----------

# versions older than the default retention period
deltaTable.vacuum() 

# COMMAND ----------

# not required by versions more than 100 hours old
deltaTable.vacuum(100) 
