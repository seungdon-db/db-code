# Databricks notebook source
# MAGIC %md
# MAGIC ## Delta 

# COMMAND ----------

# DBTITLE 1,medium to large table config
# MAGIC %sql
# MAGIC --one time
# MAGIC ALTER TABLE <table_name>
# MAGIC SET TBLPROPERTIES (
# MAGIC  delta.autoOptimize.optimizeWrite = true,
# MAGIC  delta.autoOptimize.autoCompact = true, -- ‘auto’ if > DBR 10.1
# MAGIC  delta.targetFileSize = 33554432, -- optimize your queries,joins,merges
# MAGIC  delta.tuneFileSizesForRewrites = true, -- If merge heavy table
# MAGIC  delta.dataSkippingNumIndexedCols = 5, -- optimize your writes
# MAGIC  delta.deletedFileRetentionDuration = “interval 7 days”,
# MAGIC  delta.logRetentionDuration = “interval 60 days”
# MAGIC );
# MAGIC --통계정보는 테이블의 첫 32 column 에 대햇
# MAGIC -- join key / foregin key / predicate 를 처음에 넣을것 

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.read.table("ecg_hit_delta_1").queryExecution.analyzed.stats
# MAGIC spark.sql("describe extended ecg_hit_delta_1")

# COMMAND ----------

# dataSkippingNumIndexedCols = 32

spark.sql("alter table change column col after col32")
set spark.databricks.delta.properties.defaults.dataSkippingNumIndexedCols = 3

# ANALYZE TABLE table COMPUTE STATISTICS FOR ALL COLUMNS 

# COMMAND ----------

# 통계를 모으지 않고 싶다

set spark.databricks.delta.stats.collect=false

alter table aa set tblproperties (delta.dataSkippingNumIndexedCols=0)



# COMMAND ----------

# MAGIC %sql 
# MAGIC -- log retention 연장 기본 30일 
# MAGIC 
# MAGIC ALTER TABLE table-name SET TBLPROPERTIES ('delta.logRetentionDuration'='60 days')

# COMMAND ----------

# DBTITLE 1,Optimized #1
# MAGIC %sql
# MAGIC optimize ecg_hit_delta_0 zorder by clsfd_session_id, ecg_session_id, ecg_hit_id;
# MAGIC --OPTIMIZE events WHERE date >= '2017-01-01'
# MAGIC 
# MAGIC /*
# MAGIC OPTIMIZE events
# MAGIC WHERE date >= current_timestamp() - INTERVAL 1 day
# MAGIC ZORDER BY (eventType)
# MAGIC */

# COMMAND ----------

spark.conf.get("spark.databricks.delta.optimize.maxFileSize")

# COMMAND ----------

# MAGIC %md 
# MAGIC 작은 파일을 compaction해서 큰파일로.  
# MAGIC binpacking = idempotent. 
# MAGIC 1073741824 = 1G  104857600  = 100M 

# COMMAND ----------

# DBTITLE 1, Auto Optimize
# MAGIC %sql
# MAGIC ALTER TABLE [table_name | delta.`<table-path>`] 
# MAGIC SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true,
# MAGIC                    delta.autoOptimize.autoCompact = true);
# MAGIC 
# MAGIC 
# MAGIC SET spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = true;
# MAGIC SET spark.databricks.delta.properties.defaults.autoOptimize.autoCompact = true;
# MAGIC 
# MAGIC spark.databricks.delta.optimizeWrite.enabled
# MAGIC spark.databricks.delta.autoCompact.enabled

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.conf.set("spark.sql.shuffle.partitions", 200)
# MAGIC // set spark.sql.shuffle.partitions = 2x # of cores

# COMMAND ----------

spark.conf.set("spark.sql.maxPartitionBytes", 16777216)

# COMMAND ----------

select add.stats:minValues as minValues
,add.stats:maxValues as maxValues
,add.stats:nullCount as nullCount
from json.`dbfs:/user/hive/warehouse/ecg_hit_delta_hit_cd/_delta_log/00000000000000000001.json`
where add is not null
limit 1

# COMMAND ----------

# DBTITLE 1,Bloom Filter
CREATE BLOOMFILTER INDEX ON TABLE users FOR COLUMNS(user_name OPTIONS(fpp=0.1, numItems=10000))


# COMMAND ----------

spark.conf.get("spark.sql.join.preferSortMergeJoin")

# COMMAND ----------

spark.conf.get("spark.sql.autoBroadcastJoinThreshold")

# COMMAND ----------

spark.conf.get("spark.sql.join.preferSortMergeJoin")

# COMMAND ----------

# MAGIC %md 
# MAGIC # Delta Cache

# COMMAND ----------

spark.databricks.io.cache.enabled true
spark.databricks.io.cache.maxDiskUsage "{DISK SPACE PER NODE RESERVED FOR CACHED DATA}"
spark.databricks.io.cache.maxMetaDataCache "{DISK SPACE PER NODE RESERVED FOR CACHED METADATA}"


# COMMAND ----------

# MAGIC %md 
# MAGIC # Vacuum

# COMMAND ----------

# vacuum files not required by versions more than 100 hours old
/* SQL */
VACUUM delta.`/data/events/` RETAIN 100 HOURS
/*python*/
deltaTable.vacuum(100)     
/*scala*/
deltaTable.vacuum(100) 

# COMMAND ----------

# Delta Table Size

spark.sql("describe detail delta-table-name").select("sizeInBytes").collect()




# COMMAND ----------

# Non Delta Table Size
%scala
spark.read.table("non-delta-table-name").queryExecution.analyzed.stats

# COMMAND ----------

# MAGIC %scala
# MAGIC import com.databricks.sql.transaction.tahoe._
# MAGIC import org.apache.spark.sql.functions._
# MAGIC val deltaLog = DeltaLog.forTable(spark, "dbfs:/user/hive/warehouse/delta_test_3")
# MAGIC val historyDataframe = spark.sql("describe history delta.`dbfs:/user/hive/warehouse/delta_test_3`")
# MAGIC val minVersion = historyDataframe.groupBy("userId").agg(min("version").alias("minVersion")).select("minVersion").head().getLong(0)
# MAGIC val maxVersion = historyDataframe.groupBy("userId").agg(max("version").alias("maxVersion")).select("maxVersion").head().getLong(0)
# MAGIC  
# MAGIC val sizes = minVersion.to(maxVersion).map(i => deltaLog.getSnapshotAt(i).sizeInBytes)
# MAGIC // print sizes of the delta tables at each commit version
# MAGIC print(sizes)

# COMMAND ----------

import multiprocessing as mp
cpus = mp.cpu_count()
print(cpus)

# COMMAND ----------

# MAGIC %sql
# MAGIC select input_file_name() from seungdon.students;

# COMMAND ----------


