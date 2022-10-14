-- Databricks notebook source
-- MAGIC %md 
-- MAGIC ## Physical Join Types
-- MAGIC - Broadcast Hash Join
-- MAGIC - Boracast Nested Loop Join
-- MAGIC - Shuffled Hash Join : 한쪽 테이블이 작고(3x 이상) 테이블 파티션이 메모리에 들어가면. 
-- MAGIC - Sort Merge Join : 동작은 함 
-- MAGIC - Cartesian : 안돼 

-- COMMAND ----------

-- Broadvasr NL/Hash Join
-- 한쪽이 작아야. no shuffle, no sort, very fast
SELECT /*+ BROADCAST(a) */ id FROM a JOIN b ON a.key = b.key

-- Shuffled Hash Join 
-- data shuffle해야하나 no sort . 큰테이블을 다룰 수 있으나 데이터가 skew되어 있으면 OOM가능성. 
-- 한쪽이 다른쪽보다 3x 이상 작고, 파티션이 메모리에 들어가면
-- spark.conf.set("spark.sql.join.preferSortMergeJoin"="true")
SELECT /*+ SHUFFLE_HASH(a, b) */ id FROM a JOIN b ON a.key = b.key

-- Sort Merge Join 
-- robust. 데이터사이즈 상관X, 
-- shuffle + sort 
-- 테이블사이즈가 작을떄 느림. 
SELECT /*+ MERGE(a, b) */ id FROM a JOIN b ON a.key = b.key


-- Shuffle Nested Loop Join (Cartesian)
-- join키가 필요없다. 
SELECT /*+ SHUFFLE_REPLICATE_NL(a, b) */ id FROM a JOIN b


-- COMMAND ----------


