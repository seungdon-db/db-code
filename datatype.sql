-- Databricks notebook source
SELECT typeof(ARRAY(1,2,3));
SELECT CAST(ARRAY(1,2,3) AS ARRAY<TINYINT>);
SELECT a[1] FROM VALUES(ARRAY(3,4,5,6)) AS T(a);
SELECT m[2] FROM VALUES(map(1,'Hello',2,'World')) AS T(m);

-- COMMAND ----------

SELECT MAP('hohoho','hehehe','lalala','llololo');

-- COMMAND ----------


