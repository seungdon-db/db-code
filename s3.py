# Databricks notebook source
# MAGIC %md
# MAGIC https://docs.databricks.com/data/data-sources/aws/amazon-s3.html

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from csv.`s3://seungdon-test-bkt/export.csv`

# COMMAND ----------

# MAGIC %md 
# MAGIC # Secret  
# MAGIC 
# MAGIC databricks --profile WS1 secrets create-scope --scope seungdon-s3  
# MAGIC databricks --profile WS1 secrets put --scope seungdon-s3 --key aws-access-key  
# MAGIC databricks --profile WS1 secrets put --scope seungdon-s3 --key aws-secret-key  
# MAGIC databricks --profile WS1 secrets list --scope seungdon-s3

# COMMAND ----------

# DBTITLE 1,s3 mount using aws access/secret key 
access_key = dbutils.secrets.get(scope = "seungdon-s3", key = "aws-access-key")
secret_key = dbutils.secrets.get(scope = "seungdon-s3", key = "aws-secret-key")
encoded_secret_key = secret_key.replace("/", "%2F")

aws_bucket_name = "seungdon-test-bkt"
mount_name = "seungdon"


dbutils.fs.mount(f"s3a://{access_key}:{encoded_secret_key}@{aws_bucket_name}", f"/mnt/{mount_name}")
display(dbutils.fs.ls(f"/mnt/{mount_name}"))


# COMMAND ----------

# MAGIC %sql select * from csv.`/mnt/seungdon/*`

# COMMAND ----------

dbutils.fs.unmount("/mnt/seungdon") 

# COMMAND ----------

# MAGIC %fs mounts

# COMMAND ----------

# MAGIC %md
# MAGIC # Instance Profile
# MAGIC 
# MAGIC 1. S3 버킷에 대한 Instance Profile 생성 
# MAGIC 2. 해당 S3 버킷에 Bucket Policy 생성 
# MAGIC 3. Cross Account Role 의 Policy 에다가 1에서 만든 IAM role 을 추가
# MAGIC 4. 1의 instance profile을 databricks account admin에서 추가
# MAGIC 5. cluster 에 instance profile넣고 다시 시작 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from csv.`s3://seungdon-test-bkt/export.csv`

# COMMAND ----------

# MAGIC %md
# MAGIC # to dos : 다른 계정의 S3 bucket을 instance profile을 이용해서 읽어오기. 
# MAGIC https://docs.databricks.com/administration-guide/cloud-configurations/aws/assume-role.html

# COMMAND ----------


