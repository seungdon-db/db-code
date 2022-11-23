# Databricks notebook source
# MAGIC %md 
# MAGIC ## Additional Ingestion Formats
# MAGIC For ingestion engineers should familiarize themselves with `spark.read`. Here are a few data source formats available
# MAGIC - `spark.read.jdbc` 
# MAGIC - `spark.read.csv` 
# MAGIC - `spark.read.format("xml")` 
# MAGIC - `spark.read.format("json")` 
# MAGIC - `spark.read.format("hl7")`

# COMMAND ----------

hostname = dbutils.secrets.get( "oetrta", "mysql-hostname" )
port     = dbutils.secrets.get( "oetrta", "mysql-port"     )
database = dbutils.secrets.get( "oetrta", "mysql-database" )
username = dbutils.secrets.get( "oetrta", "mysql-username" )
password = dbutils.secrets.get( "oetrta", "mysql-password" )

# COMMAND ----------

people = spark.createDataFrame( [ ("Bilbo",     50), 
                                  ("Gandalf", 1000), 
                                  ("Thorin",   195),  
                                  ("Balin",    178), 
                                  ("Kili",      77),
                                  ("Dwalin",   169), 
                                  ("Oin",      167), 
                                  ("Gloin",    158), 
                                  ("Fili",      82), 
                                  ("Bombur",  None)
                                ], 
                                ["name", "age"] 
                              )

# COMMAND ----------

mysql_url = f"jdbc:mysql://{hostname}:{port}/{database}?user={username}&password={password}"

# COMMAND ----------

# DBTITLE 1,Write
( people.write 
    .format( "jdbc" ) 
    .option( "url"    , mysql_url ) 
    .option( "dbtable", "test" ) 
    .mode("overwrite")
    .save()
)

# COMMAND ----------

# DBTITLE 1,Read
df = ( spark.read 
       .format( "jdbc") 
       .option( "url",     mysql_url ) 
       .option( "dbtable", "test"    ) 
       .load()
     )
 
display( df )

# COMMAND ----------

# MAGIC %python 
# MAGIC # jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
# MAGIC # connectionProperties = {
# MAGIC #   "user" : user,
# MAGIC #   "password" : password,
# MAGIC #   "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
# MAGIC # }
# MAGIC 
# MAGIC 
# MAGIC # housePowerConsAggDf = spark.read.jdbc(url=jdbcUrl, table="house_power_consumption_agg", properties=connectionProperties)
# MAGIC 
# MAGIC 
# MAGIC # housePowerConsAggDf.createOrReplaceTempView("house_power_consumption_agg")
# MAGIC 
# MAGIC # select * from house_power_consumption_agg 
# MAGIC # where  date_format(window_day, 'yyyy') =  '2009'
# MAGIC # and date_format(window_day,"M") = 1 
