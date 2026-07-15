# Databricks notebook source
# DBTITLE 1,import client parameter
dbutils.widgets.text("ClientContainer","","")
dbutils.widgets.text("SubGroupConfigPath","","")

clientContainer = dbutils.widgets.get("ClientContainer")

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from delta.tables import *

# COMMAND ----------

# DBTITLE 1,Setup Database connection
dbEnv = spark.conf.get("spark.databricks.clusterUsageTags.clusterOwnerOrgId")
envLetter = "" 
envUser="_ETLUSER_SQL"
blobKey = ""

if(dbEnv == "934226345849410"):
  envLetter = "d"
  envUser = "DEV"+envUser
  blobKey = "zbeO33jn/dsLe/dzJiWbpRhsEdS7OR4+kwi/OuEiZkq6qxNYsiHmvCQejOYYhSSwhTJAYBqVTY9Kwe0yyXRmMQ=="
elif(dbEnv == "5826678703751685"):
  envLetter = "q"
  envUser = "QA"+envUser
  blobKey = "tjmO3z7qpHlUNRnZ4cYtRTbIWlypTEX/D+6HFtLHXNs5wSDpAXHaVa4/G/8IYxaavqXw53vj3uaolw1SEYB82Q=="
elif(dbEnv == "7093677384385470"):
  envLetter = "s"
  envUser = "STG"+envUser
  blobKey = "5a3ho8IS2Xvfp458gqh42DL021Tq0WyuDy8BgLjvUiZFZWXZPBEpwudAhD0yPsocNsWAsLJv7MziyRYGYPOKPA=="
else:
  envLetter = "p"
  envUser = "PRD"+envUser
  blobKey = "wzOEDvQP/12TggUpV8diII/T1q/3mUj2l+C5E/GSD964A7k/N2TDVF6MvHCD7PpwU4FPtx5pMslYEzWJlh2Lew=="

jdbcPort = "1433"
jdbcUsername = envUser
jdbcPassword = dbutils.secrets.get(scope = "idapkeyvault", key = "ETLUSER-SQL")
jdbcHostname = "sql-c-"+envLetter+"-shrd-idap0000-01.database.windows.net"
jdbcDatabase = "Configuration_DB_" + clientContainer.upper()
jdbcUrl = "jdbc:sqlserver://" + jdbcHostname+":"+jdbcPort +";database="+jdbcDatabase  

jdbcProperties = {
              "user" : jdbcUsername,
              "password" : jdbcPassword,
              "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
            }

# COMMAND ----------

# DBTITLE 1,Read source refclient Data and Merge into dimClient
sqlQuery = """
SELECT
 ClientDescription AS clientCode
,ClientDescription AS clientName 
,cast(NULL as char) AS subClientCode 
,cast(NULL as char) AS subClientName 
FROM refClient
"""
#Putting a query in () braces and aliasing it gives all the column names for automatic type conversion
pushdown_query = "(" + sqlQuery + ") a"
try:
  df_query = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=jdbcProperties)
except Exception as e:
  print("JDBC Connection failed. Falling back to local mock client CSV.")
  df_query = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/home/mi/Desktop/claim processing/Factrevenugugap/source/Client/client_metadata.csv")

df_src = df_query.withColumn("clientKey",hash(col("clientCode"))).select(col("clientKey"),col("clientCode"),col("clientName"),col("subClientCode"),col("subClientName"))

# target dimClient

DestinationPath = '/mnt/'+ clientContainer.lower() + '/Platinum/dimClient'
destinationDF = spark.createDataFrame(spark.sparkContext.emptyRDD(), df_src.schema) #uses same schema as the source
destinationDF.write.format("delta").option("mergeSchema", "true").mode("append").save(DestinationPath)

deltaTableDimClient = DeltaTable.forPath(spark, DestinationPath)

#Merge

deltaTableDimClient.alias('client') \
  .merge(
    df_src.alias('updates'),
    'client.clientKey = updates.clientKey'
  ) \
  .whenMatchedUpdate(
    condition = "updates.clientCode <> updates.clientCode",
    set =
    {
      "clientKey": "updates.clientKey",
      "clientCode": "updates.clientCode",
      "clientName": "updates.clientName",
      "subClientCode": "updates.subClientCode",
      "subClientName": "updates.subClientName",
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      "clientKey": "updates.clientKey",
      "clientCode": "updates.clientCode",
      "clientName": "updates.clientName",
      "subClientCode": "updates.subClientCode",
      "subClientName": "updates.subClientName",
    }
  ) \
  .execute()
