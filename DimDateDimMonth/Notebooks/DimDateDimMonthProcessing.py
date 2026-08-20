# Databricks notebook source
# DBTITLE 1,DAILY DATE & MONTH CALENDAR DIMENSIONS PIPELINE

# Define Parameters
dbutils.widgets.text("ClientContainer", "", "")
dbutils.widgets.text("SubGroupConfigPath", "", "")

client_container = dbutils.widgets.get("ClientContainer")

# COMMAND ----------
# DBTITLE 1,Declare Variables
catalog_schema = "claimsprocessing.gold"
date_table_name = f"{catalog_schema}.gold_dimdate"
month_table_name = f"{catalog_schema}.gold_dimmonth"

start_date = "1900-01-01"
end_date = "9999-12-31"

# COMMAND ----------
# DBTITLE 1,Import Libraries
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, expr, explode, sequence, date_format, 
    year, quarter, month, weekofyear, dayofweek, dayofyear, concat, datediff
)
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DateType, BooleanType
)

# COMMAND ----------
# DBTITLE 1,Method: Check Table Utility
def table_exists(table_name: str) -> bool:
    return spark.catalog.tableExists(table_name)

# COMMAND ----------
# DBTITLE 1,Define Schemas
dim_date_schema = StructType([
    StructField("dateKey", IntegerType(), False),
    StructField("date", DateType(), False),
    StructField("shortDateName", StringType(), False),
    StructField("longDateName", StringType(), False),
    StructField("yearNumber", IntegerType(), False),
    StructField("yearName", StringType(), False),
    StructField("quarterKey", IntegerType(), False),
    StructField("quarterNumber", IntegerType(), False),
    StructField("quarterName", StringType(), False),
    StructField("quarterOfYearNumber", IntegerType(), False),
    StructField("quarterOfYearName", StringType(), False),
    StructField("monthKey", IntegerType(), False),
    StructField("monthNumber", IntegerType(), False),
    StructField("monthName", StringType(), False),
    StructField("monthOfQuarterNumber", IntegerType(), False),
    StructField("monthOfQuarterName", StringType(), False),
    StructField("monthOfYearShortName", StringType(), False),
    StructField("weekKey", IntegerType(), False),
    StructField("weekNumber", IntegerType(), False),
    StructField("weekName", StringType(), False),
    StructField("dayOfWeekNumber", IntegerType(), False),
    StructField("dayOfWeekName", StringType(), False),
    StructField("dayOfYear", IntegerType(), False),
    StructField("isWorkDay", BooleanType(), False)
])

dim_mon_schema = StructType([
    StructField("monthKey", IntegerType(), False),
    StructField("monthNumber", IntegerType(), False),
    StructField("monthName", StringType(), False),
    StructField("yearNumber", IntegerType(), False),
    StructField("yearName", StringType(), False),
    StructField("quarterNumber", IntegerType(), False),
    StructField("quarterName", StringType(), False)
])

# COMMAND ----------
# DBTITLE 1,Base Range Dataframe
base_df = spark.createDataFrame([(start_date, end_date)], ["startDate", "endDate"]) \
    .select(
        col("startDate").cast("date"), 
        col("endDate").cast("date")
    )

# COMMAND ----------
# DBTITLE 1,dimDate Load
spark.sql("CREATE DATABASE IF NOT EXISTS claimsprocessing.gold")
dest_check_date = table_exists(date_table_name)

if not dest_check_date:
    destination_data_model = spark.createDataFrame([], dim_date_schema)
    dates_df = base_df.withColumn("newDate", explode(sequence(col("startDate"), col("endDate"), expr("interval 1 day"))))
    
    curr_date_df = dates_df.select(
        date_format(col("newDate"), "yyyyMMdd").cast("integer").alias("dateKey"),
        date_format(col("newDate"), "yyyy-MM-dd HH:mm:ss").cast("date").alias("date"),
        date_format(col("newDate"), "MMM d, yyyy").alias("shortDateName"),
        date_format(col("newDate"), "MMMM d, yyyy").alias("longDateName"),
        year(col("newDate")).alias("yearNumber"),
        date_format(col("newDate"), "yyyy").alias("yearName"),
        concat(year(col("newDate")), quarter(col("newDate"))).cast("integer").alias("quarterKey"),
        quarter(col("newDate")).alias("quarterNumber"),
        concat(lit("Q"), quarter(col("newDate"))).alias("quarterName"),
        quarter(col("newDate")).alias("quarterOfYearNumber"),
        concat(lit("Q"), quarter(col("newDate")), lit(", "), year(col("newDate"))).alias("quarterOfYearName"),
        date_format(col("newDate"), "yyyyMM").cast("integer").alias("monthKey"),
        month(col("newDate")).alias("monthNumber"),
        date_format(col("newDate"), "MMMM").alias("monthName"),
        expr("case when month(newDate) in (1,4,7,10) then 1 when month(newDate) in (2,5,8,11) then 2 else 3 end").alias("monthOfQuarterNumber"),
        concat(lit("Month "), expr("case when month(newDate) in (1,4,7,10) then 1 when month(newDate) in (2,5,8,11) then 2 else 3 end")).alias("monthOfQuarterName"),
        concat(date_format(col("newDate"), "MMM"), lit("-"), date_format(col("newDate"), "yyyy")).alias("monthOfYearShortName"),
        ((year(col("newDate")) * 1000) + weekofyear(col("newDate"))).alias("weekKey"),
        weekofyear(col("newDate")).alias("weekNumber"),
        concat(lit("Week "), weekofyear(col("newDate"))).alias("weekName"),
        dayofweek(col("newDate")).alias("dayOfWeekNumber"),
        date_format(col("newDate"), "EEEE").alias("dayOfWeekName"),
        dayofyear(col("newDate")).alias("dayOfYear"),
        expr("case when dayofweek(newDate) in (1,7) then false else true end").alias("isWorkDay")
    )
    
    destination_data_model = destination_data_model.union(curr_date_df)
    destination_data_model.repartition(1).write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable(date_table_name)
    print(f"Successfully generated dimDate into {date_table_name}")

# COMMAND ----------
# DBTITLE 1,dimMonth Load
dest_check_mon = table_exists(month_table_name)

if not dest_check_mon:
    destination_data_model_mon = spark.createDataFrame([], dim_mon_schema)
    months_df = base_df.withColumn("newDate", explode(sequence(col("startDate"), col("endDate"), expr("interval 1 month"))))
    
    curr_mon_df = months_df.select(
        date_format(col("newDate"), "yyyyMM").cast("integer").alias("monthKey"),
        month(col("newDate")).alias("monthNumber"),
        date_format(col("newDate"), "MMMM").alias("monthName"),
        year(col("newDate")).alias("yearNumber"),
        date_format(col("newDate"), "yyyy").alias("yearName"),
        quarter(col("newDate")).alias("quarterNumber"),
        concat(lit("Q"), quarter(col("newDate"))).alias("quarterName")
    )
    
    destination_data_model_mon = destination_data_model_mon.union(curr_mon_df)
    destination_data_model_mon.repartition(1).write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable(month_table_name)
    print(f"Successfully generated dimMonth into {month_table_name}")
