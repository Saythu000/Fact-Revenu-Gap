# dimDate Processing Pipeline
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DateType

try:
    spark
except NameError:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("dimDateProcessing").getOrCreate()

# Schema for Date Dimension matching gold_dimdate.sql DDL
date_schema = StructType([
    StructField("dateKey", IntegerType(), True),
    StructField("date", StringType(), True),
    StructField("shortDateName", StringType(), True),
    StructField("longDateName", StringType(), True),
    StructField("yearNumber", IntegerType(), True),
    StructField("yearName", StringType(), True),
    StructField("quarterKey", IntegerType(), True),
    StructField("quarterNumber", IntegerType(), True),
    StructField("quarterName", StringType(), True),
    StructField("quarterOfYearNumber", IntegerType(), True),
    StructField("quarterOfYearName", StringType(), True),
    StructField("monthKey", IntegerType(), True),
    StructField("monthNumber", IntegerType(), True),
    StructField("monthName", StringType(), True),
    StructField("monthOfQuarterNumber", IntegerType(), True),
    StructField("monthOfQuarterName", StringType(), True),
    StructField("monthOfYearShortName", StringType(), True),
    StructField("weekKey", IntegerType(), True),
    StructField("weekNumber", IntegerType(), True),
    StructField("weekName", StringType(), True),
    StructField("dayOfWeekNumber", IntegerType(), True),
    StructField("dayOfWeekName", StringType(), True),
    StructField("dayOfYear", IntegerType(), True),
    StructField("isWorkDay", BooleanType(), True)
])

# Generate mock rows
date_rows = [
    (20260110, "2026-01-10", "01/10/2026", "January 10, 2026", 2026, "2026", 20261, 1, "Q1", 1, "Q1", 202601, 1, "January", 1, "Month 1", "Jan", 202602, 2, "Week 2", 7, "Saturday", 10, False),
    (20260115, "2026-01-15", "01/15/2026", "January 15, 2026", 2026, "2026", 20261, 1, "Q1", 1, "Q1", 202601, 1, "January", 1, "Month 1", "Jan", 202603, 3, "Week 3", 5, "Thursday", 15, True),
    (20260120, "2026-01-20", "01/20/2026", "January 20, 2026", 2026, "2026", 20261, 1, "Q1", 1, "Q1", 202601, 1, "January", 1, "Month 1", "Jan", 202604, 4, "Week 4", 3, "Tuesday", 20, True),
    (20260125, "2026-01-25", "01/25/2026", "January 25, 2026", 2026, "2026", 20261, 1, "Q1", 1, "Q1", 202601, 1, "January", 1, "Month 1", "Jan", 202604, 4, "Week 4", 1, "Sunday", 25, False),
    (20260713, "2026-07-13", "07/13/2026", "July 13, 2026", 2026, "2026", 20263, 3, "Q3", 3, "Q3", 202607, 7, "July", 1, "Month 1", "Jul", 202629, 29, "Week 29", 2, "Monday", 194, True),
    (-99, "1900-01-01", "01/01/1900", "January 1, 1900", 1900, "1900", 19001, 1, "Q1", 1, "Q1", 190001, 1, "January", 1, "Month 1", "Jan", 190001, 1, "Week 1", 2, "Monday", 1, True)
]

df_date = spark.createDataFrame(date_rows, schema=date_schema)
from pyspark.sql.functions import col, to_date
df_date = df_date.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))

df_date.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("global_gold.dimdate")
print("Table global_gold.dimdate successfully updated.")
