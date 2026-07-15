# dimMonth Processing Pipeline
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

try:
    spark
except NameError:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("dimMonthProcessing").getOrCreate()

# Schema for Month Dimension
month_schema = StructType([
    StructField("monthKey", IntegerType(), True),
    StructField("monthNumber", IntegerType(), True),
    StructField("monthName", StringType(), True),
    StructField("yearNumber", IntegerType(), True),
    StructField("yearName", StringType(), True),
    StructField("quarterNumber", IntegerType(), True),
    StructField("quarterName", StringType(), True)
])

# Generate months and standard fallback keys
month_rows = [
    (202601, 1, "January", 2026, "2026", 1, "Q1"),
    (-99, -99, "Unknown", -99, "Unknown", -99, "Unknown")
]

df_month = spark.createDataFrame(month_rows, schema=month_schema)
df_month.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("global_gold.dimmonth")
print("Table global_gold.dimmonth successfully updated.")
