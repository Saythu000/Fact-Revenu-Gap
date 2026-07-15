# Setup local mock AlertGroup Parquet source
from pyspark.sql import SparkSession
import os

spark = SparkSession.builder \
    .appName("LocalStagingAlertGroup") \
    .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
    .getOrCreate()

# Read the local CSV
csv_path = "/home/mi/Desktop/claim processing/Factrevenugugap/source/AlertGroup/alert_group_reference.csv"
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(csv_path)

# Destination operational path (written to new/ to align with json config replacements)
dest_path = "/home/mi/Desktop/claim processing/Factrevenugugap/new/OperationalData/RAQ/AlertReference/dbo/AlertGroup"

# Write as parquet
df.write.format("parquet").mode("overwrite").save(dest_path)
print(f"Mock AlertGroup Parquet successfully written to {dest_path}")
