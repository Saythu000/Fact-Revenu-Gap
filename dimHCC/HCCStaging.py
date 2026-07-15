# Setup local mock HCC Parquet sources
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DateType
import datetime
import os

spark = SparkSession.builder \
    .appName("LocalStagingHCC") \
    .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
    .getOrCreate()

# 1. Populate HCCEffectiveYear
effective_year_schema = StructType([
    StructField("HCCEffectiveYearID", IntegerType(), False),
    StructField("EffectiveYear", IntegerType(), False),
    StructField("EffectiveDateStart", StringType(), False),
    StructField("EffectiveDateEnd", StringType(), False)
])

effective_year_data = [
    (1, 2026, "2026-01-01", "2026-12-31"),
    (2, 2025, "2025-01-01", "2025-12-31"),
    (3, 2024, "2024-01-01", "2024-12-31")
]

df_effective_year = spark.createDataFrame(effective_year_data, schema=effective_year_schema)

# 2. Populate HCCDataset
dataset_schema = StructType([
    StructField("HCCNumber", StringType(), False),
    StructField("HCCDescription", StringType(), False),
    StructField("HCCVersion", StringType(), False),
    StructField("HCCType", StringType(), False),
    StructField("IsChronic", BooleanType(), False),
    StructField("HCCEffectiveYearID", IntegerType(), False),
    StructField("CreationDatetime", StringType(), False)
])

# Standard sample definitions for Risk Adjustment model evaluation
dataset_data = [
    # V24COMM Gaps
    ("18", "Diabetes with Chronic Complications", "V24", "COMM", True, 1, "2026-01-01"),
    ("19", "Diabetes without Complication", "V24", "COMM", True, 1, "2026-01-01"),
    # V22COMM Gaps
    ("18", "Diabetes with Chronic Complications", "V22", "COMM", True, 2, "2025-01-01"),
    ("19", "Diabetes without Complication", "V22", "COMM", True, 2, "2025-01-01"),
    # V24ESRD Gaps
    ("18", "Diabetes with Chronic Complications", "V24", "ESRD", True, 1, "2026-01-01"),
    ("19", "Diabetes without Complication", "V24", "ESRD", True, 1, "2026-01-01")
]

df_dataset = spark.createDataFrame(dataset_data, schema=dataset_schema)

# Destination operational paths (written to new/ to align with json config replacements)
dest_dataset = "/home/mi/Desktop/claim processing/Factrevenugugap/new/OperationalData/RAQ/TepReference/dbo/HCCDataset"
dest_effective_year = "/home/mi/Desktop/claim processing/Factrevenugugap/new/OperationalData/RAQ/TepReference/dbo/HCCEffectiveYear"

# Write out as Parquet formats
df_dataset.write.format("parquet").mode("overwrite").save(dest_dataset)
df_effective_year.write.format("parquet").mode("overwrite").save(dest_effective_year)

print(f"Mock HCCDataset Parquet successfully written to {dest_dataset}")
print(f"Mock HCCEffectiveYear Parquet successfully written to {dest_effective_year}")
