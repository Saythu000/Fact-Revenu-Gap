# Databricks notebook source
# DBTITLE 1,Seed AlertGroup Parquet Reference Data
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType
import os

spark = SparkSession.builder \
    .appName("SeedAlertGroupReference") \
    .getOrCreate()

# 1. Resolve Workspace path dynamically
current_dir = os.getcwd()
if os.path.basename(current_dir).lower() in ["seeding", "dimalertgroup"]:
    project_root = os.path.abspath(os.path.join(current_dir, ".."))
    if os.path.basename(project_root).lower() == "dimalertgroup":
        project_root = os.path.abspath(os.path.join(project_root, ".."))
else:
    project_root = current_dir

raw_path = os.path.join(project_root, "source", "AlertGroup", "alert_group_reference.csv")
print(f"Project Root: {project_root}")
print(f"Reading raw alert group metadata from: {raw_path}")

schema = StructType([
    StructField("AlertGroupID", IntegerType(), False),
    StructField("AlertGroupCode", StringType(), False),
    StructField("AlertGroupDescription", StringType(), False),
    StructField("DisplayText", StringType(), False),
    StructField("SortOrder", IntegerType(), False),
    StructField("Active", BooleanType(), False)
])

df_raw = spark.read.format("csv").option("header", "true").schema(schema).load(raw_path)
print(f"Loaded {df_raw.count()} raw AlertGroup rows.")

# 2. Write to Delta Table in Silver Catalog
spark.sql("CREATE DATABASE IF NOT EXISTS claimsprocessing.silver")
df_raw.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("claimsprocessing.silver.silver_alertgroup_raw")

print("Successfully seeded AlertGroup reference data into claimsprocessing.silver.silver_alertgroup_raw!")

