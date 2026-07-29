# Databricks notebook source
# DBTITLE 1,SILVER MEMBER GROUP ENROLLMENT PIPELINE
import logging
from datetime import datetime
import os
from pyspark.sql.functions import col, sha2, concat_ws, current_timestamp, to_date

# Ensure target databases exist
spark.sql("CREATE DATABASE IF NOT EXISTS claimsprocessing.silver")
spark.sql("CREATE DATABASE IF NOT EXISTS claimsprocessing.gold")

sourcePath = "/Volumes/claimsprocessing/bronze/member_consolidated"
silverMemGroupTable = "claimsprocessing.silver.silver_member_group"

print(f"Source Path: {sourcePath}")
print(f"Target Silver Member Group Table: {silverMemGroupTable}")

# Ensure target silver table exists via DDL fallback
spark.sql("""
CREATE TABLE IF NOT EXISTS claimsprocessing.silver.silver_member_group (
    SubscriberID        STRING,
    BeneficiaryID       STRING,
    CMSContractNumber   STRING,
    GroupNumber         STRING,
    GroupSuffix         STRING,
    StartDate           DATE,
    EndDate             DATE,
    SourceFileID        BIGINT,
    LoadDateTime        TIMESTAMP,
    hashKey             STRING
)
""")

if spark.catalog.tableExists("claimsprocessing.silver.silver_member"):
    df_src = spark.table("claimsprocessing.silver.silver_member")
    
    df_grp = df_src.select(
        col("SubscriberID"),
        col("BeneficiaryID"),
        col("ProductID").alias("CMSContractNumber"),
        col("PlanMemberID").alias("GroupNumber"),
        col("ClientID").alias("GroupSuffix"),
        to_date(col("LoadDateTime")).alias("StartDate"),
        to_date(col("DeceasedDate")).alias("EndDate"),
        col("FileID").cast("bigint").alias("SourceFileID"),
        current_timestamp().alias("LoadDateTime")
    ).distinct()
    
    df_grp_final = df_grp.withColumn(
        "hashKey",
        sha2(concat_ws("|", col("SubscriberID"), col("BeneficiaryID"), col("CMSContractNumber"), col("GroupNumber"), col("GroupSuffix")), 256)
    )
    
    df_grp_final.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(silverMemGroupTable)
    print(f"=== Silver MemberGroup processing complete: {df_grp_final.count()} records created ===")
else:
    print("Silver member table not found, waiting for member ingestion.")

