# ======================================================================================
# Pipeline Script: dimAlertGroup (CDI Clinical Alert Classification) Pipeline
# Standard: HL7 FHIR R4 (DetectedIssue / Observation Category Mapping)
# Target Tables: claimsprocessing.silver.silver_alertgroup
#                claimsprocessing.gold.gold_dimalertgroup
# Description: Executes 2-step Delta Lake ingestion for CDI clinical alert categories:
#              1. Raw CSV -> Silver Staging (silver_alertgroup) with hash CDC
#              2. Silver Staging -> Gold Dimension (gold_dimalertgroup) via SCD Type 1 MERGE
# ======================================================================================

import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType


def run_dimalertgroup_pipeline():
    """Executes the complete Silver and Gold dimAlertGroup transformation pipeline."""
    spark = SparkSession.builder \
        .appName("dimAlertGroup_FHIR_Pipeline") \
        .getOrCreate()

    # Step 1: Ensure Target Databases Exist (Handled gracefully for Databricks & local Spark)
    try:
        spark.sql("CREATE DATABASE IF NOT EXISTS claimsprocessing.silver")
        spark.sql("CREATE DATABASE IF NOT EXISTS claimsprocessing.gold")
    except Exception:
        spark.sql("CREATE DATABASE IF NOT EXISTS silver")
        spark.sql("CREATE DATABASE IF NOT EXISTS gold")

    # Step 2: Resolve Project Root and Configuration Paths
    current_dir = os.getcwd()
    if os.path.basename(current_dir).lower() == "dimalertgroup":
        project_root = os.path.abspath(os.path.join(current_dir, ".."))
        config_silver = os.path.join(current_dir, "Gold", "Config", "salertgroup.json")
        config_gold = os.path.join(current_dir, "Gold", "Config", "gdimalertgroup.json")
    else:
        project_root = current_dir
        config_silver = os.path.join(current_dir, "dimAlertGroup", "Gold", "Config", "salertgroup.json")
        config_gold = os.path.join(current_dir, "dimAlertGroup", "Gold", "Config", "gdimalertgroup.json")

    # Step 3: Load Raw CSV Reference Data into temp view 'AlertGroup'
    raw_csv_path = os.path.join(project_root, "source", "AlertGroup", "alert_group_reference.csv")
    print(f"[dimAlertGroup] Reading raw reference CSV from: {raw_csv_path}")

    schema = StructType([
        StructField("AlertGroupID", IntegerType(), False),
        StructField("AlertGroupCode", StringType(), False),
        StructField("AlertGroupDescription", StringType(), False),
        StructField("DisplayText", StringType(), False),
        StructField("SortOrder", IntegerType(), False),
        StructField("Active", BooleanType(), False)
    ])

    df_raw = spark.read.format("csv").option("header", "true").schema(schema).load(raw_csv_path)
    df_raw.createOrReplaceTempView("AlertGroup")
    print(f"[dimAlertGroup] Loaded {df_raw.count()} raw records into temporary view 'AlertGroup'.")

    # Step 4: Execute Silver Staging Pipeline (salertgroup.json)
    print("=== Step 1: Processing Silver Staging (claimsprocessing.silver.silver_alertgroup) ===")
    with open(config_silver, "r") as f:
        c_silver = json.load(f)["SubLayerProcessing"][0]

    df_silver_updates = spark.sql(c_silver["SQLScript"])
    df_silver_updates.createOrReplaceTempView("temp_updates")

    dest_silver = c_silver["DestinationTable"]
    merge_silver = c_silver["MergeScript"].replace("tempSQLScript", "temp_updates")

    try:
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {dest_silver} (
            alert_group_id          INT          COMMENT 'Source alert group ID',
            alert_group_code        STRING       COMMENT 'Natural alert group code',
            alert_group_description STRING       COMMENT 'Detailed category description',
            display_text            STRING       COMMENT 'User display text',
            sort_order              INT          COMMENT 'UI sort sequence',
            is_active               BOOLEAN      COMMENT 'Active status flag',
            hash_key                BIGINT       COMMENT 'CDC Hash Key'
        ) USING delta;
        """)
        spark.sql(merge_silver)
    except Exception as e:
        print(f"[dimAlertGroup] Silver execution fallback for local metastore: {e}")
        local_dest = dest_silver.replace("claimsprocessing.", "")
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {local_dest} (
            alert_group_id          INT,
            alert_group_code        STRING,
            alert_group_description STRING,
            display_text            STRING,
            sort_order              INT,
            is_active               BOOLEAN,
            hash_key                BIGINT
        ) USING delta;
        """)
        local_merge = merge_silver.replace(dest_silver, local_dest)
        spark.sql(local_merge)

    print("[dimAlertGroup] Silver Staging table MERGE complete.")

    # Step 5: Execute Gold Conformed Dimension Pipeline (gdimalertgroup.json)
    print("=== Step 2: Processing Gold Dimension (claimsprocessing.gold.gold_dimalertgroup) ===")
    with open(config_gold, "r") as f:
        c_gold = json.load(f)["SubLayerProcessing"][0]

    # Point temp view 'alertGroup' to Silver
    try:
        spark.table("claimsprocessing.silver.silver_alertgroup").createOrReplaceTempView("alertGroup")
    except Exception:
        spark.table("silver.silver_alertgroup").createOrReplaceTempView("alertGroup")

    df_gold_updates = spark.sql(c_gold["SQLScript"])
    df_gold_updates.createOrReplaceTempView("temp_updates")

    dest_gold = c_gold["DestinationTable"]
    merge_gold = c_gold["MergeScript"].replace("tempSQLScript", "temp_updates")

    try:
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {dest_gold} (
            alert_group_key         BIGINT       COMMENT 'Surrogate primary key',
            alert_group_code        STRING       COMMENT 'Natural alert group code',
            alert_group_description STRING       COMMENT 'Detailed category description',
            display_text            STRING       COMMENT 'User display text',
            sort_order              INT          COMMENT 'UI sort sequence',
            is_active               BOOLEAN      COMMENT 'Active status flag'
        ) USING delta;
        """)
        spark.sql(merge_gold)
    except Exception as e:
        print(f"[dimAlertGroup] Gold execution fallback for local metastore: {e}")
        local_dest = dest_gold.replace("claimsprocessing.", "")
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {local_dest} (
            alert_group_key         BIGINT,
            alert_group_code        STRING,
            alert_group_description STRING,
            display_text            STRING,
            sort_order              INT,
            is_active               BOOLEAN
        ) USING delta;
        """)
        local_merge = merge_gold.replace(dest_gold, local_dest)
        spark.sql(local_merge)

    print("=== [dimAlertGroup] Gold Conformed Dimension Load complete! ===")


if __name__ == "__main__":
    run_dimalertgroup_pipeline()
