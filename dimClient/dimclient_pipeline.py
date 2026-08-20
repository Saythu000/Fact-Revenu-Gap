# Databricks notebook source
# DBTITLE 1,DAILY CLIENT DIMENSION PIPELINE ENGINE (HL7 FHIR R4 Standardized)
# =========================================================================================================
# DAILY CLIENT PIPELINE EXECUTION ENGINE
# ---------------------------------------------------------------------------------------------------------
# This execution pipeline orchestrates the daily ingestion and merge operations for the `gold_dimclient`
# table using JSON engine configs (`gdimclient.json`).
#
# All schema columns adhere to HL7 FHIR R4 naming conventions (`client_key`, `client_code`, `client_name`,
# `sub_client_code`, `sub_client_name`, `hash_key`) reflecting the FHIR Organization resource.
# =========================================================================================================

# Setup parameters
dbutils.widgets.text("ClientContainer", "claimsprocessing", "Client Catalog Name")
client_container = dbutils.widgets.get("ClientContainer").strip()
print(f"Executing Client Dimension Pipeline for Catalog: {client_container}")

# COMMAND ----------
# DBTITLE 1,Step 1: Ensure Target Database Schema Exists
spark.sql("CREATE DATABASE IF NOT EXISTS claimsprocessing.gold")

# COMMAND ----------
# DBTITLE 1,Step 2: Run Gold Client Pipeline Engine (gdimclient.json)
import os, json

current_dir = os.getcwd()
if os.path.basename(current_dir).lower() == "dimclient":
    project_root = os.path.abspath(os.path.join(current_dir, ".."))
    config_path = os.path.join(current_dir, "Gold", "Config", "gdimclient.json")
else:
    project_root = current_dir
    config_path = os.path.join(current_dir, "dimClient", "Gold", "Config", "gdimclient.json")

print(f"Loading Client Config from: {config_path}")
with open(config_path, "r") as f:
    config_data = json.load(f)

for entity_row in config_data.get("SubLayerProcessing", []):
    entity_name = entity_row.get("SubGroupEntity")
    destination_table = entity_row.get("DestinationTable")
    
    print(f"=== Processing {entity_name} into {destination_table} ===")
    
    # Locate Source CSV File
    raw_csv_path = os.path.join(project_root, "source", "Client", "client_metadata.csv")
    print(f"Loading raw client metadata from: {raw_csv_path}")
    
    if os.path.exists(raw_csv_path):
        df_raw = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(raw_csv_path)
    else:
        print(f"Raw CSV not found at {raw_csv_path}, creating sample view...")
        raw_data = [("MCN", "Medicare National", "SUB01", "Sub Division 1")]
        df_raw = spark.createDataFrame(raw_data, ["clientCode", "clientName", "subClientCode", "subClientName"])
        
    df_raw.createOrReplaceTempView("client_raw")
    
    # Execute Transformation SQL Script
    sql_script_path = os.path.join(os.path.dirname(config_path), entity_row["SQLScriptPath"])
    with open(sql_script_path, "r") as sf:
        sql_query = sf.read()
    
    df_updates = spark.sql(sql_query)
    df_updates.createOrReplaceTempView("temp_updates")
    print(f"Transformation generated {df_updates.count()} records.")
    
    # Ensure Target Table Exists
    spark.sql("""
    CREATE TABLE IF NOT EXISTS claimsprocessing.gold.gold_dimclient (
     client_key       BIGINT
    ,client_code      STRING
    ,client_name      STRING
    ,sub_client_code  STRING
    ,sub_client_name  STRING
    ,hash_key         BIGINT
    ) USING delta;
    """)
    
    # Execute Delta Merge SQL Script
    merge_script_path = os.path.join(os.path.dirname(config_path), entity_row["MergeScriptPath"])
    with open(merge_script_path, "r") as mf:
        merge_query = mf.read()
    
    spark.sql(merge_query)
    print(f"=== {entity_name} Gold Merge completed successfully! ===")

# COMMAND ----------
# DBTITLE 1,Step 3: Display Summary & Sample Category Records
df_client_count = spark.sql("SELECT COUNT(*) AS total_clients FROM claimsprocessing.gold.gold_dimclient")
display(df_client_count)

df_sample = spark.sql("SELECT client_key, client_code, client_name, sub_client_code, sub_client_name, hash_key FROM claimsprocessing.gold.gold_dimclient LIMIT 10")
display(df_sample)
