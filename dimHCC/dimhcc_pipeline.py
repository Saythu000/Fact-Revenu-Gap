# Databricks notebook source
# DBTITLE 1,DAILY HCC DIMENSION PIPELINE ENGINE (HL7 FHIR R4 Standardized)
# =========================================================================================================
# DAILY HCC PIPELINE EXECUTION ENGINE
# ---------------------------------------------------------------------------------------------------------
# This execution pipeline orchestrates the daily ingestion and merge operations for the `gold_dimhcc`
# and `gold_icdhccxref` tables using JSON engine configs (`gdimhcc.json`).
#
# All schema columns adhere to HL7 FHIR R4 naming conventions (`hcc_code`, `hcc_model_version`, 
# `hcc_model_type`, `hcc_description`, `effective_year`, `hcc_key`).
# =========================================================================================================

# Setup parameters
dbutils.widgets.text("ClientContainer", "claimsprocessing", "Client Catalog Name")
client_container = dbutils.widgets.get("ClientContainer").strip()
print(f"Executing HCC Dimension Pipeline for Catalog: {client_container}")

# COMMAND ----------
# DBTITLE 1,Step 1: Ensure Target Database Schemas Exist
spark.sql("CREATE DATABASE IF NOT EXISTS claimsprocessing.silver")
spark.sql("CREATE DATABASE IF NOT EXISTS claimsprocessing.gold")

# COMMAND ----------
# DBTITLE 1,Step 2: Run Gold HCC Pipeline Engine (gdimhcc.json)
import os, json

current_dir = os.getcwd()
if os.path.basename(current_dir).lower() == "dimhcc":
    project_root = os.path.abspath(os.path.join(current_dir, ".."))
else:
    project_root = current_dir

# Dynamic Recursive Config Finder for gdimhcc.json
config_gold = None
for root, dirs, files in os.walk(project_root):
    if "gdimhcc.json" in files:
        config_gold = os.path.join(root, "gdimhcc.json")
        break

if not config_gold:
    raise FileNotFoundError("Could not locate gdimhcc.json in workspace!")

print(f"Loading Gold HCC Config from: {config_gold}")
with open(config_gold, "r") as f:
    c_gold = json.load(f)["SubLayerProcessing"][0]

# Ensure temp view 'hcc' points to gold_dimhcc
spark.table("claimsprocessing.gold.gold_dimhcc").createOrReplaceTempView("hcc")

df_gold_updates = spark.sql(c_gold["SQLScript"])
df_gold_updates.createOrReplaceTempView("temp_updates")

# Execute Merge into Gold Dimension Table
merge_sql = c_gold["MergeScript"].replace("tempSQLScript", "temp_updates")
spark.sql(merge_sql)
print("=== dimHCC Gold SubGroup Engine completed successfully! ===")

# COMMAND ----------
# DBTITLE 1,Step 3: Display Summary & Sample Category Records
df_hcc_count = spark.sql("SELECT COUNT(*) AS total_hcc_categories FROM claimsprocessing.gold.gold_dimhcc")
df_xref_count = spark.sql("SELECT COUNT(*) AS total_crosswalk_mappings FROM claimsprocessing.gold.gold_icdhccxref")

display(df_hcc_count)
display(df_xref_count)

df_sample = spark.sql("SELECT HCCNumber, HCCVersion, HCCType, HCCDescription, EffectiveYear, hccKey FROM claimsprocessing.gold.gold_dimhcc LIMIT 10")
display(df_sample)


