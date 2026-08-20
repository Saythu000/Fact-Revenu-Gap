# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,FACT MEMBER REVENUE GAP PIPELINE ORCHESTRATOR
import os, sys, json
from pathlib import Path

# Step 1: Ensure Target Database Schemas Exist
spark.sql("CREATE DATABASE IF NOT EXISTS claimsprocessing.silver")
spark.sql("CREATE DATABASE IF NOT EXISTS claimsprocessing.gold")

# Step 2: Ensure DDL tables exist
spark.sql("""
CREATE TABLE IF NOT EXISTS claimsprocessing.gold.gold_factmemberrevenuegap (
    pecYearMonthKey             INT,
    clientKey                  INT,
    memberKey                  BIGINT,
    memberGroupKey             STRING,
    planID                     STRING,
    hccKey                     INT,
    snapshotDateKey            INT,
    planProviderKey            BIGINT,
    alertGroupKey              INT,
    isHCCClosed                STRING,
    lastDCConfirmedDateKey     INT,
    lastPCPVisitDateKey        INT,
    lastAWVDateKey             INT,
    factMemberRevenueGapHashKey STRING,
    fullRowHash                STRING,
    loadDateKey                INT
) USING delta;
""")

current_dir = Path(os.getcwd())
if current_dir.name.lower() == "factrevenugap":
    project_root = current_dir.parent
else:
    project_root = current_dir

# Dynamic Recursive Config Finder for factMemberRevenueGap.json
config_path = None
for root, dirs, files in os.walk(project_root):
    if "factMemberRevenueGap.json" in files:
        config_path = os.path.join(root, "factMemberRevenueGap.json")
        break

if not config_path:
    raise FileNotFoundError("Could not locate factMemberRevenueGap.json in workspace!")

print(f"Loading Fact Config from: {config_path}")
with open(config_path, "r") as f:
    config_data = json.load(f)["SubLayerProcessing"][0]

# Setup Temporary Views for all conformed dimension tables
spark.sql("CREATE TABLE IF NOT EXISTS claimsprocessing.silver.silver_member_revenue_gap (clientCode STRING, planMemberID STRING, subscriberID STRING, providerID STRING, hccNumber STRING, HCCVersion STRING, alertCategory STRING, planID STRING, reportMonth STRING, closureReason STRING, snapshotDate DATE, lastDCConfirmedDate DATE, lastPCPVisitDate DATE, lastAWVDate DATE) USING delta")

spark.table("claimsprocessing.silver.silver_member_revenue_gap").createOrReplaceTempView("memberRevenueGap")
spark.table("claimsprocessing.gold.gold_dimclient").createOrReplaceTempView("dimClient")
spark.table("claimsprocessing.gold.gold_dimmember").createOrReplaceTempView("dimMember")
spark.table("claimsprocessing.gold.gold_ma_membergroup").createOrReplaceTempView("dimMemberGroup")
spark.table("claimsprocessing.gold.gold_dimhcc").createOrReplaceTempView("dimHCC")
spark.table("claimsprocessing.gold.gold_dimalertgroup").createOrReplaceTempView("dimAlertGroup")
spark.table("claimsprocessing.gold.gold_dimdate").createOrReplaceTempView("dimDate")
spark.table("claimsprocessing.gold.gold_dimmonth").createOrReplaceTempView("dimMonth")

if spark.catalog.tableExists("claimsprocessing.gold.gold_dimprovider"):
    spark.table("claimsprocessing.gold.gold_dimprovider").createOrReplaceTempView("dimProvider")
else:
    spark.sql("CREATE OR REPLACE TEMP VIEW dimProvider AS SELECT -99 AS providerKey, '' AS ESAIInternalProviderID, 1 AS isCurrent")

# Load SQL Processing Script
sql_script_file = os.path.join(os.path.dirname(config_path), config_data["SQLScriptPath"])
with open(sql_script_file, "r") as f:
    sql_script = f.read()

df_fact_updates = spark.sql(sql_script)
df_fact_updates.createOrReplaceTempView("tempSQLScript")

# Load Merge Script
merge_script_file = os.path.join(os.path.dirname(config_path), config_data["MergeScriptPath"])
with open(merge_script_file, "r") as f:
    merge_script = f.read()

destination_table = config_data["DestinationTable"]
final_merge_sql = merge_script.replace("DestinationTable", destination_table)
spark.sql(final_merge_sql)

print(f"=== factMemberRevenueGap Pipeline Execution Completed Successfully into {destination_table} ===")

# Display Final Row Count
df_count = spark.sql(f"SELECT COUNT(*) AS total_fact_rows FROM {destination_table}")
display(df_count)

# COMMAND ----------

display(spark.sql("SHOW TABLES IN claimsprocessing.gold"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN claimsprocessing.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     pecYearMonthKey,
# MAGIC     clientKey,
# MAGIC     memberKey,
# MAGIC     memberGroupKey,
# MAGIC     planID,
# MAGIC     hccKey,
# MAGIC     snapshotDateKey,
# MAGIC     isHCCClosed
# MAGIC FROM claimsprocessing.gold.gold_factmemberrevenuegap;
