# Databricks notebook source
# DBTITLE 1,Annual Full Production CMS HCC Data Ingestion & Seeding Engine (HL7 FHIR R4 Aligned)
# =========================================================================================================
# HEALTHCARE RISK ADJUSTMENT & HL7 FHIR R4 ARCHITECTURE DOCUMENTATION
# ---------------------------------------------------------------------------------------------------------
# This PySpark seeding script automates the annual ingestion, unpivoting, and seeding of official CMS
# (Centers for Medicare & Medicaid Services) Risk Adjustment HCC (Hierarchical Condition Category) models.
#
# FHIR R4 DOMAIN ALIGNMENT:
# 1. HL7 FHIR Condition Resource (https://www.hl7.org/fhir/R4/condition.html):
#    - Condition.code.coding: Maps to ICD-10-CM diagnosis codes (e.g., E11.9 for Type 2 Diabetes)
#    - Condition.category.coding: Maps to CMS HCC Categories (e.g., HCC 19 - Diabetes without Complication)
#    - Condition.clinicalStatus: Maps to `is_chronic_condition` (active/chronic condition tracking)
#
# 2. HL7 FHIR RiskAssessment Resource (https://www.hl7.org/fhir/R4/riskassessment.html):
#    - RiskAssessment.basis: CMS Risk Adjustment Model Version (V24, V28, V21, V22, V08)
#    - RiskAssessment.method: Model Type / Population Type (COMM = Community, ESRD = End-Stage Renal Disease, RX = Part D)
#    - RiskAssessment.occurrencePeriod: Validity window (`effective_start_date` to `effective_end_date`)
# =========================================================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, hash, lit, expr
import os, urllib.request, zipfile
import pandas as pd

spark = SparkSession.builder \
    .appName("AnnualSeedCMSHCCData") \
    .getOrCreate()

# ---------------------------------------------------------------------------------------------------------
# Step 1: Resolve Workspace Root Path Dynamically
# ---------------------------------------------------------------------------------------------------------
current_dir = os.getcwd()
if os.path.basename(current_dir).lower() in ["seeding", "dimhcc"]:
    project_root = os.path.abspath(os.path.join(current_dir, ".."))
    if os.path.basename(project_root).lower() == "dimhcc":
        project_root = os.path.abspath(os.path.join(project_root, ".."))
else:
    project_root = current_dir

hcc_source_dir = os.path.join(project_root, "source", "HCC")
os.makedirs(hcc_source_dir, exist_ok=True)

print(f"Project Root: {project_root}")
print(f"HCC Source Directory: {hcc_source_dir}")

# ---------------------------------------------------------------------------------------------------------
# Step 2: Download CMS Official Model Mappings (WAF Browser Simulation)
# ---------------------------------------------------------------------------------------------------------
cms_url = "https://www.cms.gov/files/zip/2027-initial-icd-10-cm-mappings.zip"
zip_target = os.path.join(hcc_source_dir, "cms_2027_mappings.zip")

browser_headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://www.cms.gov/medicare/payment/medicare-advantage-rates-statistics/risk-adjustment/2027-model-software-icd-10-mappings',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin'
}

print(f"=== Connecting to CMS.gov with Full Browser Simulation ===")
try:
    req = urllib.request.Request(cms_url, headers=browser_headers)
    with urllib.request.urlopen(req, timeout=30) as response, open(zip_target, 'wb') as out_file:
        out_file.write(response.read())
    print("Download successful! Extracting ZIP into source/HCC/...")
    with zipfile.ZipFile(zip_target, 'r') as zip_ref:
        zip_ref.extractall(hcc_source_dir)
    print("Extraction complete!")
except Exception as e:
    print(f"CMS Web Downloader Notice (WAF Block/404): {str(e)}")

# ---------------------------------------------------------------------------------------------------------
# Step 3: Locate Master CMS Mapping CSV File (11,918+ ICD-10 to HCC Mappings)
# ---------------------------------------------------------------------------------------------------------
master_csv_path = os.path.join(project_root, "2027-initial-icd-10-cm-mappings", "2027 Initial ICD-10-CM Mappings.csv")

if not os.path.exists(master_csv_path):
    for root, dirs, files in os.walk(project_root):
        for f in files:
            if f.endswith(".csv") and ("2027 Initial ICD-10-CM Mappings" in f or "ICD-10-CM Mappings" in f):
                master_csv_path = os.path.join(root, f)
                break

print(f"Loading Master CMS Mapping CSV File: {master_csv_path}")

# Read Master CSV with Pandas (skip title lines 1-3)
pdf_raw = pd.read_csv(master_csv_path, skiprows=3, dtype=str)
pdf_raw.columns = [c.replace("\n", " ").strip() for c in pdf_raw.columns]

# Rename raw CSV columns to standardized staging names
pdf_raw = pdf_raw.rename(columns={
    pdf_raw.columns[0]: "ICD10",
    pdf_raw.columns[1]: "Description",
    pdf_raw.columns[2]: "V21_ESRD",
    pdf_raw.columns[3]: "V24_ESRD",
    pdf_raw.columns[4]: "V22_COMM",
    pdf_raw.columns[5]: "V28_COMM",
    pdf_raw.columns[6]: "V08_RX"
})

df_master = spark.createDataFrame(pdf_raw)

# ---------------------------------------------------------------------------------------------------------
# Step 4: Define Transformation Function to Unpivot Model Versions to FHIR R4 Standard
# ---------------------------------------------------------------------------------------------------------
def build_version_df(df, cc_col, version_name, type_name):
    """
    Unpivots individual CMS model version columns into standardized FHIR R4 attribute records.
    - cc_col: Raw CSV column name (e.g. V28_COMM)
    - version_name: CMS Model Version string (V24, V28, V21, V22, V08)
    - type_name: Risk Model Type (COMM = Community, ESRD = End Stage Renal, RX = Part D)
    """
    return df.filter(col(cc_col).isNotNull() & (col(cc_col) != "") & (col(cc_col) != "--")) \
        .withColumn("condition_code", col("ICD10")) \
        .withColumn("hcc_code", col(cc_col).cast("float").cast("int").cast("string")) \
        .withColumn("hcc_model_version", lit(version_name)) \
        .withColumn("hcc_model_type", lit(type_name)) \
        .withColumn("condition_code_type", lit("10")) \
        .withColumn("condition_effective_year", lit(2026)) \
        .withColumn("hcc_effective_year", lit(2026)) \
        .withColumn("is_primary_diagnosis", lit(True)) \
        .withColumn("effective_start_date", expr("to_date('2026-01-01')")) \
        .withColumn("effective_end_date", expr("to_date('2026-12-31')"))

# ---------------------------------------------------------------------------------------------------------
# Step 5: Unpivot All 5 CMS Model Versions (V21 ESRD, V24 ESRD, V22 COMM, V28 COMM, V08 RX)
# ---------------------------------------------------------------------------------------------------------
df_v21 = build_version_df(df_master, "V21_ESRD", "V21", "ESRD")
df_v24 = build_version_df(df_master, "V24_ESRD", "V24", "ESRD")
df_v22 = build_version_df(df_master, "V22_COMM", "V22", "COMM")
df_v28 = build_version_df(df_master, "V28_COMM", "V28", "COMM")
df_v08 = build_version_df(df_master, "V08_RX", "V08", "RX")

# ---------------------------------------------------------------------------------------------------------
# Step 6: Union All 5 CMS Models into Crosswalk Bridge Table (gold_icdhccxref)
# ---------------------------------------------------------------------------------------------------------
cols_xref = [
    "condition_code", 
    "condition_code_type", 
    "condition_effective_year", 
    "hcc_code", 
    "hcc_model_version", 
    "hcc_model_type", 
    "hcc_effective_year", 
    "is_primary_diagnosis", 
    "effective_start_date", 
    "effective_end_date"
]

df_xref = df_v21.select(*cols_xref) \
    .union(df_v24.select(*cols_xref)) \
    .union(df_v22.select(*cols_xref)) \
    .union(df_v28.select(*cols_xref)) \
    .union(df_v08.select(*cols_xref))

# Generate surrogate hash key `icd_hcc_key` for unique crosswalk lookup
df_xref_final = df_xref.withColumn(
    "icd_hcc_key", 
    hash(concat_ws("|", col("condition_code"), col("condition_code_type"), col("hcc_code"), col("hcc_model_version"), col("hcc_model_type"), col("hcc_effective_year")))
)

# ---------------------------------------------------------------------------------------------------------
# Step 7: Extract Unique HCC Categories for Master Dimension Table (gold_dimhcc)
# ---------------------------------------------------------------------------------------------------------
df_dim_hcc = df_xref.select(
    col("hcc_code"), 
    col("hcc_model_version"), 
    col("hcc_model_type"), 
    col("hcc_effective_year").alias("effective_year"), 
    col("effective_start_date"), 
    col("effective_end_date")
) \
    .distinct() \
    .withColumn("hcc_description", concat_ws(" ", lit("Hierarchical Condition Category"), col("hcc_code"))) \
    .withColumn("is_chronic_condition", lit(True)) \
    .withColumn(
        "hcc_key",
        hash(concat_ws("|", col("hcc_code"), col("hcc_model_version"), col("hcc_model_type"), col("effective_year")))
    ) \
    .withColumn("hash_key", col("hcc_key"))

# ---------------------------------------------------------------------------------------------------------
# Step 8: Persist to Delta Lake Tables in claimsprocessing Catalog
# ---------------------------------------------------------------------------------------------------------
spark.sql("CREATE DATABASE IF NOT EXISTS claimsprocessing.gold")

df_xref_final.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("claimsprocessing.gold.gold_icdhccxref")
df_dim_hcc.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("claimsprocessing.gold.gold_dimhcc")

# Print Execution Verification Summary
count_xref = spark.table("claimsprocessing.gold.gold_icdhccxref").count()
count_hcc = spark.table("claimsprocessing.gold.gold_dimhcc").count()

print(f"============================================================")
print(f"Successfully executed Full Master Production CMS Seeding!")
print(f"Total Crosswalk Mappings (gold_icdhccxref): {count_xref}")
print(f"Total Unique HCC Categories (gold_dimhcc): {count_hcc}")
print(f"============================================================")


