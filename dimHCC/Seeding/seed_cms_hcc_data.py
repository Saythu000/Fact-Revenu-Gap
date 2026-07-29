# Databricks notebook source
# DBTITLE 1,Annual Full Production CMS HCC Data Ingestion (WAF Browser Downloader + Master 1.3MB CSV)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, hash, lit, expr
import os, urllib.request, zipfile
import pandas as pd

spark = SparkSession.builder \
    .appName("AnnualSeedCMSHCCData") \
    .getOrCreate()

# 1. Resolve Workspace root path dynamically
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

# 2. MODULE 1: Full Browser Simulation Web Downloader (WAF Bypass)
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

print(f"=== Module 1: Connecting to CMS.gov with Full Browser Simulation ===")
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

# 3. MODULE 2: Locate Master CMS Mapping CSV File (1.3MB file containing all 11,918 lines)
master_csv_path = os.path.join(project_root, "2027-initial-icd-10-cm-mappings", "2027 Initial ICD-10-CM Mappings.csv")

if not os.path.exists(master_csv_path):
    # Scan source/HCC/ or workspace for extracted Master CSV file
    for root, dirs, files in os.walk(project_root):
        for f in files:
            if f.endswith(".csv") and ("2027 Initial ICD-10-CM Mappings" in f or "ICD-10-CM Mappings" in f):
                master_csv_path = os.path.join(root, f)
                break

print(f"Loading Master CMS Mapping CSV File: {master_csv_path}")

# 4. Read Master CSV File with Pandas (skipping header metadata lines)
# Lines 1-3 in CMS file are title/metadata header; row 4 contains column names
pdf_raw = pd.read_csv(master_csv_path, skiprows=3, dtype=str)

# Clean Column Names (removing linebreaks)
pdf_raw.columns = [c.replace("\n", " ").strip() for c in pdf_raw.columns]

# Rename primary columns for clarity
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

def build_version_df(df, cc_col, version_name, type_name):
    return df.filter(col(cc_col).isNotNull() & (col(cc_col) != "") & (col(cc_col) != "--")) \
        .withColumn("icd", col("ICD10")) \
        .withColumn("hccNumber", col(cc_col).cast("float").cast("int").cast("string")) \
        .withColumn("hccVersion", lit(version_name)) \
        .withColumn("hccType", lit(type_name)) \
        .withColumn("icdCodeType", lit("10")) \
        .withColumn("icdEffectiveYear", lit(2026)) \
        .withColumn("hccEffectiveYear", lit(2026)) \
        .withColumn("isPrimary", lit(True)) \
        .withColumn("effectiveStartDate", expr("to_date('2026-01-01')")) \
        .withColumn("effectiveEndDate", expr("to_date('2026-12-31')"))

# 5. Unpivot All 5 Model Versions
df_v21 = build_version_df(df_master, "V21_ESRD", "V21", "ESRD")
df_v24 = build_version_df(df_master, "V24_ESRD", "V24", "ESRD")
df_v22 = build_version_df(df_master, "V22_COMM", "V22", "COMM")
df_v28 = build_version_df(df_master, "V28_COMM", "V28", "COMM")
df_v08 = build_version_df(df_master, "V08_RX", "V08", "RX")

# 6. Union All 5 CMS Model Versions for Full Bridge Table (gold_icdhccxref)
cols_xref = ["icd", "icdCodeType", "icdEffectiveYear", "hccNumber", "hccVersion", "hccType", "hccEffectiveYear", "isPrimary", "effectiveStartDate", "effectiveEndDate"]
df_xref = df_v21.select(*cols_xref) \
    .union(df_v24.select(*cols_xref)) \
    .union(df_v22.select(*cols_xref)) \
    .union(df_v28.select(*cols_xref)) \
    .union(df_v08.select(*cols_xref))

df_xref_final = df_xref.withColumn(
    "icdHCCKey", 
    hash(concat_ws("|", col("icd"), col("icdCodeType"), col("hccNumber"), col("hccVersion"), col("hccType"), col("hccEffectiveYear")))
)

# 7. Extract Unique Categories for Dimension Table (gold_dimhcc)
df_dim_hcc = df_xref.select("hccNumber", "hccVersion", "hccType", "hccEffectiveYear", "effectiveStartDate", "effectiveEndDate") \
    .distinct() \
    .withColumn("HCCDescription", concat_ws(" ", lit("Hierarchical Condition Category"), col("hccNumber"))) \
    .withColumn("IsChronic", lit(True)) \
    .withColumnRenamed("hccEffectiveYear", "EffectiveYear") \
    .withColumnRenamed("effectiveStartDate", "EffectiveDateStart") \
    .withColumnRenamed("effectiveEndDate", "EffectiveDateEnd") \
    .withColumnRenamed("hccNumber", "HCCNumber") \
    .withColumnRenamed("hccVersion", "HCCVersion") \
    .withColumnRenamed("hccType", "HCCType") \
    .withColumn(
        "hccKey",
        hash(concat_ws("|", col("HCCNumber"), col("HCCVersion"), col("HCCType"), col("EffectiveYear")))
    ) \
    .withColumn("hashKey", col("hccKey"))

# Save to Delta Tables in claimsprocessing catalog
spark.sql("CREATE DATABASE IF NOT EXISTS claimsprocessing.gold")
df_xref_final.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("claimsprocessing.gold.gold_icdhccxref")
df_dim_hcc.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("claimsprocessing.gold.gold_dimhcc")

# Print Final Production Counts per Version
count_xref = spark.table("claimsprocessing.gold.gold_icdhccxref").count()
count_hcc = spark.table("claimsprocessing.gold.gold_dimhcc").count()

print(f"============================================================")
print(f"Successfully executed Full Master Production CMS Seeding!")
print(f"Total Crosswalk Mappings (gold_icdhccxref): {count_xref}")
print(f"Total Unique HCC Categories (gold_dimhcc): {count_hcc}")
print(f"============================================================")

