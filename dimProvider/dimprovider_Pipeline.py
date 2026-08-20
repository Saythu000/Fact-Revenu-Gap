# Databricks notebook source
# MAGIC %pip install pyx12==2.3.3 pyedi==1.1.0 --force-reinstall

# COMMAND ----------

# Programmatically restart python interpreter to load pyx12 2.3.3
dbutils.library.restartPython()

# COMMAND ----------

import pyx12
print("Active pyx12 version:", pyx12.__version__)

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# Wipe the bronze parquet folder
dbutils.fs.rm("/Volumes/provider_274/bronze/processed_parquet/provider_hierarchy", recurse=True)

# COMMAND ----------

import os
import sys
import shutil
import json
from pathlib import Path

# Force local file system synchronization
os.sync()

# Dynamic workspace root directory resolution
ROOT_DIR = Path(os.getcwd()).parent
sys.path.append(str(ROOT_DIR))

# Define widgets for dynamic ClientContainer selection
try:
    dbutils.widgets.text("ClientContainer", "provider_274", "Client Container / Catalog Name")
    client_container = dbutils.widgets.get("ClientContainer").strip()
except Exception:
    client_container = "provider_274"

# COMMAND ----------


from Shared.EDIProcessing import EDIProcessor, CSVConverter
from DimProvider.EDIProcessing.mapper import Mapper

# COMMAND ----------

def move_file(src_path: Path, target_dir: Path) -> Path:
    """Moves a file to a target directory cleanly, ensuring the directory exists."""
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / src_path.name
    
    # Overwrite if target exists to prevent pipeline blocks
    if target_path.exists():
        target_path.unlink()
        
    shutil.move(str(src_path), str(target_path))
    # Flush disk to ensure sync
    os.sync()
    return target_path

# COMMAND ----------

def process_single_file(incoming_file_path: Path, base_source_dir: Path, layout_id: str) -> tuple:
    """Parses an EDI file, extracts metadata, maps records, and generates a schema-compliant CSV."""
    active_file_path = incoming_file_path
    try:
        if not active_file_path.exists():
            raise FileNotFoundError(f"Input file missing: {active_file_path}")
        
        # Transition: pending -> inprogress
        active_file_path = move_file(active_file_path, base_source_dir / "inprogress")

        # Core Parsing & Domain Mapping
        structured_json = EDIProcessor().parse(str(active_file_path))
        
        # Metadata Extraction
        interchange = structured_json.get('interchange', {})
        client_id = interchange.get('sender_id', '02').strip()
        file_id = interchange.get('control_number', '01').strip()
        
        if not client_id:
            client_id = "02"
        if not file_id:
            file_id = "01"
            
        # Target CSV Location
        target_csv_name = f"{active_file_path.stem}.csv"
        target_csv_path = ROOT_DIR / "temp" / layout_id / target_csv_name
        target_csv_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Code Conversion Execution
        CSVConverter().converter(Mapper().map_provider(structured_json), str(target_csv_path))
            
        print(f"client id: {client_id}, file id: {file_id}, layout id: {layout_id}, csv name: {target_csv_name}")
        return client_id, file_id, layout_id, target_csv_name, active_file_path
        
    except Exception as e:
        print(f"Failed processing {active_file_path.name}: {e}")
        if active_file_path.exists():
            move_file(active_file_path, base_source_dir / "failed")
        raise

# COMMAND ----------

def build_payloads(processed_files: list) -> str:
    """Generates precise production payloads for downstream bronze ingest stages."""
    process_list = []
    
    for f in processed_files:
        specific_client_container = f"{ROOT_DIR}/temp/{f['layout_id']}"
        if f['layout_id'] == "837":
            schema_file = "provider_7.12_schema.json"
            dest_folder = "provider"
        else:
            schema_file = "provider_hierarchy_7.12_schema.json"
            dest_folder = "provider_hierarchy"
        
        process_list.append({
            "ClientID": f['client_id'],
            "FileID": f['file_id'],
            "FileName": f['csv_filename'],
            "ClientContainer": specific_client_container,
            "CurrentFolderPath": "",
            "ProcessedFolderPath": f"/Volumes/{client_container}/bronze/processed_parquet/{dest_folder}",
            "ColumnDelimiter": ",",
            "HasHeader": "true",
            "IgnoreHeader": "False",
            "FileLayoutID": f['layout_id'],
            "FileLayoutDescription": f"Standard{f['layout_id']}",
            "SchemaFileName": schema_file,
            "SchemaFilePath": f"{ROOT_DIR}/DimProvider/Bronze/Schema",
            "TextQualifier": "\""
        })
        
    return json.dumps({"FileIds": process_list})

# COMMAND ----------

def trigger_silver_notebooks(client_container_val: str):
    """Triggers Silver layer notebooks in the correct dependency order."""
    silver_notebooks_base = f"{ROOT_DIR}/DimProvider/Silver/Notebooks"
    
    try:
        # Create Provider Hierarchy table (exclusively for 274 project)
        print("\n=== Triggering ProviderHierarchy (Silver Layer) ===")
        dbutils.notebook.run(
            f"{silver_notebooks_base}/ProviderHierarchy", 
            600, 
            {"ClientContainer": client_container_val}
        )
        print("ProviderHierarchy completed successfully")

        # Create Provider Person Bridge (for 837 integration)
        print("\n=== Triggering ProviderPersonBridge (Silver Layer) ===")
        dbutils.notebook.run(
            f"{silver_notebooks_base}/ProviderPersonBridge", 
            600, 
            {"ClientContainer": client_container_val}
        )
        print("ProviderPersonBridge completed successfully")

        # Create Provider table (for 837 integration)
        print("\n=== Triggering Provider (Silver Layer) ===")
        dbutils.notebook.run(
            f"{silver_notebooks_base}/Provider", 
            600, 
            {"ClientContainer": client_container_val}
        )
        print("Provider completed successfully")
        
    except Exception as e:
        print(f"Silver layer processing failed: {e}")
        raise


# COMMAND ----------

def trigger_gold_notebooks(client_container_val: str):
    """Triggers Gold layer notebooks using the generic subgroup processor for dimProvider (FHIR R4 standard)."""
    gold_notebooks_base = f"{ROOT_DIR}/DimProvider/Gold/Notebooks"
    
    try:
        # Load dimProvider (final merged SCD Type 2 dimension)
        print("\n=== Triggering dimProvider (Gold Layer - FHIR R4 Standard) ===")
        dbutils.notebook.run(
            f"{gold_notebooks_base}/GenericSubGroupProcessing", 
            600, 
            {
                "ClientContainer": client_container_val,
                "SubGroupConfigPath": f"{ROOT_DIR}/dimProvider/Gold/Config/dimProvider.json"
            }
        )
        print("dimProvider Gold load completed successfully")
        
    except Exception as e:
        print(f"Gold layer processing failed: {e}")
        raise

# COMMAND ----------

def main():
    # Detect pending EDI files
    base_274_dir = ROOT_DIR / "source/274"
    pending_274_dir = base_274_dir / "pending"
    
    base_837_dir = ROOT_DIR / "source/837"
    pending_837_dir = base_837_dir / "pending"
    
    processed_files = []
    
    # Check for 274 (Provider Hierarchy) pending files
    if pending_274_dir.exists():
        incoming_274 = [f for f in pending_274_dir.iterdir() if f.is_file() and not f.name.startswith('.')]
        for file_path in incoming_274:
            try:
                c_id, f_id, l_id, csv_filename, active_path = process_single_file(file_path, base_274_dir, "274")
                processed_files.append({
                    'client_id': c_id, 
                    'file_id': f_id, 
                    'layout_id': l_id,
                    'csv_filename': csv_filename,
                    'active_file_path': active_path
                })
            except Exception as e:
                print(f"Skipping 274 file {file_path.name}: {e}")

    # Check for 837 (Provider Specialty/Claims) pending files
    if pending_837_dir.exists():
        incoming_837 = [f for f in pending_837_dir.iterdir() if f.is_file() and not f.name.startswith('.')]
        for file_path in incoming_837:
            try:
                c_id, f_id, l_id, csv_filename, active_path = process_single_file(file_path, base_837_dir, "837")
                processed_files.append({
                    'client_id': c_id, 
                    'file_id': f_id, 
                    'layout_id': l_id,
                    'csv_filename': csv_filename,
                    'active_file_path': active_path
                })
            except Exception as e:
                print(f"Skipping 837 file {file_path.name}: {e}")

    if not processed_files:
        print("No pending EDI files found to process. Proceeding directly to Silver and Gold layers.")
        # Trigger Silver and Gold layers using existing Bronze data
        try:
            trigger_silver_notebooks(client_container)
            trigger_gold_notebooks(client_container)
            print("\nPipeline execution sequence completed successfully (Silver → Gold).")
        except Exception as e:
            print(f"Pipeline execution failed: {e}")
            raise
        return

    # If there are new CSVs, trigger Bronze Ingestion
    process_payload = build_payloads(processed_files)
    notebook_base = f"{ROOT_DIR}/Shared/Notebooks"
    
    try:
        print("\n=== Triggering FilesToProcess (Bronze Layer) ===")
        res_str = dbutils.notebook.run(f"{notebook_base}/FilesToProcess", 600, {"ProcessedJSON": process_payload})
        print(f"FilesToProcess completed with response: {res_str}")
        
        # Validate child notebook execution status
        try:
            res_json = json.loads(res_str)
            for run_res in res_json:
                if run_res.get("Status") != "SUCCESS":
                    raise Exception(f"Bronze ingestion failed for file {run_res.get('FileName')}: {run_res.get('ErrorMessage')}")
        except Exception as e:
            if "Bronze ingestion failed" in str(e):
                raise
            print(f"Warning: Could not parse response JSON: {e}")
        
        # Trigger Silver layer notebooks (Fixed to pass catalog)
        trigger_silver_notebooks(client_container)
        
        # Trigger Gold layer notebooks
        trigger_gold_notebooks(client_container)
        
        # Transition: inprogress -> processed ONLY after complete success
        for f in processed_files:
            if f['layout_id'] == "837":
                move_file(f['active_file_path'], base_837_dir / "processed")
            else:
                move_file(f['active_file_path'], base_274_dir / "processed")
        
        print("\nPipeline execution sequence completed successfully (Bronze → Silver → Gold).")
    except Exception as e:
        print(f"Downstream orchestration failed: {e}")
        # Transition: inprogress -> failed on downstream failure
        for f in processed_files:
            if f['active_file_path'].exists():
                if f['layout_id'] == "837":
                    move_file(f['active_file_path'], base_837_dir / "failed")
                else:
                    move_file(f['active_file_path'], base_274_dir / "failed")
        raise

if __name__ == "__main__":
    main()

# COMMAND ----------

# 1. Resolve raw files path
base_274_dir = ROOT_DIR / "source/274"
pending_274_dir = base_274_dir / "pending"

# 2. Convert raw files to CSV
if pending_274_dir.exists():
    incoming_274 = [f for f in pending_274_dir.iterdir() if f.is_file() and not f.name.startswith('.')]
    for file_path in incoming_274:
        print(f"Processing raw file: {file_path.name}")
        c_id, f_id, l_id, csv_filename, active_path = process_single_file(file_path, base_274_dir, "274")
        print(f"Successfully created CSV: {csv_filename} at {active_path}")
else:
    print(f"Pending folder not found at: {pending_274_dir}")

# COMMAND ----------

# Run this to print the exact raw content of the CSV file
csv_path = "/Workspace/Users/saythu000@gmail.com/provider_274_template/temp/274/provider_hierarchy_nonsolo.csv"
with open(csv_path, "r", encoding="utf-8") as f:
    print(f.read())

# COMMAND ----------

import json
from pathlib import Path
from Shared.EDIProcessing import EDIProcessor

# 1. Search for the raw file in all folders
project_root = Path("/Workspace/Users/saythu000@gmail.com/provider_274_template")
raw_file = None
for folder in ["pending", "inprogress", "processed", "failed"]:
    search_path = project_root / f"source/274/{folder}/provider_hierarchy_nonsolo.txt"
    if search_path.exists():
        raw_file = search_path
        print(f"Found raw file in: {folder}")
        break

if raw_file:
    # 2. Parse and print the generated JSON
    structured_json = EDIProcessor().parse(str(raw_file))
    print("\n--- Parsed JSON Structure ---")
    print(json.dumps(structured_json, indent=2))
else:
    print("Could not find the raw file in any of the source folders!")

# COMMAND ----------

import pyx12
import pyedi

print("Active pyx12 version:", pyx12.__version__ if hasattr(pyx12, "__version__") else "unknown")
print("Active pyx12 file location:", pyx12.__file__)
print("Active pyedi version:", pyedi.__version__ if hasattr(pyedi, "__version__") else "unknown")

# COMMAND ----------

catalog = "provider_274"

# ── Step 1: Add columns (use try/except since IF NOT EXISTS is not supported) ──
try:
    spark.sql(f"ALTER TABLE `{catalog}`.silver.ref_provider_affiliation ADD COLUMN Tier3IDType STRING")
    print("✅ Tier3IDType column added")
except Exception as e:
    if "already exists" in str(e).lower():
        print("ℹ️ Tier3IDType column already exists — skipping")
    else:
        raise e

try:
    spark.sql(f"ALTER TABLE `{catalog}`.silver.ref_provider_affiliation ADD COLUMN Tier3Address2 STRING")
    print("✅ Tier3Address2 column added")
except Exception as e:
    if "already exists" in str(e).lower():
        print("ℹ️ Tier3Address2 column already exists — skipping")
    else:
        raise e

# ── Step 2: Set the values ──────────────────────────────────────────────────
spark.sql(f"""
    UPDATE `{catalog}`.silver.ref_provider_affiliation
    SET 
        Tier3IDType   = 'XX',
        Tier3Address2 = 'STE 100'
    WHERE Tier2ID = '1992837465'
""")
print("✅ Values updated successfully")

# ── Step 3: Verify ───────────────────────────────────────────────────────────
display(spark.sql(f"SELECT * FROM `{catalog}`.silver.ref_provider_affiliation"))

# COMMAND ----------

catalog = "provider_274"

spark.sql(f"""
    UPDATE `{catalog}`.silver.ref_provider_affiliation
    SET 
        Tier3IDType   = 'XX',
        Tier3Address2 = 'STE 100'
    WHERE Tier1BillingProviderTIN = '123456789'
""")
print("✅ Values updated successfully")

# Verify
display(spark.sql(f"SELECT * FROM `{catalog}`.silver.ref_provider_affiliation"))

# COMMAND ----------

catalog = "provider_274"

# Delete ghost records where providerID is null
spark.sql(f"""
    DELETE FROM `{catalog}`.gold.dimprovider
    WHERE providerID IS NULL
""")
print("✅ Ghost records deleted")

# Verify - should now show only 3 rows
display(spark.sql(f"""
    SELECT providerKey, providerID, isCurrent, effectiveStartDate, effectiveEndDate, 
           lastName, npi, tin, providerOrgName
    FROM `{catalog}`.gold.dimprovider
    ORDER BY providerID, isCurrent DESC
"""))

# COMMAND ----------

# Find the correct catalog and table name
display(spark.sql("SHOW CATALOGS"))

# Replace 'provider_274' with whatever catalog you saw in Step 1
display(spark.sql("SHOW DATABASES IN provider_274"))

display(spark.sql("SHOW TABLES IN provider_274.gold"))

# COMMAND ----------

display(spark.sql("""
    SELECT providerKey, providerID, isCurrent, 
           effectiveStartDate, effectiveEndDate, lastName, providerOrgName
    FROM provider_274.gold.dimprovider
    ORDER BY providerID, isCurrent DESC
"""))

# COMMAND ----------

spark.sql("""
    DELETE FROM provider_274.gold.dimprovider
    WHERE providerID IS NULL
""")
print("✅ Ghost records deleted")


# COMMAND ----------

display(spark.sql("""
    SELECT providerKey, providerID, isCurrent, 
           effectiveStartDate, effectiveEndDate, lastName, npi, tin, providerOrgName
    FROM provider_274.gold.dimprovider
    ORDER BY providerID, isCurrent DESC
"""))