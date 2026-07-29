# Databricks notebook source
# Programmatically restart python interpreter to load pyx12 2.3.3
dbutils.library.restartPython()
%pip install pyx12==4.0.0
# Databricks notebook source
# DBTITLE 1,Install Required Libraries
%pip install recordlinkage -q
dbutils.widgets.removeAll()
%pip install pyedi

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,MEMBER & MEMBERGROUP DIMENSION PIPELINE ORCHESTRATOR
import os
import sys
import shutil
from pathlib import Path
import json

dbutils.widgets.removeAll()

# Force local file system synchronization
os.sync()

# Absolute workspace configuration matching colleague working setup
ROOT_DIR = Path("/Workspace/Repos/saythu000@gmail.com/claimprocessing")
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

# COMMAND ----------

# DBTITLE 1,Import Shared EDI & Mapper Modules
from shared.EDIProcessing import EDIProcessor, CSVConverter
from DimMember.EDIProcessing.mapper import Mapper

# COMMAND ----------

# DBTITLE 1,File Management Helper Methods
def move_file(src_path: Path, target_dir: Path) -> Path:
    """Moves a file to a target directory cleanly, ensuring the directory exists."""
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / src_path.name
    shutil.move(str(src_path), str(target_path))
    return target_path

def finalize_file_tracking(file_path: Path, base_source_dir: Path, success: bool):
    """Moves the raw EDI file out of inprogress based on lifecycle completion status."""
    try:
        if success:
            print(f"--> Cycle Complete. Archiving raw file to processed: {file_path.name}")
            move_file(file_path, base_source_dir / "processed")
        else:
            print(f"--> Cycle Failed. Moving raw file to failed triage: {file_path.name}")
            move_file(file_path, base_source_dir / "failed")
    except Exception as e:
        print(f"Failed to cleanly update tracking directory state: {e}")

# COMMAND ----------

# DBTITLE 1,Core Single File EDI Processor
def process_single_file(incoming_file_path: Path, base_source_dir: Path) -> tuple:
    """Parses an EDI file, extracts metadata, maps records, and generates a targeted CSV."""
    active_file_path = incoming_file_path
    try:
        if not active_file_path.exists():
            raise FileNotFoundError(f"Input file missing: {active_file_path}")
        
        active_file_path = move_file(active_file_path, base_source_dir / "inprogress")

        # Core Parsing & Domain Mapping
        structured_json = EDIProcessor().parse(str(active_file_path))
        
        # Metadata Extraction
        interchange = structured_json.get('interchange', {})
        client_id = interchange.get('sender_id', '').strip()
        file_id = interchange.get('control_number', '').strip()
        
        st_segment = structured_json.get('heading', {}).get('transaction_set_header_loop', {}).get('transaction_set_header_ST', {})
        layout_id = st_segment.get('transaction_set_identifier_code', '834').strip()
        
        # Isolated CSV Delivery Production Rules
        target_csv_name = f"{active_file_path.stem}.csv"
        target_csv_path = ROOT_DIR / "temp" / layout_id / target_csv_name
        target_csv_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Code Conversion Execution
        CSVConverter().converter(Mapper().map_member(structured_json), str(target_csv_path))
             
        print("client id: ", client_id, " file id: ", file_id, " layout id: ", layout_id, " csv name: ", target_csv_name)
        return client_id, file_id, layout_id, target_csv_name, active_file_path
        
    except Exception as e:
        print(f"Failed processing {active_file_path.name}: {e}")
        if active_file_path.exists():
            move_file(active_file_path, base_source_dir / "failed")
        raise

# COMMAND ----------

# DBTITLE 1,Payload Builders & Orchestration Triggers
def build_payloads(processed_files: list) -> tuple:
    """Generates precise production payloads for downstream orchestration stages."""
    process_list = []
    consolidation_list = []
    
    for f in processed_files:
        specific_client_container = f"{ROOT_DIR}/temp/{f['layout_id']}"
        
        process_list.append({
            "ClientID": f['client_id'],
            "FileID": f['file_id'],
            "FileName": f['csv_filename'],
            "ClientContainer": specific_client_container,
            "CurrentFolderPath": "",
            "ProcessedFolderPath": "/Volumes/claimsprocessing/bronze/member",
            "ColumnDelimiter": ",",
            "HasHeader": "true",
            "IgnoreHeader": "False",
            "FileLayoutID": f['layout_id'],
            "FileLayoutDescription": f"Standard{f['layout_id']}",
            "SchemaFileName": "MemberSchema.json",
            "SchemaFilePath": f"{ROOT_DIR}/DimMember/Bronze/Schema",
            "TextQualifier": "\""
        })
        
        consolidation_list.append({
            "DataGroupTrackingID": f"TRACK_MEMBER_{f['layout_id']}_{f['file_id']}",
            "DataGroupMappingId": f"{f['layout_id']}MEMBER",
            "FileId": f['file_id'],
            "FileLayoutID": f['layout_id'],
            "FileLayoutDescription": f"Standard{f['layout_id']}",
            "CurrentContainer": "/Volumes/claimsprocessing/bronze/member",
            "CurrentFolderPath": "",
            "ConsolidatedMappingFilePath": f"{ROOT_DIR}/DimMember/Bronze/Schema/Consolidation",
            "ConsolidatedMappingFileName": "ConsolidationMember.json",
            "ConsolidatedLayerDataModelFilePath": f"{ROOT_DIR}/DimMember/Bronze/Schema/Consolidation/DataModels",
            "ConsolidatedLayerDataModel": "MemberDataModel.json",
            "ConsolidatedFolderPath": "/Volumes/claimsprocessing/bronze/member_consolidated"
        })
        
    return json.dumps({"FileIds": process_list}), json.dumps({"FileIds": consolidation_list})

def trigger_silver_notebooks():
    """Triggers Silver layer notebooks in dependency order."""
    silver_notebooks_base = f"{ROOT_DIR}/DimMember/Silver/Notebooks"
    
    try:
        print("\n=== Triggering MemberPersonBridge (Silver Layer) ===")
        dbutils.notebook.run(f"{silver_notebooks_base}/MemberPersonBridge", 600)
        print("MemberPersonBridge completed successfully")
        
        print("\n=== Triggering Member (Silver Layer) ===")
        dbutils.notebook.run(f"{silver_notebooks_base}/Member", 600)
        print("Member completed successfully")
        
        print("\n=== Triggering MemberGroup (Silver Layer for gold_ma_membergroup) ===")
        dbutils.notebook.run(f"{silver_notebooks_base}/MemberGroup", 600)
        print("MemberGroup completed successfully")
        
        print("\nAll Silver layer notebooks completed successfully.")
    except Exception as e:
        print(f"Silver layer processing failed: {e}")
        raise

def build_gold_payload(processed_files: list) -> str:
    """Generates Gold layer payload for GenericSubGroupProcessing."""
    if not processed_files:
        return json.dumps({"FileIds": []})
    
    first_file = processed_files[0]
    gold_entry = {
        "FileId": first_file['file_id'],
        "ClientID": first_file['client_id'],
        "SourceContainer": "claimsprocessing.silver.member",
        "FileLayoutID": first_file['layout_id'],
        "FileLayoutDescription": f"Standard{first_file['layout_id']}"
    }
    return json.dumps({"FileIds": [gold_entry]})

def trigger_gold_notebooks(gold_payload: str):
    """Triggers Gold layer notebook for dimension processing."""
    gold_notebooks_base = f"{ROOT_DIR}/DimMember/Gold/Notebooks"
    
    try:
        print("\n=== Triggering GenericSubGroupProcessing (Gold Layer) ===")
        dbutils.notebook.run(
            f"{gold_notebooks_base}/GenericSubGroupProcessing", 
            600, 
            {"GoldProcessingJSON": gold_payload}
        )
        print("GenericSubGroupProcessing completed successfully")
        print("\nAll Gold layer notebooks completed successfully.")
        
    except Exception as e:
        print(f"Gold layer processing failed: {e}")
        raise

# COMMAND ----------

# DBTITLE 1,Main Pipeline Execution Entrypoint
def main():
    base_source_dir = ROOT_DIR / "source/834"
    pending_dir = base_source_dir / "pending"
    
    if not pending_dir.exists():
        print(f"Pending directory does not exist: {pending_dir}")
        return
        
    incoming_files = [f for f in pending_dir.iterdir() if f.is_file() and not f.name.startswith('.')]
    if not incoming_files:
        print("No files found to process.")
        return

    notebook_base = f"{ROOT_DIR}/shared/Notebooks"

    for file_path in incoming_files:
        active_tracked_file = None
        try:
            c_id, f_id, l_id, csv_filename, active_tracked_file = process_single_file(file_path, base_source_dir)
            
            single_file_list = [{
                'client_id': c_id, 
                'file_id': f_id, 
                'layout_id': l_id,
                'csv_filename': csv_filename
            }]
            
            process_payload, consolidation_payload = build_payloads(single_file_list)
            gold_payload = build_gold_payload(single_file_list)
            
            print(f"\n=======================================================")
            print(f"STARTING LIFECYCLE FOR DETACHED FILE: {csv_filename}")
            print(f"=======================================================")

            print("\n=== Triggering FilesToProcess (Bronze) ===")
            dbutils.notebook.run(f"{notebook_base}/FilesToProcess", 600, {"ProcessedJSON": process_payload})
            
            print("\n=== Triggering LoopConsolidationFiles (Bronze Consolidated) ===")
            dbutils.notebook.run(f"{notebook_base}/LoopConsolidationFiles", 600, {"ConsolidationJSON": consolidation_payload})
            
            trigger_silver_notebooks()
            trigger_gold_notebooks(gold_payload)
            
            finalize_file_tracking(active_tracked_file, base_source_dir, success=True)
            print(f"\nFull end-to-end processing successful for file: {csv_filename}\n")
            
        except Exception as e:
            print(f"Pipeline crashed during track processing: {e}")
            target_to_fail = active_tracked_file if active_tracked_file else file_path
            finalize_file_tracking(target_to_fail, base_source_dir, success=False)
            continue

if __name__ == "__main__":
    main()
