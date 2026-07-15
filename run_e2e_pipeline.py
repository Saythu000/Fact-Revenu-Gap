# run_e2e_pipeline.py
import os
import glob
import json
import shutil
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import col, lit, hash as spark_hash
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

# Clean up local warehouse folder to avoid conflicts
shutil.rmtree("/tmp/spark-warehouse", ignore_errors=True)

print("Initializing Spark Session with Delta support...")
builder = SparkSession.builder \
    .appName("E2EPipelineTest") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Mock dbutils widgets
class MockDbutils:
    class Widgets:
        def text(self, name, default_value, label=""):
            pass
        def get(self, name):
            if name == "ClientContainer":
                return "new"
            if name == "ProgramYear":
                return "2026"
            if name == "HCCVersionRAPS":
                return "V24"
            if name == "HCCVersionEDPS":
                return "V24"
            if name == "SubGroupConfigPath":
                return ""
            return ""
        def removeAll(self):
            pass
    widgets = Widgets()
dbutils = MockDbutils()

# Create target databases in Spark locally as 2-part namespaces
spark.sql("CREATE DATABASE IF NOT EXISTS new_silver")
spark.sql("CREATE DATABASE IF NOT EXISTS new_gold")
spark.sql("CREATE DATABASE IF NOT EXISTS global_gold")

# Helper function to conform UC 3-part names in SQL queries to local 2-part database names
def clean_sql(sql_str):
    # Strip backticks to simplify string matching
    cleaned = sql_str.replace("`", "")
    return cleaned \
        .replace("new.silver.", "new_silver.") \
        .replace("new.gold.", "new_gold.") \
        .replace("global.gold.", "global_gold.") \
        .replace("new.gold", "new_gold") \
        .replace("new.silver", "new_silver") \
        .replace("global.gold", "global_gold")

# Monkey-patch spark.sql to clean all dynamic queries automatically
original_sql = spark.sql
def patched_sql(sqlQuery, *args, **kwargs):
    return original_sql(clean_sql(sqlQuery), *args, **kwargs)
spark.sql = patched_sql

# Helper function to resolve JSON path templates to local filesystem warehouse paths
def resolve_local_path(path_str):
    return path_str \
        .replace("#clientCode/Gold/alertGroup", "/tmp/spark-warehouse/new_gold/alertgroup") \
        .replace("#clientCode/Platinum/dimAlertGroup", "/tmp/spark-warehouse/new_gold/dimalertgroup") \
        .replace("#clientCode/Gold/hcc", "/tmp/spark-warehouse/new_gold/hcc") \
        .replace("#clientCode/Platinum/dimHCC", "/tmp/spark-warehouse/new_gold/dimhcc") \
        .replace("#clientCode/Gold/memberRevenueGap", "/tmp/spark-warehouse/new_gold/memberrevenuegap") \
        .replace("#clientCode/Platinum/factMemberRevenueGap", "/tmp/spark-warehouse/new_gold/factmemberrevenuegap") \
        .replace("#clientCode/OperationalData/RAQ/AlertReference/dbo/AlertGroup", "/home/mi/Desktop/claim processing/Factrevenugugap/new/OperationalData/RAQ/AlertReference/dbo/AlertGroup") \
        .replace("#clientCode/OperationalData/RAQ/TepReference/dbo/HCCDataset", "/home/mi/Desktop/claim processing/Factrevenugugap/new/OperationalData/RAQ/TepReference/dbo/HCCDataset") \
        .replace("#clientCode/OperationalData/RAQ/TepReference/dbo/HCCEffectiveYear", "/home/mi/Desktop/claim processing/Factrevenugugap/new/OperationalData/RAQ/TepReference/dbo/HCCEffectiveYear")

# 1. Run DDL scripts
print("Running DDL scripts to create schemas and tables...")
ddl_base_dir = "/home/mi/Desktop/claim processing/Factrevenugugap/DDL"
subdirs = sorted([
    os.path.join(ddl_base_dir, d) 
    for d in os.listdir(ddl_base_dir) 
    if os.path.isdir(os.path.join(ddl_base_dir, d))
])

for subdir in subdirs:
    sql_files = sorted(glob.glob(os.path.join(subdir, "*.sql")))
    for sql_file in sql_files:
        with open(sql_file, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        # Clean statements
        statements = [s.strip() for s in sql_content.split(";") if s.strip()]
        for stmt in statements:
            stmt_clean = clean_sql(stmt)
            try:
                spark.sql(stmt_clean)
            except Exception as e:
                # Some statements like IF NOT EXISTS are fine if they run out of order
                pass

print("DDL Execution completed.")

# 2. Populate shared conformed dimensions with mock rows
print("Populating conformed dimensions (dimProvider, dimMember, dimDate, dimMonth)...")

# dimProvider
provider_schema = StructType([
    StructField("providerKey", IntegerType(), True),
    StructField("providerID", StringType(), True),
    StructField("effectiveStartDate", StringType(), True),
    StructField("effectiveEndDate", StringType(), True),
    StructField("isCurrent", IntegerType(), True),
    StructField("npi", StringType(), True),
    StructField("tin", StringType(), True),
    StructField("lastName", StringType(), True),
    StructField("firstName", StringType(), True),
    StructField("middleName", StringType(), True),
    StructField("phoneNumber", StringType(), True),
    StructField("address1", StringType(), True),
    StructField("address2", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zipCode", StringType(), True),
    StructField("practiceCode", StringType(), True),
    StructField("practiceName", StringType(), True),
    StructField("providerOrgCode", StringType(), True),
    StructField("providerOrgName", StringType(), True),
    StructField("providerSpecialtyDescription", StringType(), True)
])
provider_data = [
    (12345, "12345", "2020-01-01", "2099-12-31", 1, "1234567890", "tin123", "Doe", "John", "", "555-0199", "123 Main St", "", "Phoenix", "AZ", "85001", "PRAC001", "Practice A", "ORG001", "Org A", "Cardiology")
]
df_prov = spark.createDataFrame(provider_data, schema=provider_schema)
from pyspark.sql.functions import col, to_date
df_prov = df_prov.withColumn("effectiveStartDate", to_date(col("effectiveStartDate"), "yyyy-MM-dd")) \
                 .withColumn("effectiveEndDate", to_date(col("effectiveEndDate"), "yyyy-MM-dd"))
df_prov.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("new_gold.dimprovider")

# Run conformed dimensions pipelines (dimDate, dimMonth, dimMember)
print("Running conformed dimensions pipelines...")

# Run Date
with open("/home/mi/Desktop/claim processing/Factrevenugugap/dimDate/DateProcessing.py", "r") as f:
    exec(f.read(), globals())

# Run Month
with open("/home/mi/Desktop/claim processing/Factrevenugugap/dimMonth/MonthProcessing.py", "r") as f:
    exec(f.read(), globals())

# Run Member
with open("/home/mi/Desktop/claim processing/Factrevenugugap/dimMember/MemberProcessing.py", "r") as f:
    exec(f.read(), globals())

# 3. Run dimClient processing
print("Running dimClient Processing...")
client_code_path = "/home/mi/Desktop/claim processing/Factrevenugugap/dimClient/DimClientProcessing.py"
with open(client_code_path, "r") as f:
    client_code = f.read()

# Replace targets in client code for local execution
client_code_clean = client_code \
    .replace("dbEnv = spark.conf.get(\"spark.databricks.clusterUsageTags.clusterOwnerOrgId\")", "dbEnv = '934226345849410'") \
    .replace("jdbcPassword = dbutils.secrets.get(scope = \"idapkeyvault\", key = \"ETLUSER-SQL\")", "jdbcPassword = 'mock_password'") \
    .replace("DestinationPath = '/mnt/'+ clientContainer.lower() + '/Platinum/dimClient'", "DestinationPath = '/tmp/spark-warehouse/new_gold/dimClient'")

# Exec client loading
exec(client_code_clean, globals())

# Register client table in catalog
spark.sql("DROP TABLE IF EXISTS new_gold.dimclient")
spark.sql("CREATE TABLE new_gold.dimclient USING DELTA LOCATION '/tmp/spark-warehouse/new_gold/dimClient'")

# 4. Run dimAlertGroup Parquet staging & loading
print("Running dimAlertGroup staging and loading...")
alert_staging_path = "/home/mi/Desktop/claim processing/Factrevenugugap/dimAlertGroup/AlertGroupStaging.py"
with open(alert_staging_path, "r") as f:
    exec(f.read(), globals())

# Setup gold_alertgroup
alert_gold_json = "/home/mi/Desktop/claim processing/Factrevenugugap/dimAlertGroup/gAlertgroup.json"
with open(alert_gold_json, "r") as f:
    conf = json.load(f)
for entity in conf["SubLayerProcessing"]:
    for src in entity["SourceTables"]:
        path = resolve_local_path(src["SourceTable"])
        df_src = spark.read.format("parquet").load(path)
        df_src.createOrReplaceTempView(src["Entity"])
    sql = clean_sql(entity["SQLScript"])
    mDF = spark.sql(sql)
    mDF.createOrReplaceTempView("tempSQLScript")
    
    dest_path = resolve_local_path(entity["DestinationTable"])
    mDF.limit(0).write.format("delta").mode("overwrite").save(dest_path)
    spark.read.format("delta").load(dest_path).createOrReplaceTempView("DestinationTable")
    
    merge = entity["MergeScript"].replace("DestinationTable", "`new_gold`.alertgroup")
    # We execute merge on the path registered table
    spark.sql("DROP TABLE IF EXISTS new_gold.alertgroup")
    spark.sql(f"CREATE TABLE new_gold.alertgroup USING DELTA LOCATION '{dest_path}'")
    merge_clean = merge.replace("new_gold.alertgroup", "`new_gold`.alertgroup")
    spark.sql(merge_clean)

# Process pdimalertgroup (Platinum Layer)
alert_plat_json = "/home/mi/Desktop/claim processing/Factrevenugugap/dimAlertGroup/pdimalertgroup.json"
with open(alert_plat_json, "r") as f:
    conf = json.load(f)
for entity in conf["SubLayerProcessing"]:
    for src in entity["SourceTables"]:
        path = resolve_local_path(src["SourceTable"])
        df_src = spark.read.format("delta").load(path)
        df_src.createOrReplaceTempView(src["Entity"])
    sql = clean_sql(entity["SQLScript"])
    mDF = spark.sql(sql)
    mDF.createOrReplaceTempView("tempSQLScript")
    
    dest_path = resolve_local_path(entity["DestinationTable"])
    mDF.limit(0).write.format("delta").mode("overwrite").save(dest_path)
    spark.read.format("delta").load(dest_path).createOrReplaceTempView("DestinationTable")
    
    merge = entity["MergeScript"].replace("DestinationTable", "`new_gold`.dimalertgroup")
    spark.sql("DROP TABLE IF EXISTS new_gold.dimalertgroup")
    spark.sql(f"CREATE TABLE new_gold.dimalertgroup USING DELTA LOCATION '{dest_path}'")
    spark.sql("DROP TABLE IF EXISTS global_gold.dimalertgroup")
    spark.sql(f"CREATE TABLE global_gold.dimalertgroup USING DELTA LOCATION '{dest_path}'")
    merge_clean = merge.replace("new_gold.dimalertgroup", "`new_gold`.dimalertgroup")
    spark.sql(merge_clean)

# 5. Run dimHCC loading
print("Running dimHCC loading...")
hcc_staging_path = "/home/mi/Desktop/claim processing/Factrevenugugap/dimHCC/HCCStaging.py"
with open(hcc_staging_path, "r") as f:
    exec(f.read(), globals())

# Run ghcc
hcc_gold_json = "/home/mi/Desktop/claim processing/Factrevenugugap/dimHCC/ghcc.json"
with open(hcc_gold_json, "r") as f:
    conf = json.load(f)
for entity in conf["SubLayerProcessing"]:
    for src in entity["SourceTables"]:
        path = resolve_local_path(src["SourceTable"])
        df_src = spark.read.format("parquet").load(path)
        df_src.createOrReplaceTempView(src["Entity"])
    sql = clean_sql(entity["SQLScript"])
    mDF = spark.sql(sql)
    mDF.createOrReplaceTempView("tempSQLScript")
    
    dest_path = resolve_local_path(entity["DestinationTable"])
    mDF.limit(0).write.format("delta").mode("overwrite").save(dest_path)
    spark.read.format("delta").load(dest_path).createOrReplaceTempView("DestinationTable")
    
    merge = entity["MergeScript"].replace("DestinationTable", "`new_gold`.hcc")
    spark.sql("DROP TABLE IF EXISTS new_gold.hcc")
    spark.sql(f"CREATE TABLE new_gold.hcc USING DELTA LOCATION '{dest_path}'")
    merge_clean = merge.replace("new_gold.hcc", "`new_gold`.hcc")
    spark.sql(merge_clean)

# Run pdimhcc
hcc_plat_json = "/home/mi/Desktop/claim processing/Factrevenugugap/dimHCC/pdimhcc.json"
with open(hcc_plat_json, "r") as f:
    conf = json.load(f)
for entity in conf["SubLayerProcessing"]:
    for src in entity["SourceTables"]:
        path = resolve_local_path(src["SourceTable"])
        df_src = spark.read.format("delta").load(path)
        df_src.createOrReplaceTempView(src["Entity"])
    sql = clean_sql(entity["SQLScript"])
    mDF = spark.sql(sql)
    mDF.createOrReplaceTempView("tempSQLScript")
    
    dest_path = resolve_local_path(entity["DestinationTable"])
    mDF.limit(0).write.format("delta").mode("overwrite").save(dest_path)
    spark.read.format("delta").load(dest_path).createOrReplaceTempView("DestinationTable")
    
    merge = entity["MergeScript"].replace("DestinationTable", "`new_gold`.dimhcc")
    spark.sql("DROP TABLE IF EXISTS new_gold.dimhcc")
    spark.sql(f"CREATE TABLE new_gold.dimhcc USING DELTA LOCATION '{dest_path}'")
    spark.sql("DROP TABLE IF EXISTS global_gold.dimhcc")
    spark.sql(f"CREATE TABLE global_gold.dimhcc USING DELTA LOCATION '{dest_path}'")
    merge_clean = merge.replace("new_gold.dimhcc", "`new_gold`.dimhcc")
    spark.sql(merge_clean)

# 6. Run Gap Engine (MemberRevenueGaps)
print("Running Gap Engine...")
notebook_path = "/home/mi/Desktop/claim processing/Factrevenugugap/factrevenugap/Silver/Notebooks/MemberRevenueGaps.ipynb"
with open(notebook_path, 'r', encoding='utf-8') as f:
    nb = json.load(f)
for cell in nb['cells']:
    if cell['cell_type'] == 'code':
        source = "".join(cell['source'])
        source_clean = clean_sql(source)
        exec(source_clean, globals())

# 7. Run Fact Table merge (factMemberRevenueGap.json)
print("Running Fact Table merge...")
fact_json = "/home/mi/Desktop/claim processing/Factrevenugugap/factrevenugap/Gold/Schema/factMemberRevenueGap.json"
with open(fact_json, "r") as f:
    conf = json.load(f)
for entity in conf["SubLayerProcessing"]:
    for src in entity["SourceTables"]:
        table_raw = src["SourceTable"].replace("#clientCode", "new")
        table_name = clean_sql(table_raw).lower()
        df_src = spark.table(table_name)
        df_src.createOrReplaceTempView(src["Entity"])
    # Load SQL Script
    sql_script_path = "/home/mi/Desktop/claim processing/Factrevenugugap/factrevenugap/Gold/Dataprocessing/factmemberrevenuegap_processing.sql"
    with open(sql_script_path, "r") as f:
        sql = f.read()
    sql_clean = clean_sql(sql)
    mDF = spark.sql(sql_clean)
    mDF.createOrReplaceTempView("tempSQLScript")
    
    # Load Merge Script
    merge_script_path = "/home/mi/Desktop/claim processing/Factrevenugugap/factrevenugap/Gold/DataUpdate/factmemberrevenuegap_update.sql"
    with open(merge_script_path, "r") as f:
        merge = f.read()
        
    merge_clean = merge.replace("DestinationTable", "new_gold.factmemberrevenuegap")
    spark.sql(merge_clean)

print("\nE2E EXECUTION COMPLETED SUCCESSFULLY!")
print("Here is the content of the conformed fact table:")
spark.sql("SELECT * FROM new_gold.factmemberrevenuegap").show(truncate=False)
