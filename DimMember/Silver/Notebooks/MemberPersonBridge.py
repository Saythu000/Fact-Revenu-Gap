# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Setup logging
import logging
from datetime import datetime
import os, sys, subprocess

# Ensure recordlinkage library is installed safely
try:
    import recordlinkage
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "recordlinkage"])
    import recordlinkage

# Create logs directory if it doesn't exist
base_dir = os.getcwd()
log_dir = os.path.abspath(os.path.join(base_dir, "../../logs"))
os.makedirs(log_dir, exist_ok=True)

log_filename = f"{log_dir}/member_person_bridge_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename.replace('file:', '')),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)
logger.info("Member Person Bridge Processing Started")

# COMMAND ----------

# DBTITLE 1,Configuration - Auto-detect source
sourcePath = "/Volumes/claimsprocessing/bronze/member_consolidated/"
silverTable = "claimsprocessing.silver.silver_member_person_bridge"

# Ensure target database and tables exist
spark.sql("CREATE DATABASE IF NOT EXISTS claimsprocessing.bronze")
spark.sql("CREATE DATABASE IF NOT EXISTS claimsprocessing.silver")

spark.sql("""
CREATE TABLE IF NOT EXISTS claimsprocessing.silver.silver_member_person_bridge (
    ESAIInternalPersonID STRING,
    IsCurrent INT,
    UniqueRecord STRING,
    FileLayoutID INT,
    FileId BIGINT,
    LastName STRING,
    FirstName STRING,
    DateOfBirth STRING,
    Gender STRING,
    PermanentAddressLine1 STRING,
    PhoneNumber STRING,
    PlanMemberID STRING,
    BeneficiaryID STRING,
    UniquePersonKey STRING,
    hashKey STRING,
    IsCurrentPlanMemberID INT,
    IsCurrentUniquePersonKey INT,
    IsOriginalMemberID INT,
    PMUP STRING,
    IsCurrentPMUP INT
) USING delta;
""")

logger.info("Configuration loaded:")
logger.info(f"  Source Path: {sourcePath}")
logger.info(f"  Silver Table: {silverTable}")

# COMMAND ----------

# DBTITLE 1,Import libraries
from pyspark.sql.functions import date_format, monotonically_increasing_id, sha2, concat_ws, col, row_number, substring, lower, coalesce, upper, trim, regexp_replace
from pyspark.sql.window import Window
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from delta.tables import DeltaTable

# COMMAND ----------

# DBTITLE 1,Load source data
def LoadSource(sourcePath):
    logger.info(f"Starting LoadSource from: {sourcePath}")
    
    # Check if Delta Table transaction log exists at sourcePath
    if not DeltaTable.isDeltaTable(spark, sourcePath):
        logger.warning(f"No Delta table transaction log found at {sourcePath}. Returning clean empty DataFrame.")
        empty_schema = StructType([
            StructField("UniqueRecord", StringType(), True),
            StructField("FileLayoutID", IntegerType(), True),
            StructField("FileID", LongType(), True),
            StructField("RowNumber", IntegerType(), True),
            StructField("LastName", StringType(), True),
            StructField("FirstName", StringType(), True),
            StructField("Gender", StringType(), True),
            StructField("PhoneNumber", StringType(), True),
            StructField("PermanentAddressLine1", StringType(), True),
            StructField("DateofBirthFormatted", StringType(), True),
            StructField("PlanMemberID", StringType(), True),
            StructField("BeneficiaryID", StringType(), True),
            StructField("UniquePersonKey", StringType(), True),
            StructField("LastInitial", StringType(), True),
            StructField("FirstInitial", StringType(), True),
            StructField("PhoneNumberFormatted", StringType(), True)
        ])
        return spark.createDataFrame([], empty_schema)

    dfMBPGold = spark.read.format("delta").load(sourcePath)
    logger.info(f"Raw records loaded: {dfMBPGold.count()}")

    windowPartition = Window.partitionBy(col("FileId")).orderBy(col("RecordHash").desc())
    sparkMem_df = dfMBPGold.distinct() \
        .withColumn("RecordHash", sha2(concat_ws("||", *dfMBPGold.columns),256)) \
        .withColumn("RowNumber", row_number().over(windowPartition)) \
        .withColumn("UniqueRecord", concat_ws("-",col("FileID"),col("RowNumber"))) \
        .withColumn("BeneficiaryID", upper(trim(col("BeneficiaryID")))) \
        .withColumn("PlanMemberID", upper(trim(col("PlanMemberID")))) \
        .withColumn("UniquePersonKey", upper(trim(col("UniquePersonKey")))) \
        .withColumn("LastName", upper(trim(col("LastName")))) \
        .withColumn("FirstName", upper(trim(col("FirstName")))) \
        .withColumn("LastInitial", upper(substring(trim(col("LastName")),1,1))) \
        .withColumn("FirstInitial", upper(substring(trim(col("FirstName")),1,1))) \
        .withColumn("DateofBirthFormatted",date_format(trim(col("DateofBirth")),"yyyyMMdd")) \
        .withColumn("PhoneNumber", trim(col("PhoneNumber"))) \
        .withColumn("PhoneNumberFormatted", regexp_replace(trim(col("PhoneNumber")),"[^0-9]","")) \
        .withColumn("PermanentAddressLine1", upper(trim(col("PermanentAddressLine1")))) \
        .select("UniqueRecord","FileLayoutID","FileID","RowNumber","LastName","FirstName","Gender","PhoneNumber","PermanentAddressLine1","DateofBirthFormatted","PlanMemberID","BeneficiaryID","UniquePersonKey","LastInitial","FirstInitial","PhoneNumberFormatted")
    
    logger.info(f"Processed records after transformations: {sparkMem_df.count()}")
    return sparkMem_df

RuleMBIColumns = ["BeneficiaryID","FirstInitial","LastInitial","DateofBirthFormatted","PhoneNumberFormatted","PermanentAddressLine1",11]
RulePMIDColumns = ["PlanMemberID","FirstInitial","LastInitial","DateofBirthFormatted","PhoneNumberFormatted","PermanentAddressLine1",11]
RuleUPKColumns = ["UniquePersonKey","FirstInitial","LastInitial","DateofBirthFormatted","PhoneNumberFormatted","PermanentAddressLine1",11]
RuleOtherColumns = ["DateofBirthFormatted","FirstName", "LastName","PhoneNumberFormatted","PermanentAddressLine1",14]
RulesAll = [RuleMBIColumns, RulePMIDColumns, RuleUPKColumns, RuleOtherColumns]
CompareColumns = ["LastInitial", "FirstInitial", "LastName", "FirstName", "BeneficiaryID", "PlanMemberID", "UniquePersonKey", "DateofBirthFormatted", "PhoneNumberFormatted", "PermanentAddressLine1"]

# COMMAND ----------

# DBTITLE 1,Record linkage - Compare function
def RulesToCompare(rules, pandasMem_df):
    matchesAllRules_df = pd.DataFrame()
    for i, lst in enumerate(rules, 1):
        indexer = recordlinkage.Index()
        indexer.block(lst[0])
        candidatesBlock = indexer.index(pandasMem_df)
        compareBlock = recordlinkage.Compare()
        threshold = lst[-1]
        for col_name in CompareColumns:
            compareBlock.exact(col_name, col_name, label=str(col_name))
        features = compareBlock.compute(candidatesBlock, pandasMem_df)
        features[lst[0]] = features[lst[0]].apply(lambda x: x*10)
        matchesRule = features[features[lst[:-1]].sum(axis=1) >= threshold]
        matchesRule_df = matchesRule.index.to_frame()
        if len(matchesRule_df) > 0:
            matchesAllRules_df = pd.concat([matchesAllRules_df, matchesRule_df])
    return matchesAllRules_df

def RunLinking(sparkMem_df):
    pandasMem_df = sparkMem_df.toPandas()
    if pandasMem_df.empty:
        empty_schema = StructType([
            StructField("ESAIInternalPersonID", StringType(), True),
            StructField("IsCurrent", IntegerType(), True),
            StructField("UniqueRecord", StringType(), True),
            StructField("FileLayoutID", IntegerType(), True),
            StructField("FileId", LongType(), True),
            StructField("LastName", StringType(), True),
            StructField("FirstName", StringType(), True),
            StructField("DateOfBirth", StringType(), True),
            StructField("Gender", StringType(), True),
            StructField("PermanentAddressLine1", StringType(), True),
            StructField("PhoneNumber", StringType(), True),
            StructField("PlanMemberID", StringType(), True),
            StructField("BeneficiaryID", StringType(), True),
            StructField("UniquePersonKey", StringType(), True),
            StructField("MatchID", StringType(), True)
        ])
        return spark.createDataFrame([], empty_schema)

    pandasMem_df = pandasMem_df.set_index("UniqueRecord", drop=False)
    
    dfCombined = RulesToCompare(RulesAll, pandasMem_df)
    
    if dfCombined.empty:
        finalPandas_df = pandasMem_df.copy()
        finalPandas_df['MatchID'] = finalPandas_df['UniqueRecord']
        return spark.createDataFrame(finalPandas_df.astype(str))
    
    dfCombined.columns = [0, 1]
    dfMatchedColumnA = dfCombined.rename(columns={0:"A", 1:"B"})
    dfMatchedColumnB = dfCombined.rename(columns={0:"B", 1:"A"})
    
    dfMatched = pd.concat([dfMatchedColumnA, dfMatchedColumnB], ignore_index=True)
    dfMatched.drop_duplicates(inplace=True)
    
    matchesAll_df = pd.concat([
        dfMatched[["A", "B"]].rename(columns={"A": "Record", "B": "Match"}),
        dfMatched[["B", "A"]].rename(columns={"B": "Record", "A": "Match"})
    ]).reset_index(drop=True)
    
    matchesSame_df = matchesAll_df[["Record", "Record"]].copy()
    matchesSame_df.columns = ['Record', 'Match']
    
    matchesAll_df = pd.concat([matchesAll_df, matchesSame_df], ignore_index=True)
    matchesAll_df.drop_duplicates(inplace=True)
    
    distinctRow = dfMatched.groupby('A').head(1).drop('B', axis=1)
    matchesAll_df = pd.merge(matchesAll_df, distinctRow, how="left", left_on="Record", right_on="A").drop('A', axis=1)
    
    matchesAll_df = pd.merge(matchesAll_df, pandasMem_df["UniqueRecord"], left_on="Record", right_index=True).rename(columns={"UniqueRecord":"RecordID"})
    matchesAll_df = pd.merge(matchesAll_df, pandasMem_df["UniqueRecord"], left_on="Match", right_index=True).rename(columns={"UniqueRecord":"MatchID"})
    
    matched_df = matchesAll_df.groupby("RecordID").agg({"MatchID": lambda x: list(pd.unique(x))}).reset_index()
    matched_df["MatchID"] = matched_df["MatchID"].apply(lambda x: sorted(x))
    
    matchesAllModified = matchesAll_df.groupby("RecordID").head(1)
    drop_cols = ['MatchID', 'Match', 'Record']
    existing_drops = [c for c in drop_cols if c in matchesAllModified.columns]
    matchesAllModified = matchesAllModified.drop(columns=existing_drops)
    if 'index' in matchesAllModified.columns:
        matchesAllModified = matchesAllModified.drop(columns=['index'])
        
    newDFToMatch = pd.merge(matched_df, matchesAllModified, how="left", on="RecordID")
    finalPandas_df = pandasMem_df.reset_index(drop=True).merge(newDFToMatch, left_on="UniqueRecord", right_on="RecordID", how="left")
    finalPandas_df['MatchID'] = finalPandas_df['MatchID'].fillna(finalPandas_df['UniqueRecord'])
    
    return spark.createDataFrame(finalPandas_df.astype(str))

# COMMAND ----------

# DBTITLE 1,Define SQL transformations
finalSQL = """
WITH ESAIPersonWithIdentifiers AS(
SELECT 
   UniqueRecord,FileLayoutID,FileId,RowNumber,LastName,FirstName,DateofBirthFormatted AS DateOfBirth,Gender,PermanentAddressLine1,PhoneNumber,PlanMemberID,BeneficiaryID,UniquePersonKey
  ,case when instr(MatchID,',')=0 then MatchID else substr(MatchID,2,instr(MatchID,',')) end as MatchID
  ,ROW_NUMBER() OVER(PARTITION BY (case when instr(MatchID,',')=0 then MatchID else substr(MatchID,2,instr(MatchID,',')) end) ORDER BY FileId ASC, RowNumber ASC) AS FirstPersonIdentifier
  ,ROW_NUMBER() OVER(PARTITION BY (case when instr(MatchID,',')=0 then MatchID else substr(MatchID,2,instr(MatchID,',')) end) ORDER BY FileId DESC, RowNumber DESC) AS CurrentPersonIdentifier
FROM ESAICompletePersonTable
)
,MemberPersonBridge AS(
SELECT 
   fp.UniqueRecord AS ESAIInternalPersonID
  ,CASE WHEN cp.CurrentPersonIdentifier = 1 THEN 1 ELSE 0 END AS IsCurrent
  ,cp.UniqueRecord,cp.FileLayoutID,cp.FileId,cp.RowNumber,cp.LastName,cp.FirstName,cp.DateOfBirth,cp.Gender,cp.PermanentAddressLine1,cp.PhoneNumber,cp.PlanMemberID,cp.BeneficiaryID,cp.UniquePersonKey,cp.MatchID
FROM ESAIPersonWithIdentifiers cp
  LEFT JOIN ESAIPersonWithIdentifiers fp ON cp.MatchId = fp.MatchId AND fp.FirstPersonIdentifier = 1
)
,MemberPersonBridge_CurrPlanMbr AS(
SELECT 
   ESAIInternalPersonID,IsCurrent,UniqueRecord,FileLayoutID,FileId,RowNumber,LastName,FirstName,DateOfBirth,Gender,PermanentAddressLine1,PhoneNumber,PlanMemberID,BeneficiaryID
  ,ifnull(nullif(PlanMemberID,'None'),'') AS PlanMemberIdModified
  ,ifnull(nullif(UniquePersonKey,'None'),'') AS UniquePersonKeyModified
  ,UniquePersonKey,MatchID
  ,case when ifnull(PlanMemberID,'None')='None' then null when row_number() over(partition by PlanMemberID order by COALESCE(FileId,0) desc, COALESCE(RowNumber,0) desc) = 1 then 1 else 0 end as IsCurrentPlanMemberID
  ,case when ifnull(UniquePersonKey,'None')='None' then null when row_number() over(partition by UniquePersonKey order by COALESCE(FileId,0) desc, COALESCE(RowNumber,0) desc) = 1 then 1 else 0 end as IsCurrentUniquePersonKey
  ,case when ESAIInternalPersonID = UniqueRecord then 1 else 0 end AS IsOriginalMemberID 
FROM MemberPersonBridge
)
,PUModPop AS(
SELECT *
    ,CASE WHEN PlanMemberIdModified <> '' THEN 1 ELSE 0 END AS IsPlanMemberIdPopulated
    ,CASE WHEN UniquePersonKeyModified <> '' THEN 1 ELSE 0 END AS IsUniquePersonKeyModifiedPopulated
    ,concat(PlanMemberIdModified,'-',UniquePersonKeyModified) AS PMUP
FROM MemberPersonBridge_CurrPlanMbr
)
,Final AS (
SELECT *
    ,CAST(COALESCE(IsCurrentPlanMemberID,IsCurrentUniquePersonKey) AS STRING) AS IsCurrentPMUP
FROM PUModPop
)
SELECT 
   ESAIInternalPersonID
  ,IsCurrent
  ,UniqueRecord
  ,CAST(FileLayoutID AS INT) AS FileLayoutID
  ,FileId
  ,LastName
  ,FirstName
  ,DateOfBirth
  ,Gender
  ,PermanentAddressLine1
  ,PhoneNumber
  ,PlanMemberID
  ,BeneficiaryID
  ,UniquePersonKey
  ,sha2(concat_ws('|',IfNull(ESAIInternalPersonID,''),IfNull(IsCurrent,''),IfNull(UniqueRecord,''),IfNull(CAST(FileLayoutID AS STRING),''),IfNull(CAST(FileId AS STRING),''),IfNull(LastName,''),IfNull(FirstName,''),IfNull(DateOfBirth,''),IfNull(Gender,''),IfNull(PermanentAddressLine1,''),IfNull(PhoneNumber,''),IfNull(PlanMemberID,''),IfNull(BeneficiaryID,''),IfNull(UniquePersonKey,''),IfNull(CAST(IsCurrentPlanMemberID AS STRING),''),IfNull(CAST(IsCurrentUniquePersonKey AS STRING),''),IfNull(CAST(IsOriginalMemberID AS STRING),''),IfNull(PMUP,''),IfNull(TRY_CAST(IsCurrentPMUP AS STRING),'')), 256) AS hashKey
  ,IsCurrentPlanMemberID
  ,IsCurrentUniquePersonKey
  ,IsOriginalMemberID
  ,PMUP
  ,TRY_CAST(IsCurrentPMUP AS INT) AS IsCurrentPMUP
FROM Final
"""

# COMMAND ----------

# DBTITLE 1,Main execution - Process and write to Silver
print(f"\nProcessing data from: {sourcePath}")
sparkMem_df = LoadSource(sourcePath)
numRows = sparkMem_df.count()
print(f"Found {numRows} records")

if numRows == 0:
    print("No records to process in bronze consolidated directory.")
elif numRows == 1:
    print("Single record - skipping linkage, processing directly")
    convertedSpark_df = sparkMem_df.withColumn("MatchID", col("UniqueRecord"))
    convertedSpark_df = convertedSpark_df.withColumn("FileID", col("FileID").cast(LongType())).withColumn("RowNumber", col("RowNumber").cast(LongType()))
    convertedSpark_df.createOrReplaceTempView("ESAICompletePersonTable")
    temp_df = spark.sql(finalSQL)
    temp_df.write.format("delta").mode("append").saveAsTable(silverTable)
    print("\n Silver layer processing completed successfully!")
else:
    print("Running record linkage...")
    convertedSpark_df = RunLinking(sparkMem_df)
    convertedSpark_df = convertedSpark_df.withColumn("FileID", col("FileID").cast(LongType())).withColumn("RowNumber", col("RowNumber").cast(LongType()))
    convertedSpark_df.createOrReplaceTempView("ESAICompletePersonTable")
    temp_df = spark.sql(finalSQL)
    temp_df.write.format("delta").mode("append").saveAsTable(silverTable)
    print("\n Silver layer processing completed successfully!")
