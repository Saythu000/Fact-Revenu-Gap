# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Setup logging
import logging
from datetime import datetime
import os, sys

base_dir = os.getcwd()
log_dir = os.path.abspath(os.path.join(base_dir, "../../logs"))
os.makedirs(log_dir, exist_ok=True)

log_filename = f"{log_dir}/member_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename.replace('file:', '')),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)
logger.info("Member Silver Layer Processing Started")

# COMMAND ----------

# DBTITLE 1,Configuration - Set paths and parameters
sourcePath = "/Volumes/claimsprocessing/bronze/member_consolidated"
silverMemPerBrdgTable = "claimsprocessing.silver.silver_member_person_bridge"
silverMemTable = "claimsprocessing.silver.silver_member"

spark.sql("CREATE DATABASE IF NOT EXISTS claimsprocessing.silver")
spark.sql("""
CREATE TABLE IF NOT EXISTS claimsprocessing.silver.silver_member (
    ESAIInternalPersonID STRING,
    UniqueRecord STRING,
    ClientID STRING,
    FileID BIGINT,
    LoadDateTime STRING,
    FileLayoutID INT,
    FileLayoutDescription STRING,
    UniquePersonKey STRING,
    PlanMemberID STRING,
    SubscriberID STRING,
    BeneficiaryID STRING,
    LastName STRING,
    FirstName STRING,
    MiddleInitial STRING,
    EnrolleeUniqueID STRING,
    DateofBirth STRING,
    DeceasedDate STRING,
    Gender STRING,
    PermanentAddressLine1 STRING,
    PermanentAddressLine2 STRING,
    PermanentCity STRING,
    PermanentCounty STRING,
    PermanentState STRING,
    PermanentZipCode STRING,
    MailingAddressLine1 STRING,
    MailingAddressLine2 STRING,
    MailingCity STRING,
    MailingState STRING,
    MailingZipCode STRING,
    MailingCounty STRING,
    PhoneNumber STRING,
    Email STRING,
    MedicaidID STRING,
    Fax STRING,
    RaceCode STRING,
    RaceDataSource STRING,
    CaretakerFirstName STRING,
    CaretakerLastName STRING,
    CaretakerMiddleInitial STRING,
    EthnicityCode STRING,
    EthnicityDatasource STRING,
    SpokenLanguage STRING,
    SpokenLanguagesourcecode STRING,
    WrittenLanguageCode STRING,
    WrittenLanguageSourcecode STRING,
    OtherLanguage STRING,
    OtherLanguageSourcecode STRING,
    USCitizen STRING,
    AlternateKey1 STRING,
    AlternateKey2 STRING,
    AlternateKey3 STRING,
    AlternateKey4 STRING,
    AlternateKey5 STRING,
    AlternateKey6 STRING,
    AlternateKey7 STRING,
    AlternateKey8 STRING,
    AlternateKey9 STRING,
    AlternateKey10 STRING,
    MaskedMemberID STRING,
    EnrolleeEducation STRING,
    EnrolleeEmployment STRING,
    PMUP STRING,
    IsCurrentPMUP INT,
    ProductID STRING,
    hashKey STRING
) USING delta;
""")

logger.info("Configuration loaded:")
logger.info(f"  Source Path: {sourcePath}")
logger.info(f"  Silver Person Bridge Table: {silverMemPerBrdgTable}")
logger.info(f"  Silver Member Table: {silverMemTable}")

# COMMAND ----------

# DBTITLE 1,Import libraries
from pyspark.sql.functions import date_format, monotonically_increasing_id, sha2, concat_ws, col, row_number
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

# DBTITLE 1,Define SQL transformations
srcsql = """
WITH consolidateMem1 as (
  SELECT 
    brdg.ESAIInternalPersonID, brdg.UniqueRecord, mem.ClientID, mem.FileID, mem.LoadDateTime, 
    mem.FileLayoutID, mem.FileLayoutDescription, mem.UniquePersonKey, mem.PlanMemberID, 
    mem.SubscriberID, mem.BeneficiaryID, mem.LastName, mem.FirstName, mem.MiddleInitial, 
    mem.EnrolleeUniqueID, mem.DateofBirth, mem.DeceasedDate, mem.Gender, 
    mem.PermanentAddressLine1, mem.PermanentAddressLine2, mem.PermanentCity, 
    mem.PermanentCounty, mem.PermanentState, mem.PermanentZipCode, 
    mem.MailingAddressLine1, mem.MailingAddressLine2, mem.MailingCity, 
    mem.MailingState, mem.MailingZipCode, mem.MailingCounty, mem.PhoneNumber, 
    mem.Email, mem.MedicaidID, mem.Fax, mem.RaceCode, mem.RaceDataSource, 
    mem.CaretakerFirstName, mem.CaretakerLastName, mem.CaretakerMiddleInitial, 
    mem.EthnicityCode, mem.EthnicityDatasource, mem.SpokenLanguage, 
    mem.SpokenLanguagesourcecode, mem.WrittenLanguageCode, mem.WrittenLanguageSourcecode, 
    mem.OtherLanguage, mem.OtherLanguageSourcecode, mem.USCitizen, 
    mem.AlternateKey1, mem.AlternateKey2, mem.AlternateKey3, mem.AlternateKey4, 
    mem.AlternateKey5, mem.AlternateKey6, mem.AlternateKey7, mem.AlternateKey8, 
    mem.AlternateKey9, mem.AlternateKey10, mem.MaskedMemberID, mem.EnrolleeEducation, 
    mem.EnrolleeEmployment, brdg.PMUP, brdg.IsCurrentPMUP, mem.ProductID
  FROM consolidateMem mem
  INNER JOIN silverMemPerBrdg brdg 
    ON mem.UniqueRecord = brdg.UniqueRecord 
    AND mem.FileLayoutID = brdg.FileLayoutID 
    AND brdg.IsCurrentPMUP = 1
)
SELECT 
  ESAIInternalPersonID, UniqueRecord, ClientID, FileID, LoadDateTime, FileLayoutID, 
  FileLayoutDescription, UniquePersonKey, PlanMemberID, SubscriberID, BeneficiaryID, 
  LastName, FirstName, MiddleInitial, EnrolleeUniqueID, DateofBirth, DeceasedDate, 
  Gender, PermanentAddressLine1, PermanentAddressLine2, PermanentCity, PermanentCounty, 
  PermanentState, PermanentZipCode, MailingAddressLine1, MailingAddressLine2, 
  MailingCity, MailingState, MailingZipCode, MailingCounty, PhoneNumber, Email, 
  MedicaidID, Fax, RaceCode, RaceDataSource, CaretakerFirstName, CaretakerLastName, 
  CaretakerMiddleInitial, EthnicityCode, EthnicityDatasource, SpokenLanguage, 
  SpokenLanguagesourcecode, WrittenLanguageCode, WrittenLanguageSourcecode, 
  OtherLanguage, OtherLanguageSourcecode, USCitizen, AlternateKey1, AlternateKey2, 
  AlternateKey3, AlternateKey4, AlternateKey5, AlternateKey6, AlternateKey7, 
  AlternateKey8, AlternateKey9, AlternateKey10, MaskedMemberID, EnrolleeEducation, 
  EnrolleeEmployment, PMUP, IsCurrentPMUP, ProductID,
  sha2(concat_ws('|',
    IfNull(ESAIInternalPersonID,""), IfNull(UniqueRecord,""), IfNull(ClientID,""), IfNull(CAST(FileID AS STRING),""), IfNull(CAST(LoadDateTime AS STRING),""), IfNull(CAST(FileLayoutID AS STRING),""), IfNull(FileLayoutDescription,""), IfNull(UniquePersonKey,""), IfNull(PlanMemberID,""), IfNull(SubscriberID,""), IfNull(BeneficiaryID,""), IfNull(LastName,""), IfNull(FirstName,""), IfNull(MiddleInitial,""), IfNull(EnrolleeUniqueID,""), IfNull(CAST(DateofBirth AS STRING),""), IfNull(CAST(DeceasedDate AS STRING),""), IfNull(Gender,""), IfNull(PermanentAddressLine1,""), IfNull(PermanentAddressLine2,""), IfNull(PermanentCity,""), IfNull(PermanentCounty,""), IfNull(PermanentState,""), IfNull(PermanentZipCode,""), IfNull(MailingAddressLine1,""), IfNull(MailingAddressLine2,""), IfNull(MailingCity,""), IfNull(MailingState,""), IfNull(MailingZipCode,""), IfNull(MailingCounty,""), IfNull(PhoneNumber,""), IfNull(Email,""), IfNull(MedicaidID,""), IfNull(Fax,""), IfNull(RaceCode,""), IfNull(RaceDataSource,""), IfNull(CaretakerFirstName,""), IfNull(CaretakerLastName,""), IfNull(CaretakerMiddleInitial,""), IfNull(EthnicityCode,""), IfNull(EthnicityDatasource,""), IfNull(SpokenLanguage,""), IfNull(SpokenLanguagesourcecode,""), IfNull(WrittenLanguageCode,""), IfNull(WrittenLanguageSourcecode,""), IfNull(OtherLanguage,""), IfNull(OtherLanguageSourcecode,""), IfNull(USCitizen,""), IfNull(AlternateKey1,""), IfNull(AlternateKey2,""), IfNull(AlternateKey3,""), IfNull(AlternateKey4,""), IfNull(AlternateKey5,""), IfNull(AlternateKey6,""), IfNull(AlternateKey7,""), IfNull(AlternateKey8,""), IfNull(AlternateKey9,""), IfNull(AlternateKey10,""), IfNull(MaskedMemberID,""), IfNull(EnrolleeEducation,""), IfNull(EnrolleeEmployment,""), IfNull(PMUP,""), IfNull(CAST(IsCurrentPMUP AS STRING),""), IfNull(ProductID,"")), 256) AS hashKey
FROM consolidateMem1
"""

# COMMAND ----------

# DBTITLE 1,Main execution - Process and write to Silver
if not DeltaTable.isDeltaTable(spark, sourcePath):
    print(f"No Delta table transaction log found at {sourcePath}. Silver Member processing skipped safely.")
else:
    dfMBP = spark.read.format("delta").load(sourcePath)
    if dfMBP.count() == 0:
        print("No records in bronze consolidated. Silver Member processing skipped safely.")
    else:
        windowPartition = Window.partitionBy(col("FileId")).orderBy(col("RecordHash").desc())
        dfMBPGold = dfMBP.distinct() \
            .withColumn("RecordHash", sha2(concat_ws("||", *dfMBP.columns), 256)) \
            .withColumn("RowNumber", row_number().over(windowPartition)) \
            .withColumn("UniqueRecord", concat_ws("-", col("FileID"), col("RowNumber")))

        dfMBPGold.createOrReplaceTempView("consolidateMem")
        spark.table(silverMemPerBrdgTable).createOrReplaceTempView("silverMemPerBrdg")

        temp_df = spark.sql(srcsql)
        temp_df.write.format("delta").mode("append").saveAsTable(silverMemTable)
        print(f"Silver Member processing completed successfully into {silverMemTable}!")
