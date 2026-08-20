-- ======================================================================================
-- DDL: Gold Fact - gold_factmemberrevenuegap
-- Standard: HL7 FHIR R4 & Conformed Star Schema Alignment
-- Schema: claimsprocessing.gold.gold_factmemberrevenuegap
-- Description: Fact table capturing member-level risk adjustment revenue gaps, HCC closure status,
--              provider associations, and clinical touchpoint dates.
-- ======================================================================================

CREATE TABLE IF NOT EXISTS claimsprocessing.gold.gold_factmemberrevenuegap (
    pecYearMonthKey             INT           COMMENT 'Date/Month dimension foreign key YYYYMM',
    clientKey                  INT           COMMENT 'Client dimension foreign key',
    memberKey                  BIGINT        COMMENT 'Member dimension foreign key',
    memberGroupKey             STRING        COMMENT 'Member Group dimension key',
    planID                     STRING        COMMENT 'Health Plan Identifier',
    hccKey                     INT           COMMENT 'HCC dimension foreign key',
    snapshotDateKey            INT           COMMENT 'Snapshot date key YYYYMMDD',
    planProviderKey            BIGINT        COMMENT 'Provider dimension foreign key',
    alertGroupKey              INT           COMMENT 'Alert Group dimension foreign key',
    isHCCClosed                STRING        COMMENT 'Gap closure status flag (Y/N)',
    lastDCConfirmedDateKey     INT           COMMENT 'Last Direct Care confirmed date key',
    lastPCPVisitDateKey        INT           COMMENT 'Last PCP visit date key',
    lastAWVDateKey             INT           COMMENT 'Last Annual Wellness Visit date key',
    factMemberRevenueGapHashKey STRING       COMMENT 'Natural business composite key hash',
    fullRowHash                STRING        COMMENT 'SCD Type 1 row attribute comparison hash',
    loadDateKey                INT           COMMENT 'ETL ingestion date key YYYYMMDD'
) USING delta;

