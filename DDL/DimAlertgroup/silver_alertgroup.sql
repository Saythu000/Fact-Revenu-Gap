-- ======================================================================================
-- DDL: Silver Staging - silver_alertgroup (CDI Clinical Alert Staging)
-- Standard: HL7 FHIR R4 (DetectedIssue / Observation Category Mapping)
-- Schema: claimsprocessing.silver.silver_alertgroup
-- Description: Staging table for CDI clinical alert reference data with hash CDC tracking.
-- ======================================================================================

CREATE TABLE IF NOT EXISTS claimsprocessing.silver.silver_alertgroup (
    alert_group_id          INT           COMMENT 'Raw source identifier for the alert group',
    alert_group_code        STRING        COMMENT 'Natural code string for clinical alert category',
    alert_group_description STRING        COMMENT 'Detailed description of the alert category',
    display_text            STRING        COMMENT 'User-facing label text',
    sort_order              INT           COMMENT 'Sort ordering value',
    is_active               BOOLEAN       COMMENT 'Active status flag',
    hash_key                BIGINT        COMMENT 'Hash key generated for Delta Lake SCD Type 1 MERGE change tracking'
) USING delta
COMMENT 'HL7 FHIR R4 Standardized Silver Staging Table for Alert Group Metadata';
