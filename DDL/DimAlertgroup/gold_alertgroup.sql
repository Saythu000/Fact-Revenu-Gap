-- ======================================================================================
-- DDL: Gold Dimension - gold_dimalertgroup Alias Definition
-- Standard: HL7 FHIR R4 (DetectedIssue / Observation Category Mapping)
-- Schema: claimsprocessing.gold.gold_dimalertgroup
-- ======================================================================================

CREATE TABLE IF NOT EXISTS claimsprocessing.gold.gold_dimalertgroup (
    alert_group_key         BIGINT        COMMENT 'Surrogate primary key for the clinical alert group',
    alert_group_code        STRING        COMMENT 'Natural code identifying the clinical alert group category',
    alert_group_description STRING        COMMENT 'Detailed description of the clinical alert category',
    display_text            STRING        COMMENT 'User-friendly UI display text for clinical documentation workflows',
    sort_order              INT           COMMENT 'Sort order index for UI display ordering',
    is_active               BOOLEAN       COMMENT 'Active flag indicating if alert group is currently operational'
) USING delta
COMMENT 'HL7 FHIR R4 Standardized Gold Dimension Table for CDI Clinical Alert Classifications';