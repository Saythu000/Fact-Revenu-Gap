-- ======================================================================================
-- DDL: Gold Dimension - dimAlertGroup (CDI Clinical Alert Classification)
-- Standard: HL7 FHIR R4 (DetectedIssue / Observation Category Mapping)
-- Schema: claimsprocessing.gold.gold_dimalertgroup
-- Description: Conformed Gold dimension table categorizing Clinical Documentation
--              Improvement (CDI) alert groups (e.g. DIABETES, CHF, COPD).
-- ======================================================================================

CREATE TABLE IF NOT EXISTS claimsprocessing.gold.gold_dimalertgroup (
 alertGroupKey         int     COMMENT 'Surrogate primary key for the clinical alert group',
 alertGroupCode        string  COMMENT 'Natural code identifying the clinical alert group category',
 alertGroupDescription string  COMMENT 'Detailed description of the clinical alert category',
 displayText            string  COMMENT 'User-friendly UI display text for clinical documentation workflows',
 sortOrder              int     COMMENT 'Sort order index for UI display ordering',
 isActive               boolean COMMENT 'Active flag indicating if alert group is currently operational'
) USING delta;

