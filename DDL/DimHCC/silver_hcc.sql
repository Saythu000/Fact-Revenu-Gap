-- ============================================================================
-- STAGING TABLE DDL: silver_hcc (Silver Layer)
-- Project Location: /home/mi/Desktop/claim processing/Factrevenugugap/DDL/DimHCC/
-- Target Table: claimsprocessing.silver.silver_hcc
-- 
-- HL7 FHIR R4 Alignment:
--   - Staging table for FHIR Condition & RiskAssessment categories
-- ============================================================================

CREATE TABLE IF NOT EXISTS claimsprocessing.silver.silver_hcc (
    -- ------------------------------------------------------------------------
    -- Staging Attributes (FHIR R4 Aligned)
    -- ------------------------------------------------------------------------
    hcc_code STRING,                            -- [FHIR: Condition.category.coding.code] HCC Category Code (e.g. 'HCC19')
    hcc_description STRING,                     -- [FHIR: Condition.code.text] Clinical Description
    hcc_model_version STRING,                   -- [FHIR: RiskAssessment.basis] CMS Model Version ('V24', 'V28', 'V08')
    hcc_model_type STRING,                      -- [FHIR: RiskAssessment.method] Risk Model Type ('COMM', 'ESRD', 'RX')
    is_chronic_condition BOOLEAN,               -- [FHIR: Condition.clinicalStatus] Chronic Flag (True/False)
    effective_year INT,                         -- Model Payment Effective Year
    effective_start_date DATE,                  -- [FHIR: Period.start] Effective Start Date
    effective_end_date DATE,                    -- [FHIR: Period.end] Effective End Date
    hash_key INT                                -- Staging record hash key
) USING delta;