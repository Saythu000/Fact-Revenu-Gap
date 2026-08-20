-- ============================================================================
-- BRIDGE TABLE DDL: gold_icdhccxref (Gold Layer)
-- Project Location: /home/mi/Desktop/claim processing/Factrevenugugap/DDL/DimHCC/
-- Target Table: claimsprocessing.gold.gold_icdhccxref
-- 
-- HL7 FHIR R4 Alignment:
--   - FHIR Condition Resource (https://www.hl7.org/fhir/R4/condition.html)
--   - Maps Clinical ICD-10-CM Diagnosis Codes to CMS/HHS HCC Categories
-- ============================================================================

CREATE TABLE IF NOT EXISTS claimsprocessing.gold.gold_icdhccxref (
    -- ------------------------------------------------------------------------
    -- 1. Primary & Relationship Keys
    -- ------------------------------------------------------------------------
    icd_hcc_key BIGINT,                          -- [FHIR: Identifier] Crosswalk Surrogate Primary Key (Deterministic Hash)
    condition_code STRING,                      -- [FHIR: Condition.code.coding.code] Clinical ICD-10-CM Diagnosis Code (e.g. 'E11.69')
    condition_code_type STRING,                 -- [FHIR: Condition.code.coding.system] ICD Code Standard ('10' for ICD-10-CM)
    condition_effective_year INT,               -- ICD-10 Coding Year (e.g. 2026)

    -- ------------------------------------------------------------------------
    -- 2. Target HCC Category Attributes
    -- ------------------------------------------------------------------------
    hcc_code STRING,                            -- [FHIR: Condition.category.coding.code] Mapped HCC Category Code (e.g. 'HCC19')
    hcc_model_version STRING,                   -- [FHIR: RiskAssessment.basis] Model Version ('V21', 'V22', 'V24', 'V28', 'V08')
    hcc_model_type STRING,                      -- [FHIR: RiskAssessment.method] Risk Model Type ('COMM', 'ESRD', 'RX')
    hcc_effective_year INT,                     -- HCC Model Effective Payment Year (e.g. 2026)

    -- ------------------------------------------------------------------------
    -- 3. Mapping Flags & Validity Period
    -- ------------------------------------------------------------------------
    is_primary_diagnosis BOOLEAN,               -- [FHIR: Condition.rank] Primary vs Secondary Diagnosis Mapping Flag (True/False)
    effective_start_date DATE,                  -- [FHIR: Period.start] Crosswalk Mapping Start Validity Date
    effective_end_date DATE                     -- [FHIR: Period.end] Crosswalk Mapping End Validity Date
) USING delta;
