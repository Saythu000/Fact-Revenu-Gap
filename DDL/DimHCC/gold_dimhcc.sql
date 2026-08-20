-- ============================================================================
-- DIMENSION TABLE DDL: gold_dimhcc (Gold Layer)
-- Project Location: /home/mi/Desktop/claim processing/Factrevenugugap/DDL/DimHCC/
-- Target Table: claimsprocessing.gold.gold_dimhcc
-- 
-- HL7 FHIR R4 Alignment:
--   - FHIR Condition Resource (https://www.hl7.org/fhir/R4/condition.html)
--   - FHIR RiskAssessment Resource (https://www.hl7.org/fhir/R4/riskassessment.html)
-- ============================================================================

CREATE TABLE IF NOT EXISTS claimsprocessing.gold.gold_dimhcc (
    -- ------------------------------------------------------------------------
    -- 1. Primary & Business Identifiers
    -- ------------------------------------------------------------------------
    hcc_key BIGINT,                             -- [FHIR: Identifier] Surrogate Primary Key (Deterministic MD5 Hash)
    hcc_code STRING,                            -- [FHIR: Condition.category.coding.code] HCC Category Code (e.g. 'HCC19', 'HCC85', 'HCC108')
    hcc_description STRING,                     -- [FHIR: Condition.code.text] Official CMS Clinical Description of the Category

    -- ------------------------------------------------------------------------
    -- 2. Risk Model Classification & Standards
    -- ------------------------------------------------------------------------
    hcc_model_version STRING,                   -- [FHIR: RiskAssessment.basis] CMS/HHS Model Version ('V21', 'V22', 'V24', 'V28', 'V08')
    hcc_model_type STRING,                      -- [FHIR: RiskAssessment.method] Risk Model Type ('COMM', 'ESRD', 'RX')
    code_system_uri STRING,                     -- [FHIR: Condition.code.coding.system] Terminology URI ('http://hl7.org/fhir/sid/icd-10-cm')

    -- ------------------------------------------------------------------------
    -- 3. Clinical & Validity Attributes
    -- ------------------------------------------------------------------------
    is_chronic_condition BOOLEAN,               -- [FHIR: Condition.clinicalStatus] Chronic Disease Flag (True = Chronic, False = Acute/Transient)
    hierarchy_parent_hcc_code STRING,           -- [FHIR: Condition.extension] Parent HCC code for hierarchical drop rules
    effective_year INT,                         -- Model Payment Year (e.g. 2026, 2027)
    effective_start_date DATE,                  -- [FHIR: Period.start] Payment Model Validity Start Date
    effective_end_date DATE,                    -- [FHIR: Period.end] Payment Model Validity End Date

    -- ------------------------------------------------------------------------
    -- 4. Financial Risk Weight Coefficients (CMS RAF Scores)
    -- ------------------------------------------------------------------------
    raf_weight_community DECIMAL(18,4),         -- [FHIR: RiskAssessment.prediction.relativeRisk] Community Risk Score Weight
    raf_weight_institutional DECIMAL(18,4),     -- [FHIR: RiskAssessment.prediction.relativeRisk] Institutional Risk Score Weight
    raf_weight_esrd DECIMAL(18,4),              -- [FHIR: RiskAssessment.prediction.relativeRisk] End-Stage Renal Disease Risk Score Weight

    -- ------------------------------------------------------------------------
    -- 5. Audit & Lineage Metadata
    -- ------------------------------------------------------------------------
    hash_key INT,                               -- Hash key for SCD change tracking
    load_timestamp TIMESTAMP                    -- ETL load timestamp
) USING delta;