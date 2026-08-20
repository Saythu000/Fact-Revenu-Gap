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
 HCCNumber         STRING   COMMENT 'HCC Category Number (e.g., HCC19, HCC85)'
,HCCDescription    STRING   COMMENT 'CMS Official Clinical Description'
,HCCVersion        STRING   COMMENT 'Risk Adjustment Model Version (e.g., V28, V24)'
,HCCType           STRING   COMMENT 'Risk Model Category (COMM, ESRD, RX)'
,IsChronic         BOOLEAN  COMMENT 'Chronic Condition Indicator'
,EffectiveYear     INT      COMMENT 'Payment Model Effective Year'
,EffectiveDateStart DATE    COMMENT 'Validity Period Start Date'
,EffectiveDateEnd   DATE    COMMENT 'Validity Period End Date'
,hashKey           INT      COMMENT 'Delta Merge Change Hash Key'
,hccKey            INT      COMMENT 'Surrogate Primary Key - Category/Version Hash'
) USING delta;