-- HL7 FHIR R4 Standardized Reference Client Metadata DDL
-- Resource Mapping: HL7 FHIR R4 Organization (https://www.hl7.org/fhir/R4/organization.html)

CREATE TABLE IF NOT EXISTS claimsprocessing.gold.ref_client_metadata (
  client_code      STRING   COMMENT 'Primary Client Code'
 ,client_name      STRING   COMMENT 'Primary Client Legal Name'
 ,sub_client_code  STRING   COMMENT 'Sub-Client / Division Code'
 ,sub_client_name  STRING   COMMENT 'Sub-Client / Division Name'
) USING delta;
