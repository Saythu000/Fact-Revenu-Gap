-- HL7 FHIR R4 Standardized Silver Client Metadata DDL
-- Resource Mapping: HL7 FHIR R4 Organization (https://www.hl7.org/fhir/R4/organization.html)

CREATE TABLE IF NOT EXISTS claimsprocessing.silver.silver_client_metadata (
  client_code      STRING   COMMENT 'Primary Client Code'
 ,sub_client_code  STRING   COMMENT 'Sub-Client Code'
) USING delta;
