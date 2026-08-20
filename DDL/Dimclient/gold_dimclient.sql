-- HL7 FHIR R4 Standardized Client Dimension DDL
-- Resource Mapping: HL7 FHIR R4 Organization (https://www.hl7.org/fhir/R4/organization.html)
-- Organization.identifier -> client_key, client_code
-- Organization.name -> client_name
-- Organization.partOf -> sub_client_code, sub_client_name

CREATE TABLE IF NOT EXISTS claimsprocessing.gold.gold_dimclient (
  client_key       BIGINT   COMMENT 'Surrogate Primary Key - Organization Identifier Hash'
 ,client_code      STRING   COMMENT 'Primary Client / Health Plan Code'
 ,client_name      STRING   COMMENT 'Primary Client / Health Plan Legal Name'
 ,sub_client_code  STRING   COMMENT 'Sub-Client / Division / Line of Business Code'
 ,sub_client_name  STRING   COMMENT 'Sub-Client / Division / Line of Business Display Name'
 ,hash_key         BIGINT   COMMENT 'Delta Lake SCD/Merge Change Hash Key'
) USING delta;
