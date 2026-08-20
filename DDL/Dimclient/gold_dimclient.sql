-- HL7 FHIR R4 Standardized Client Dimension DDL
-- Resource Mapping: HL7 FHIR R4 Organization (https://www.hl7.org/fhir/R4/organization.html)
-- Organization.identifier -> client_key, client_code
-- Organization.name -> client_name
-- Organization.partOf -> sub_client_code, sub_client_name

CREATE TABLE IF NOT EXISTS claimsprocessing.gold.gold_dimclient (
  clientKey      int      COMMENT 'Surrogate Primary Key - Organization Identifier Hash'
 ,clientCode     string   COMMENT 'Primary Client / Health Plan Code'
 ,clientName     string   COMMENT 'Primary Client / Health Plan Legal Name'
 ,subClientCode  string   COMMENT 'Sub-Client / Division / Line of Business Code'
 ,subClientName  string   COMMENT 'Sub-Client / Division / Line of Business Display Name'
 ,hashKey        int      COMMENT 'Delta Lake SCD/Merge Change Hash Key'
) USING delta;

