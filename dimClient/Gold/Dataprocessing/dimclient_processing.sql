-- HL7 FHIR R4 Standardized Client Dimension Transformation Query
-- Maps raw client metadata to FHIR R4 Organization schema
SELECT DISTINCT
  cast(hash(clientCode, subClientCode) as bigint) AS client_key
 ,clientCode AS client_code
 ,IFNULL(clientName, 'Unspecified') AS client_name
 ,subClientCode AS sub_client_code
 ,IFNULL(subClientName, 'Unspecified') AS sub_client_name
 ,cast(hash(clientCode, subClientCode, IFNULL(clientName, 'Unspecified'), IFNULL(subClientName, 'Unspecified')) as bigint) AS hash_key
FROM client_raw;
