-- HL7 FHIR R4 Standardized Client Dimension Transformation Query
-- Maps raw client metadata to FHIR R4 Organization schema
SELECT DISTINCT
  cast(hash(clientCode, subClientCode) as int) AS clientKey
 ,clientCode
 ,IFNULL(clientName, 'Unspecified') AS clientName
 ,subClientCode
 ,IFNULL(subClientName, 'Unspecified') AS subClientName
 ,cast(hash(clientCode, subClientCode, IFNULL(clientName, 'Unspecified'), IFNULL(subClientName, 'Unspecified')) as int) AS hashKey
FROM client_raw;

