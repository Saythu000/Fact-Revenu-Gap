SELECT DISTINCT
  cast(hash(clientCode, subClientCode) as bigint) AS clientKey
 ,clientCode AS clientCode
 ,IFNULL(clientName, 'Unspecified') AS clientName
 ,subClientCode AS subClientCode
 ,IFNULL(subClientName, 'Unspecified') AS subClientName
FROM client_raw;
