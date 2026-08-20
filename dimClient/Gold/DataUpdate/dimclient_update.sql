-- HL7 FHIR R4 Standardized Client Dimension Delta Merge Engine
MERGE INTO claimsprocessing.gold.gold_dimclient AS client
USING temp_updates AS updates
ON client.clientKey = updates.clientKey
WHEN MATCHED AND (
   client.clientName <> updates.clientName 
   OR client.subClientName <> updates.subClientName
) THEN
  UPDATE SET
     client.clientName = updates.clientName
    ,client.subClientName = updates.subClientName
    ,client.hashKey = updates.hashKey
WHEN NOT MATCHED THEN
  INSERT (
     clientKey
    ,clientCode
    ,clientName
    ,subClientCode
    ,subClientName
    ,hashKey
  )
  VALUES (
     updates.clientKey
    ,updates.clientCode
    ,updates.clientName
    ,updates.subClientCode
    ,updates.subClientName
    ,updates.hashKey
  );

