-- HL7 FHIR R4 Standardized Client Dimension Delta Merge Engine
MERGE INTO claimsprocessing.gold.gold_dimclient AS client
USING temp_updates AS updates
ON client.client_key = updates.client_key
WHEN MATCHED AND (
   client.client_name <> updates.client_name 
   OR client.sub_client_name <> updates.sub_client_name
) THEN
  UPDATE SET
     client.client_name = updates.client_name
    ,client.sub_client_name = updates.sub_client_name
    ,client.hash_key = updates.hash_key
WHEN NOT MATCHED THEN
  INSERT (
     client_key
    ,client_code
    ,client_name
    ,sub_client_code
    ,sub_client_name
    ,hash_key
  )
  VALUES (
     updates.client_key
    ,updates.client_code
    ,updates.client_name
    ,updates.sub_client_code
    ,updates.sub_client_name
    ,updates.hash_key
  );
