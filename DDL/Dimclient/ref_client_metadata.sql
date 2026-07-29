CREATE TABLE IF NOT EXISTS claimsprocessing.gold.ref_client_metadata (
 clientCode  string
,clientName  string
,subClientCode  string
,subClientName  string
) USING delta;
