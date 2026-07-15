CREATE TABLE IF NOT EXISTS claimsprocessing.silver.silver_dimclient (
 clientKey  int
,clientCode  string
,clientName  string
,subClientCode  string
,subClientName  string
) USING delta;

