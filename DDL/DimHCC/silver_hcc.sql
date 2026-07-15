CREATE TABLE IF NOT EXISTS claimsprocessing.silver.silver_hcc (
 HCCNumber  string
,HCCDescription  string
,HCCVersion  string
,HCCType  string
,IsChronic  boolean
,EffectiveYear  int
,EffectiveDateStart date
,EffectiveDateEnd date
,hashKey  int
) USING delta;