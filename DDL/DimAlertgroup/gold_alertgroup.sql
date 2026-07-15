CREATE TABLE IF NOT EXISTS claimsprocessing.gold.gold_dimalertgroup(
 alertGroupKey  int
,alertGroupCode  string
,alertGroupDescription  string
,displayText  string
,sortOrder  int
,isActive  boolean
) USING delta;