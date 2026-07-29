CREATE TABLE IF NOT EXISTS claimsprocessing.gold.gold_icdhccxref (
 icd                 string
,icdCodeType         string
,icdEffectiveYear    int
,hccNumber           string
,hccVersion          string
,hccType             string
,hccEffectiveYear    int
,isPrimary           boolean
,effectiveStartDate  date
,effectiveEndDate    date
,icdHCCKey           bigint
) USING delta;
