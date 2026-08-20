CREATE TABLE IF NOT EXISTS claimsprocessing.gold.gold_dimmonth (
 monthKey  int
,monthNumber  int
,monthName  string
,yearNumber  int
,yearName  string
,quarterNumber  int
,quarterName  string
) USING delta;