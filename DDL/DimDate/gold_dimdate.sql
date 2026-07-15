CREATE TABLE IF NOT EXISTS claimsprocessing.gold.gold_dimdate (
 dateKey  int
,date date
,shortDateName  string
,longDateName  string
,yearNumber  int
,yearName  string
,quarterKey  int
,quarterNumber  int
,quarterName  string
,quarterOfYearNumber  int
,quarterOfYearName  string
,monthKey  int
,monthNumber  int
,monthName  string
,monthOfQuarterNumber  int
,monthOfQuarterName  string
,monthOfYearShortName  string
,weekKey  int
,weekNumber  int
,weekName  string
,dayOfWeekNumber  int
,dayOfWeekName  string
,dayOfYear  int
,isWorkDay  boolean
) USING delta;