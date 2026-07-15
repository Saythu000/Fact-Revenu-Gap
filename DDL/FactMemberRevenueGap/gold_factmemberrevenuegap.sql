CREATE TABLE IF NOT EXISTS claimsprocessing.gold.gold_factmemberrevenuegap (
   pecYearMonthKey             int
  ,clientKey                  int
  ,memberKey                  bigint
  ,planID                     string
  ,hccKey                     int
  ,snapshotDateKey            int
  ,planProviderKey            bigint
  ,alertGroupKey              int
  ,isHCCClosed                string
  ,lastDCConfirmedDateKey     int
  ,lastPCPVisitDateKey        int
  ,lastAWVDateKey             int
  ,factMemberRevenueGapHashKey string
  ,fullRowHash                string
  ,loadDateKey                int
) USING delta;
