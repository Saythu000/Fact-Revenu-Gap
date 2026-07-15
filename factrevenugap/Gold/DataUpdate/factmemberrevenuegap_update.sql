MERGE INTO DestinationTable t
USING (SELECT * FROM tempSQLScript) s
ON s.factMemberRevenueGapHashKey = t.factMemberRevenueGapHashKey
WHEN MATCHED AND s.fullRowHash <> t.fullRowHash
THEN UPDATE SET
   pecYearMonthKey             = s.pecYearMonthKey
  ,clientKey                  = s.clientKey
  ,memberKey                  = s.memberKey
  ,planID                     = s.planID
  ,hccKey                     = s.hccKey
  ,snapshotDateKey            = s.snapshotDateKey
  ,planProviderKey            = s.planProviderKey
  ,alertGroupKey              = s.alertGroupKey
  ,isHCCClosed                = s.isHCCClosed
  ,lastDCConfirmedDateKey     = s.lastDCConfirmedDateKey
  ,lastPCPVisitDateKey        = s.lastPCPVisitDateKey
  ,lastAWVDateKey             = s.lastAWVDateKey
  ,fullRowHash                = s.fullRowHash
  ,loadDateKey                = s.loadDateKey
WHEN NOT MATCHED 
THEN INSERT
(
   pecYearMonthKey
  ,clientKey
  ,memberKey
  ,planID
  ,hccKey
  ,snapshotDateKey
  ,planProviderKey
  ,alertGroupKey
  ,isHCCClosed
  ,lastDCConfirmedDateKey
  ,lastPCPVisitDateKey
  ,lastAWVDateKey
  ,factMemberRevenueGapHashKey
  ,fullRowHash
  ,loadDateKey
)
VALUES
(
   s.pecYearMonthKey
  ,s.clientKey
  ,s.memberKey
  ,s.planID
  ,s.hccKey
  ,s.snapshotDateKey
  ,s.planProviderKey
  ,s.alertGroupKey
  ,s.isHCCClosed
  ,s.lastDCConfirmedDateKey
  ,s.lastPCPVisitDateKey
  ,s.lastAWVDateKey
  ,s.factMemberRevenueGapHashKey
  ,s.fullRowHash
  ,s.loadDateKey
)
