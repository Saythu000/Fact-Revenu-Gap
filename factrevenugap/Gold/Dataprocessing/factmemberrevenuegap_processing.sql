WITH sourceTbl AS
(
SELECT
  ifnull(dimMonth.monthKey, -99) as pecYearMonthKey
 ,ifnull(dimClient.client_key, -99) as clientKey
 ,ifnull(dimMember.memberKey, -99) as memberKey
 ,ifnull(dimMemberGroup.memberGroupKey, '-99') as memberGroupKey
 ,ifnull(mrg.planID, '') as planID
 ,ifnull(dimHCC.hcc_key, -99) as hccKey
 ,ifnull(dimDate1.dateKey, -99) as snapshotDateKey
 ,ifnull(dimProvider.providerKey, -99) as planProviderKey
 ,ifnull(dimAlertGroup.alertGroupKey, -99) as alertGroupKey
 ,case when mrg.closureReason is not null then 'Y' else 'N' end as isHCCClosed
 ,ifnull(dimDate2.dateKey, -99) as lastDCConfirmedDateKey
 ,ifnull(dimDate3.dateKey, -99) as lastPCPVisitDateKey
 ,ifnull(dimDate4.dateKey, -99) as lastAWVDateKey
 ,current_date()  as loadDate
from memberRevenueGap mrg
left join dimMonth
 on mrg.reportMonth = concat(dimMonth.yearNumber,lpad(dimMonth.monthNumber,2,'0'))
left join dimClient
 on upper(mrg.clientCode) = upper(dimClient.client_code)
left join dimMember
 on mrg.planMemberID = dimMember.planMemberID
 and CAST(dimMember.isCurrent AS INT) = 1
left join dimMemberGroup
 on dimMember.subscriberID = dimMemberGroup.SubscriberID
left join dimHCC
 on mrg.hccNumber = dimHCC.hcc_code
 and substring(mrg.reportMonth, 1,4) = dimHCC.effective_year
 and mrg.HCCVersion = dimHCC.hcc_model_version
 and upper(dimHCC.hcc_model_type) in ('COMM', 'ESRD', 'RX')
left join dimProvider
 on mrg.providerID = dimProvider.providerID
 and CAST(dimProvider.isCurrent AS INT) = 1
left join dimAlertGroup
 on mrg.alertCategory = dimAlertGroup.alertGroupCode
left join dimDate dimDate1
 on mrg.snapshotDate = dimDate1.date
left join dimDate dimDate2
 on mrg.lastDCConfirmedDate = dimDate2.date
left join dimDate dimDate3
 on mrg.lastPCPVisitDate = dimDate3.date
left join dimDate dimDate4
 on mrg.lastAWVDate = dimDate4.date
)
SELECT 
 pecYearMonthKey
,clientKey
,memberKey
,memberGroupKey
,planID
,hccKey
,snapshotDateKey
,planProviderKey
,alertGroupKey
,isHCCClosed
,lastDCConfirmedDateKey
,lastPCPVisitDateKey
,lastAWVDateKey
,sha2(concat_ws('|', pecYearMonthKey, memberKey, hccKey, clientKey), 256) as factMemberRevenueGapHashKey
,sha2(concat_ws('|',
   isHCCClosed,
   planID,
   snapshotDateKey,
   planProviderKey,
   alertGroupKey,
   lastDCConfirmedDateKey,
   lastPCPVisitDateKey,
   lastAWVDateKey), 256) as fullRowHash
,ifnull(dim.dateKey, -99) as loadDateKey
FROM sourceTbl sr
CROSS JOIN dimDate dim
ON sr.loadDate = dim.date
