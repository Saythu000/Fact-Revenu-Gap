-- ======================================================================================
-- Transformation: Gold Fact - factmemberrevenuegap Processing
-- Standard: HL7 FHIR R4 & Conformed Star Schema Alignment
-- Target Table: claimsprocessing.gold.gold_factmemberrevenuegap
-- Description: Aggregates member revenue gaps with conformed Gold dimensions (Client, Member,
--              MemberGroup, HCC, Provider, AlertGroup, Date, Month).
-- ======================================================================================

WITH sourceTbl AS (
SELECT
   IFNULL(dimMonth.monthKey, -99) AS pecYearMonthKey
  ,IFNULL(dimClient.clientKey, -99) AS clientKey
  ,IFNULL(dimMember.memberKey, -99) AS memberKey
  ,IFNULL(dimMemberGroup.memberGroupKey, '-99') AS memberGroupKey
  ,IFNULL(mrg.planID, '') AS planID
  ,IFNULL(dimHCC.hccKey, -99) AS hccKey
  ,IFNULL(dimDate1.dateKey, -99) AS snapshotDateKey
  ,IFNULL(dimProvider.providerKey, -99) AS planProviderKey
  ,IFNULL(dimAlertGroup.alertGroupKey, -99) AS alertGroupKey
  ,CASE WHEN mrg.closureReason IS NOT NULL THEN 'Y' ELSE 'N' END AS isHCCClosed
  ,IFNULL(dimDate2.dateKey, -99) AS lastDCConfirmedDateKey
  ,IFNULL(dimDate3.dateKey, -99) AS lastPCPVisitDateKey
  ,IFNULL(dimDate4.dateKey, -99) AS lastAWVDateKey
  ,CURRENT_DATE() AS loadDate
FROM memberRevenueGap mrg
LEFT JOIN dimMonth
  ON mrg.reportMonth = CONCAT(dimMonth.yearNumber, LPAD(dimMonth.monthNumber, 2, '0'))
LEFT JOIN dimClient
  ON UPPER(mrg.clientCode) = UPPER(dimClient.clientCode)
LEFT JOIN dimMember
  ON mrg.planMemberID = dimMember.planMemberID
  AND CAST(dimMember.isCurrent AS INT) = 1
LEFT JOIN dimMemberGroup
  ON dimMember.subscriberID = dimMemberGroup.SubscriberID
LEFT JOIN dimHCC
  ON mrg.hccNumber = dimHCC.HCCNumber
  AND SUBSTRING(mrg.reportMonth, 1, 4) = dimHCC.EffectiveYear
  AND mrg.HCCVersion = dimHCC.HCCVersion
  AND UPPER(dimHCC.HCCType) IN ('COMM', 'ESRD', 'RX')
LEFT JOIN dimProvider
  ON mrg.providerID = dimProvider.ESAIInternalProviderID
  AND CAST(dimProvider.isCurrent AS INT) = 1
LEFT JOIN dimAlertGroup
  ON mrg.alertCategory = dimAlertGroup.alertGroupCode
LEFT JOIN dimDate dimDate1
  ON mrg.snapshotDate = dimDate1.date
LEFT JOIN dimDate dimDate2
  ON mrg.lastDCConfirmedDate = dimDate2.date
LEFT JOIN dimDate dimDate3
  ON mrg.lastPCPVisitDate = dimDate3.date
LEFT JOIN dimDate dimDate4
  ON mrg.lastAWVDate = dimDate4.date
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
  ,SHA2(CONCAT_WS('|', pecYearMonthKey, memberKey, hccKey, clientKey), 256) AS factMemberRevenueGapHashKey
  ,SHA2(CONCAT_WS('|',
     isHCCClosed,
     planID,
     snapshotDateKey,
     planProviderKey,
     alertGroupKey,
     lastDCConfirmedDateKey,
     lastPCPVisitDateKey,
     lastAWVDateKey), 256) AS fullRowHash
  ,IFNULL(dim.dateKey, -99) AS loadDateKey
FROM sourceTbl sr
CROSS JOIN dimDate dim
  ON sr.loadDate = dim.date;

