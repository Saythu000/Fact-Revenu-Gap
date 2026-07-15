WITH Type2ProvidersToUpdate AS(
SELECT 
   NULL AS pID
  ,a.*
FROM tempSQLScript a
  INNER JOIN DestinationTable t 
    ON a.providerID = t.providerID
      AND a.providerKey <> t.providerKey
      AND t.isCurrent = 1
),
AllProvidersFromSource AS(
SELECT 
   a.providerID AS pID
  ,a.*
FROM tempSQLScript a
),
ProvidersCombined AS(
SELECT * 
FROM Type2ProvidersToUpdate
UNION ALL
SELECT * 
FROM AllProvidersFromSource
)
MERGE INTO DestinationTable t 
USING (SELECT * FROM ProvidersCombined) s	
   ON s.pID = t.providerID 
WHEN MATCHED AND s.providerKey <> t.providerKey AND t.isCurrent = 1 THEN UPDATE SET	
	 t.effectiveEndDate = current_date() 
	,t.isCurrent = 0 
WHEN NOT MATCHED THEN INSERT 
( 
   providerKey
  ,effectiveStartDate
  ,effectiveEndDate
  ,isCurrent
  ,providerID
  ,npi
  ,tin
  ,lastName
  ,firstName
  ,middleName
  ,phoneNumber
  ,address1
  ,address2
  ,city
  ,state
  ,zipCode
  ,practiceCode
  ,practiceName
  ,providerOrgCode
  ,providerOrgName
  ,providerSpecialtyDescription
  ,taxonomyCode1
  ,hpSpecialtyCode1
  ,advProviderSpecialtyCode1
  ,taxonomyCode2
  ,hpSpecialtyCode2
  ,advProviderSpecialtyCode2
  ,taxonomyCode3
  ,hpSpecialtyCode3
  ,advProviderSpecialtyCode3
  ,taxonomyCode4
  ,hpSpecialtyCode4
  ,advProviderSpecialtyCode4
  ,taxonomyCode5
  ,hpSpecialtyCode5
  ,advProviderSpecialtyCode5
  ,isPrescribePrivilege
  ,providerDEA
  ,payerID
  ,isContracted
  ,providerHAI
  ,hospitalID
  ,isExcludedFromProviderReporting
  ,altProvReporting1
  ,altProvReporting2
  ,altProvReporting3
  ,altProvReporting4
  ,altProvReporting5
  ,altProvReporting6
  ,altProvReporting7
  ,altProvReporting8
  ,altProvReporting9
  ,altProvReporting10
  ,programType
  ,practiceTargetedStatus
  ,ProductID
  ,ProviderType
  ) 
 VALUES ( 
   s.providerKey
  ,s.effectiveStartDate
  ,s.effectiveEndDate
  ,s.isCurrent
  ,s.providerID
  ,s.npi
  ,s.tin
  ,s.lastName
  ,s.firstName
  ,s.middleName
  ,s.phoneNumber
  ,s.address1
  ,s.address2
  ,s.city
  ,s.state
  ,s.zipCode
  ,s.practiceCode
  ,s.practiceName
  ,s.providerOrgCode
  ,s.providerOrgName
  ,s.providerSpecialtyDescription
  ,s.taxonomyCode1
  ,s.hpSpecialtyCode1
  ,s.advProviderSpecialtyCode1
  ,s.taxonomyCode2
  ,s.hpSpecialtyCode2
  ,s.advProviderSpecialtyCode2
  ,s.taxonomyCode3
  ,s.hpSpecialtyCode3
  ,s.advProviderSpecialtyCode3
  ,s.taxonomyCode4
  ,s.hpSpecialtyCode4
  ,s.advProviderSpecialtyCode4
  ,s.taxonomyCode5
  ,s.hpSpecialtyCode5
  ,s.advProviderSpecialtyCode5
  ,s.isPrescribePrivilege
  ,s.providerDEA
  ,s.payerID
  ,s.isContracted
  ,s.providerHAI
  ,s.hospitalID
  ,s.isExcludedFromProviderReporting
  ,s.altProvReporting1
  ,s.altProvReporting2
  ,s.altProvReporting3
  ,s.altProvReporting4
  ,s.altProvReporting5
  ,s.altProvReporting6
  ,s.altProvReporting7
  ,s.altProvReporting8
  ,s.altProvReporting9
  ,s.altProvReporting10
  ,s.programType
  ,s.practiceTargetedStatus
  ,s.ProductID
  ,s.ProviderType
)
