WITH ProviderHierarchy AS (
  SELECT 
     pgr.PROVIDERID AS providerID
    ,pgr.PROVIDERNPI AS npi
    ,pgr.LOCATIONTIN AS tin
    ,pgr.PROVIDERLASTNAME AS lastName
    ,CAST(NULL AS string) AS firstName
    ,CAST(NULL AS string) AS middleName
    ,pgr.PHONENUMBER AS phoneNumber
    ,pgr.LOCATIONADDRESS1 AS address1
    ,pgr.LOCATIONADDRESS2 AS address2
    ,pgr.LOCATIONCITY AS city
    ,pgr.LOCATIONSTATE AS state
    ,pgr.LOCATIONZIP AS zipCode
    ,pgr.LOCATIONID AS practiceCode
    ,pgr.LOCATIONDESC AS practiceName
    ,pgr.LOCATIONTIN AS poCode
    ,pgr.TIER2DESC AS poName
    ,CASE WHEN pgr.PROVIDERNPI IS NULL THEN '' ELSE pgr.LOCATIONDESC END AS providerSpecialtyDescription
    ,CAST(NULL AS string) AS taxonomyCode1
    ,CAST(NULL AS string) AS hpSpecialtyCode1
    ,CAST(NULL AS string) AS advProviderSpecialtyCode1
    ,CAST(NULL AS string) AS taxonomyCode2
    ,CAST(NULL AS string) AS hpSpecialtyCode2
    ,CAST(NULL AS string) AS advProviderSpecialtyCode2
    ,CAST(NULL AS string) AS taxonomyCode3
    ,CAST(NULL AS string) AS hpSpecialtyCode3
    ,CAST(NULL AS string) AS advProviderSpecialtyCode3
    ,CAST(NULL AS string) AS taxonomyCode4
    ,CAST(NULL AS string) AS hpSpecialtyCode4
    ,CAST(NULL AS string) AS advProviderSpecialtyCode4
    ,CAST(NULL AS string) AS taxonomyCode5
    ,CAST(NULL AS string) AS hpSpecialtyCode5
    ,CAST(NULL AS string) AS advProviderSpecialtyCode5
    ,CAST(NULL AS string) AS isPrescribePrivilege
    ,CAST(NULL AS string) AS providerDEA
    ,CAST(NULL AS string) AS payerID
    ,CAST(NULL AS string) AS isContracted
    ,CAST(NULL AS string) AS providerHAI
    ,CAST(NULL AS string) AS hospitalID
    ,CAST(NULL AS string) AS isExcludedFromProviderReporting
    ,CAST(NULL AS string) AS altProvReporting1
    ,CAST(NULL AS string) AS altProvReporting2
    ,CAST(NULL AS string) AS altProvReporting3
    ,CAST(NULL AS string) AS altProvReporting4
    ,CAST(NULL AS string) AS altProvReporting5
    ,CAST(NULL AS string) AS altProvReporting6
    ,CAST(NULL AS string) AS altProvReporting7
    ,CAST(NULL AS string) AS altProvReporting8
    ,CAST(NULL AS string) AS altProvReporting9
    ,CAST(NULL AS string) AS altProvReporting10
    ,'Targeted' AS programType
    ,'New - Targeted' AS practiceTargetedStatus
    ,CAST(NULL AS string) AS ProductID
    ,CAST(NULL AS string) AS ProviderType
    ,ROW_NUMBER() OVER(PARTITION BY pgr.PROVIDERID ORDER BY pgr.STARTDATE DESC) AS RowNumber
  FROM provider_hierarchy pgr
),
ProviderHierarchyFiltered AS (
  SELECT *
  FROM ProviderHierarchy
  WHERE RowNumber = 1
),
CombinedProvider AS (
  SELECT 
     p.providerID
    ,CURRENT_DATE() AS effectiveStartDate
    ,CAST(null AS date) AS effectiveEndDate
    ,1 AS isCurrent
    ,p.npi
    ,p.tin
    ,p.lastName
    ,p.firstName
    ,p.middleName
    ,p.phoneNumber
    ,p.address1
    ,p.address2
    ,p.city
    ,p.state
    ,p.zipCode
    ,p.practiceCode
    ,p.practiceName
    ,p.poCode AS providerOrgCode
    ,p.poName AS providerOrgName
    ,p.providerSpecialtyDescription
    ,p.taxonomyCode1
    ,p.hpSpecialtyCode1
    ,p.advProviderSpecialtyCode1
    ,p.taxonomyCode2
    ,p.hpSpecialtyCode2
    ,p.advProviderSpecialtyCode2
    ,p.taxonomyCode3
    ,p.hpSpecialtyCode3
    ,p.advProviderSpecialtyCode3
    ,p.taxonomyCode4
    ,p.hpSpecialtyCode4
    ,p.advProviderSpecialtyCode4
    ,p.taxonomyCode5
    ,p.hpSpecialtyCode5
    ,p.advProviderSpecialtyCode5
    ,p.isPrescribePrivilege
    ,p.providerDEA
    ,p.payerID
    ,p.isContracted
    ,p.providerHAI
    ,p.hospitalID
    ,p.isExcludedFromProviderReporting
    ,p.altProvReporting1
    ,p.altProvReporting2
    ,p.altProvReporting3
    ,p.altProvReporting4
    ,p.altProvReporting5
    ,p.altProvReporting6
    ,p.altProvReporting7
    ,p.altProvReporting8
    ,p.altProvReporting9
    ,p.altProvReporting10
    ,p.programType
    ,p.practiceTargetedStatus
    ,p.ProductID
    ,p.ProviderType
  FROM ProviderHierarchyFiltered p
),
FinalProvider AS (
  SELECT 
      HASH(
         IFNULL(p.providerID,""),"|"
        ,IFNULL(p.npi,""),"|"
        ,IFNULL(p.tin,""),"|"
        ,IFNULL(p.lastName,""),"|"
        ,IFNULL(p.firstName,""),"|"
        ,IFNULL(p.middleName,""),"|"
        ,IFNULL(p.phoneNumber,""),"|"
        ,IFNULL(p.address1,""),"|"
        ,IFNULL(p.address2,""),"|"
        ,IFNULL(p.city,""),"|"
        ,IFNULL(p.state,""),"|"
        ,IFNULL(p.zipCode,""),"|"
        ,IFNULL(p.practiceCode,""),"|"
        ,IFNULL(p.practiceName,""),"|"
        ,IFNULL(p.providerOrgCode,""),"|"
        ,IFNULL(p.providerOrgName,""),"|"
        ,IFNULL(p.providerSpecialtyDescription,""),"|"
        ,IFNULL(p.taxonomyCode1,""),"|"
        ,IFNULL(p.hpSpecialtyCode1,""),"|"
        ,IFNULL(p.advProviderSpecialtyCode1,""),"|"
        ,IFNULL(p.taxonomyCode2,""),"|"
        ,IFNULL(p.hpSpecialtyCode2,""),"|"
        ,IFNULL(p.advProviderSpecialtyCode2,""),"|"
        ,IFNULL(p.taxonomyCode3,""),"|"
        ,IFNULL(p.hpSpecialtyCode3,""),"|"
        ,IFNULL(p.advProviderSpecialtyCode3,""),"|"
        ,IFNULL(p.taxonomyCode4,""),"|"
        ,IFNULL(p.hpSpecialtyCode4,""),"|"
        ,IFNULL(p.advProviderSpecialtyCode4,""),"|"
        ,IFNULL(p.taxonomyCode5,""),"|"
        ,IFNULL(p.hpSpecialtyCode5,""),"|"
        ,IFNULL(p.advProviderSpecialtyCode5,""),"|"
        ,IFNULL(p.isPrescribePrivilege,""),"|"
        ,IFNULL(p.providerDEA,""),"|"
        ,IFNULL(p.payerID,""),"|"
        ,IFNULL(p.isContracted,""),"|"
        ,IFNULL(p.providerHAI,""),"|"
        ,IFNULL(p.hospitalID,""),"|"
        ,IFNULL(p.isExcludedFromProviderReporting,""),"|"
        ,IFNULL(p.altProvReporting1,""),"|"
        ,IFNULL(p.altProvReporting2,""),"|"
        ,IFNULL(p.altProvReporting3,""),"|"
        ,IFNULL(p.altProvReporting4,""),"|"
        ,IFNULL(p.altProvReporting5,""),"|"
        ,IFNULL(p.altProvReporting6,""),"|"
        ,IFNULL(p.altProvReporting7,""),"|"
        ,IFNULL(p.altProvReporting8,""),"|"
        ,IFNULL(p.altProvReporting9,""),"|" 
        ,IFNULL(p.altProvReporting10,""),"|"
        ,IFNULL(p.programType,""),"|"
        ,IFNULL(p.practiceTargetedStatus,"")
        ,IFNULL(p.ProductID,""),"|"
        ,IFNULL(p.ProviderType,"")
     ) AS providerKey
    ,p.providerID
    ,p.effectiveStartDate
    ,p.effectiveEndDate
    ,p.isCurrent
    ,p.npi
    ,p.tin
    ,p.lastName
    ,p.firstName
    ,p.middleName
    ,p.phoneNumber
    ,p.address1
    ,p.address2
    ,p.city
    ,p.state
    ,p.zipCode
    ,p.practiceCode
    ,p.practiceName
    ,p.providerOrgCode
    ,p.providerOrgName
    ,p.providerSpecialtyDescription
    ,p.taxonomyCode1
    ,p.hpSpecialtyCode1
    ,p.advProviderSpecialtyCode1
    ,p.taxonomyCode2
    ,p.hpSpecialtyCode2
    ,p.advProviderSpecialtyCode2
    ,p.taxonomyCode3
    ,p.hpSpecialtyCode3
    ,p.advProviderSpecialtyCode3
    ,p.taxonomyCode4
    ,p.hpSpecialtyCode4
    ,p.advProviderSpecialtyCode4
    ,p.taxonomyCode5
    ,p.hpSpecialtyCode5
    ,p.advProviderSpecialtyCode5
    ,p.isPrescribePrivilege
    ,p.providerDEA
    ,p.payerID
    ,p.isContracted
    ,p.providerHAI
    ,p.hospitalID
    ,p.isExcludedFromProviderReporting
    ,p.altProvReporting1
    ,p.altProvReporting2
    ,p.altProvReporting3
    ,p.altProvReporting4
    ,p.altProvReporting5
    ,p.altProvReporting6
    ,p.altProvReporting7
    ,p.altProvReporting8
    ,p.altProvReporting9
    ,p.altProvReporting10
    ,p.programType
    ,p.practiceTargetedStatus
    ,p.ProductID
    ,p.ProviderType
  FROM CombinedProvider p
)
SELECT * FROM FinalProvider
