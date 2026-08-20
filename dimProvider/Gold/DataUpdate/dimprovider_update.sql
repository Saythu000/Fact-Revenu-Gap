-- ======================================================================================
-- MERGE Script: dimProvider (Healthcare Practitioner & Organization) SCD Type 2 Load
-- Standard: HL7 FHIR R4 (Practitioner / PractitionerRole / Organization Alignment)
-- Target Table: claimsprocessing.gold.gold_dimprovider (DestinationTable)
-- Source Table: tempSQLScript
-- Description: Performs SCD Type 2 MERGE updates to maintain historical validity
--              windows and active flags for healthcare provider records.
-- ======================================================================================

WITH Type2ProvidersToUpdate AS (
SELECT 
   NULL AS pID
  ,a.*
FROM tempSQLScript a
  INNER JOIN DestinationTable t 
    ON a.ESAIInternalProviderID = t.ESAIInternalProviderID
      AND a.providerKey <> t.providerKey
      AND t.isCurrent = true
),
AllProvidersFromSource AS (
SELECT 
   a.ESAIInternalProviderID AS pID
  ,a.*
FROM tempSQLScript a
),
ProvidersCombined AS (
SELECT * 
FROM Type2ProvidersToUpdate
UNION ALL
SELECT * 
FROM AllProvidersFromSource
)
MERGE INTO DestinationTable t 
USING (SELECT * FROM ProvidersCombined) s	
   ON s.pID = t.ESAIInternalProviderID 
WHEN MATCHED AND s.providerKey <> t.providerKey AND t.isCurrent = true THEN UPDATE SET	
 	 t.effectiveEndDate = current_date() 
 	,t.isCurrent = false 
WHEN NOT MATCHED THEN INSERT 
( 
   providerKey
  ,effectiveStartDate
  ,effectiveEndDate
  ,isCurrent
  ,ESAIInternalProviderID
  ,identifier_npi
  ,identifier_tin
  ,name_family
  ,name_given_first
  ,name_given_middle
  ,telecom_phone
  ,address_line1
  ,address_line2
  ,address_city
  ,address_state
  ,address_postalCode
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
  ,extension_isPrescribePrivilege
  ,identifier_providerDEA
  ,identifier_payerID
  ,extension_isContracted
  ,extension_providerHAI
  ,identifier_hospitalID
  ,extension_isExcludedFromProviderReporting
  ,identifier_alternateKey1
  ,identifier_alternateKey2
  ,identifier_alternateKey3
  ,identifier_alternateKey4
  ,identifier_alternateKey5
  ,identifier_alternateKey6
  ,identifier_alternateKey7
  ,identifier_alternateKey8
  ,identifier_alternateKey9
  ,identifier_alternateKey10
  ,extension_programType
  ,extension_practiceTargetedStatus
  ,extension_ProductID
  ,extension_ProviderType
) 
VALUES ( 
   s.providerKey
  ,s.effectiveStartDate
  ,s.effectiveEndDate
  ,s.isCurrent
  ,s.ESAIInternalProviderID
  ,s.identifier_npi
  ,s.identifier_tin
  ,s.name_family
  ,s.name_given_first
  ,s.name_given_middle
  ,s.telecom_phone
  ,s.address_line1
  ,s.address_line2
  ,s.address_city
  ,s.address_state
  ,s.address_postalCode
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
  ,s.extension_isPrescribePrivilege
  ,s.identifier_providerDEA
  ,s.identifier_payerID
  ,s.extension_isContracted
  ,s.extension_providerHAI
  ,s.identifier_hospitalID
  ,s.extension_isExcludedFromProviderReporting
  ,s.identifier_alternateKey1
  ,s.identifier_alternateKey2
  ,s.identifier_alternateKey3
  ,s.identifier_alternateKey4
  ,s.identifier_alternateKey5
  ,s.identifier_alternateKey6
  ,s.identifier_alternateKey7
  ,s.identifier_alternateKey8
  ,s.identifier_alternateKey9
  ,s.identifier_alternateKey10
  ,s.extension_programType
  ,s.extension_practiceTargetedStatus
  ,s.extension_ProductID
  ,s.extension_ProviderType
);
