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
    ON a.provider_id = t.provider_id
      AND a.provider_key <> t.provider_key
      AND t.is_current = 1
),
AllProvidersFromSource AS (
SELECT 
   a.provider_id AS pID
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
   ON s.pID = t.provider_id 
WHEN MATCHED AND s.provider_key <> t.provider_key AND t.is_current = 1 THEN UPDATE SET	
	 t.effective_end_date = current_date() 
	,t.is_current = 0 
WHEN NOT MATCHED THEN INSERT 
( 
   provider_key
  ,effective_start_date
  ,effective_end_date
  ,is_current
  ,provider_id
  ,npi
  ,tin
  ,last_name
  ,first_name
  ,middle_name
  ,phone_number
  ,address1
  ,address2
  ,city
  ,state
  ,zip_code
  ,practice_code
  ,practice_name
  ,provider_org_code
  ,provider_org_name
  ,provider_specialty_description
  ,taxonomy_code_1
  ,hp_specialty_code_1
  ,adv_provider_specialty_code_1
  ,taxonomy_code_2
  ,hp_specialty_code_2
  ,adv_provider_specialty_code_2
  ,taxonomy_code_3
  ,hp_specialty_code_3
  ,adv_provider_specialty_code_3
  ,taxonomy_code_4
  ,hp_specialty_code_4
  ,adv_provider_specialty_code_4
  ,taxonomy_code_5
  ,hp_specialty_code_5
  ,adv_provider_specialty_code_5
  ,is_prescribe_privilege
  ,provider_dea
  ,payer_id
  ,is_contracted
  ,provider_hai
  ,hospital_id
  ,is_excluded_from_provider_reporting
  ,alt_prov_reporting_1
  ,alt_prov_reporting_2
  ,alt_prov_reporting_3
  ,alt_prov_reporting_4
  ,alt_prov_reporting_5
  ,alt_prov_reporting_6
  ,alt_prov_reporting_7
  ,alt_prov_reporting_8
  ,alt_prov_reporting_9
  ,alt_prov_reporting_10
  ,program_type
  ,practice_targeted_status
  ,product_id
  ,provider_type
) 
VALUES ( 
   s.provider_key
  ,s.effective_start_date
  ,s.effective_end_date
  ,s.is_current
  ,s.provider_id
  ,s.npi
  ,s.tin
  ,s.last_name
  ,s.first_name
  ,s.middle_name
  ,s.phone_number
  ,s.address1
  ,s.address2
  ,s.city
  ,s.state
  ,s.zip_code
  ,s.practice_code
  ,s.practice_name
  ,s.provider_org_code
  ,s.provider_org_name
  ,s.provider_specialty_description
  ,s.taxonomy_code_1
  ,s.hp_specialty_code_1
  ,s.adv_provider_specialty_code_1
  ,s.taxonomy_code_2
  ,s.hp_specialty_code_2
  ,s.adv_provider_specialty_code_2
  ,s.taxonomy_code_3
  ,s.hp_specialty_code_3
  ,s.adv_provider_specialty_code_3
  ,s.taxonomy_code_4
  ,s.hp_specialty_code_4
  ,s.adv_provider_specialty_code_4
  ,s.taxonomy_code_5
  ,s.hp_specialty_code_5
  ,s.adv_provider_specialty_code_5
  ,s.is_prescribe_privilege
  ,s.provider_dea
  ,s.payer_id
  ,s.is_contracted
  ,s.provider_hai
  ,s.hospital_id
  ,s.is_excluded_from_provider_reporting
  ,s.alt_prov_reporting_1
  ,s.alt_prov_reporting_2
  ,s.alt_prov_reporting_3
  ,s.alt_prov_reporting_4
  ,s.alt_prov_reporting_5
  ,s.alt_prov_reporting_6
  ,s.alt_prov_reporting_7
  ,s.alt_prov_reporting_8
  ,s.alt_prov_reporting_9
  ,s.alt_prov_reporting_10
  ,s.program_type
  ,s.practice_targeted_status
  ,s.product_id
  ,s.provider_type
);
