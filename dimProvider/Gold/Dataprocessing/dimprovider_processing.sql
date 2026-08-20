-- ======================================================================================
-- Transformation: dimProvider (Healthcare Practitioner & Organization) Processing
-- Standard: HL7 FHIR R4 (Practitioner / PractitionerRole / Organization Mapping)
-- Source Table: claimsprocessing.silver.silver_provider_hierarchy
-- Target Table: claimsprocessing.gold.gold_dimprovider
-- Description: Extracts, deduplicates, and enriches healthcare provider profiles from
--              Silver Provider Hierarchy staging records into FHIR R4 snake_case attributes.
-- ======================================================================================

WITH ProviderHierarchy AS (
  SELECT 
     pgr.provider_id                          AS provider_id
    ,pgr.provider_npi                         AS npi
    ,pgr.location_tin                         AS tin
    ,pgr.provider_last_name                   AS last_name
    ,CAST(NULL AS STRING)                     AS first_name
    ,CAST(NULL AS STRING)                     AS middle_name
    ,pgr.phone_number                         AS phone_number
    ,pgr.location_address1                    AS address1
    ,pgr.location_address2                    AS address2
    ,pgr.location_city                        AS city
    ,pgr.location_state                       AS state
    ,pgr.location_zip                         AS zip_code
    ,pgr.location_id                          AS practice_code
    ,pgr.location_desc                        AS practice_name
    ,pgr.location_tin                         AS provider_org_code
    ,pgr.tier2_desc                           AS provider_org_name
    ,CASE WHEN pgr.provider_npi IS NULL THEN '' ELSE pgr.location_desc END AS provider_specialty_description
    ,CAST(NULL AS STRING)                     AS taxonomy_code_1
    ,CAST(NULL AS STRING)                     AS hp_specialty_code_1
    ,CAST(NULL AS STRING)                     AS adv_provider_specialty_code_1
    ,CAST(NULL AS STRING)                     AS taxonomy_code_2
    ,CAST(NULL AS STRING)                     AS hp_specialty_code_2
    ,CAST(NULL AS STRING)                     AS adv_provider_specialty_code_2
    ,CAST(NULL AS STRING)                     AS taxonomy_code_3
    ,CAST(NULL AS STRING)                     AS hp_specialty_code_3
    ,CAST(NULL AS STRING)                     AS adv_provider_specialty_code_3
    ,CAST(NULL AS STRING)                     AS taxonomy_code_4
    ,CAST(NULL AS STRING)                     AS hp_specialty_code_4
    ,CAST(NULL AS STRING)                     AS adv_provider_specialty_code_4
    ,CAST(NULL AS STRING)                     AS taxonomy_code_5
    ,CAST(NULL AS STRING)                     AS hp_specialty_code_5
    ,CAST(NULL AS STRING)                     AS adv_provider_specialty_code_5
    ,CAST(NULL AS STRING)                     AS is_prescribe_privilege
    ,CAST(NULL AS STRING)                     AS provider_dea
    ,CAST(NULL AS STRING)                     AS payer_id
    ,CAST(NULL AS STRING)                     AS is_contracted
    ,CAST(NULL AS STRING)                     AS provider_hai
    ,CAST(NULL AS STRING)                     AS hospital_id
    ,CAST(NULL AS STRING)                     AS is_excluded_from_provider_reporting
    ,CAST(NULL AS STRING)                     AS alt_prov_reporting_1
    ,CAST(NULL AS STRING)                     AS alt_prov_reporting_2
    ,CAST(NULL AS STRING)                     AS alt_prov_reporting_3
    ,CAST(NULL AS STRING)                     AS alt_prov_reporting_4
    ,CAST(NULL AS STRING)                     AS alt_prov_reporting_5
    ,CAST(NULL AS STRING)                     AS alt_prov_reporting_6
    ,CAST(NULL AS STRING)                     AS alt_prov_reporting_7
    ,CAST(NULL AS STRING)                     AS alt_prov_reporting_8
    ,CAST(NULL AS STRING)                     AS alt_prov_reporting_9
    ,CAST(NULL AS STRING)                     AS alt_prov_reporting_10
    ,'Targeted'                               AS program_type
    ,'New - Targeted'                         AS practice_targeted_status
    ,CAST(NULL AS STRING)                     AS product_id
    ,CAST(NULL AS STRING)                     AS provider_type
    ,ROW_NUMBER() OVER(PARTITION BY pgr.provider_id ORDER BY pgr.start_date DESC) AS row_number
  FROM provider_hierarchy pgr
),
ProviderHierarchyFiltered AS (
  SELECT *
  FROM ProviderHierarchy
  WHERE row_number = 1
),
CombinedProvider AS (
  SELECT 
     p.provider_id
    ,CURRENT_DATE()                           AS effective_start_date
    ,CAST(NULL AS DATE)                       AS effective_end_date
    ,1                                        AS is_current
    ,p.npi
    ,p.tin
    ,p.last_name
    ,p.first_name
    ,p.middle_name
    ,p.phone_number
    ,p.address1
    ,p.address2
    ,p.city
    ,p.state
    ,p.zip_code
    ,p.practice_code
    ,p.practice_name
    ,p.provider_org_code
    ,p.provider_org_name
    ,p.provider_specialty_description
    ,p.taxonomy_code_1
    ,p.hp_specialty_code_1
    ,p.adv_provider_specialty_code_1
    ,p.taxonomy_code_2
    ,p.hp_specialty_code_2
    ,p.adv_provider_specialty_code_2
    ,p.taxonomy_code_3
    ,p.hp_specialty_code_3
    ,p.adv_provider_specialty_code_3
    ,p.taxonomy_code_4
    ,p.hp_specialty_code_4
    ,p.adv_provider_specialty_code_4
    ,p.taxonomy_code_5
    ,p.hp_specialty_code_5
    ,p.adv_provider_specialty_code_5
    ,p.is_prescribe_privilege
    ,p.provider_dea
    ,p.payer_id
    ,p.is_contracted
    ,p.provider_hai
    ,p.hospital_id
    ,p.is_excluded_from_provider_reporting
    ,p.alt_prov_reporting_1
    ,p.alt_prov_reporting_2
    ,p.alt_prov_reporting_3
    ,p.alt_prov_reporting_4
    ,p.alt_prov_reporting_5
    ,p.alt_prov_reporting_6
    ,p.alt_prov_reporting_7
    ,p.alt_prov_reporting_8
    ,p.alt_prov_reporting_9
    ,p.alt_prov_reporting_10
    ,p.program_type
    ,p.practice_targeted_status
    ,p.product_id
    ,p.provider_type
  FROM ProviderHierarchyFiltered p
),
FinalProvider AS (
  SELECT 
      HASH(
         IFNULL(p.provider_id,""),"|"
        ,IFNULL(p.npi,""),"|"
        ,IFNULL(p.tin,""),"|"
        ,IFNULL(p.last_name,""),"|"
        ,IFNULL(p.first_name,""),"|"
        ,IFNULL(p.middle_name,""),"|"
        ,IFNULL(p.phone_number,""),"|"
        ,IFNULL(p.address1,""),"|"
        ,IFNULL(p.address2,""),"|"
        ,IFNULL(p.city,""),"|"
        ,IFNULL(p.state,""),"|"
        ,IFNULL(p.zip_code,""),"|"
        ,IFNULL(p.practice_code,""),"|"
        ,IFNULL(p.practice_name,""),"|"
        ,IFNULL(p.provider_org_code,""),"|"
        ,IFNULL(p.provider_org_name,""),"|"
        ,IFNULL(p.provider_specialty_description,""),"|"
        ,IFNULL(p.taxonomy_code_1,""),"|"
        ,IFNULL(p.hp_specialty_code_1,""),"|"
        ,IFNULL(p.adv_provider_specialty_code_1,""),"|"
        ,IFNULL(p.taxonomy_code_2,""),"|"
        ,IFNULL(p.hp_specialty_code_2,""),"|"
        ,IFNULL(p.adv_provider_specialty_code_2,""),"|"
        ,IFNULL(p.taxonomy_code_3,""),"|"
        ,IFNULL(p.hp_specialty_code_3,""),"|"
        ,IFNULL(p.adv_provider_specialty_code_3,""),"|"
        ,IFNULL(p.taxonomy_code_4,""),"|"
        ,IFNULL(p.hp_specialty_code_4,""),"|"
        ,IFNULL(p.adv_provider_specialty_code_4,""),"|"
        ,IFNULL(p.taxonomy_code_5,""),"|"
        ,IFNULL(p.hp_specialty_code_5,""),"|"
        ,IFNULL(p.adv_provider_specialty_code_5,""),"|"
        ,IFNULL(p.is_prescribe_privilege,""),"|"
        ,IFNULL(p.provider_dea,""),"|"
        ,IFNULL(p.payer_id,""),"|"
        ,IFNULL(p.is_contracted,""),"|"
        ,IFNULL(p.provider_hai,""),"|"
        ,IFNULL(p.hospital_id,""),"|"
        ,IFNULL(p.is_excluded_from_provider_reporting,""),"|"
        ,IFNULL(p.alt_prov_reporting_1,""),"|"
        ,IFNULL(p.alt_prov_reporting_2,""),"|"
        ,IFNULL(p.alt_prov_reporting_3,""),"|"
        ,IFNULL(p.alt_prov_reporting_4,""),"|"
        ,IFNULL(p.alt_prov_reporting_5,""),"|"
        ,IFNULL(p.alt_prov_reporting_6,""),"|"
        ,IFNULL(p.alt_prov_reporting_7,""),"|"
        ,IFNULL(p.alt_prov_reporting_8,""),"|"
        ,IFNULL(p.alt_prov_reporting_9,""),"|" 
        ,IFNULL(p.alt_prov_reporting_10,""),"|"
        ,IFNULL(p.program_type,""),"|"
        ,IFNULL(p.practice_targeted_status,""),"|"
        ,IFNULL(p.product_id,""),"|"
        ,IFNULL(p.provider_type,"")
      ) AS provider_key
     ,p.provider_id
     ,p.effective_start_date
     ,p.effective_end_date
     ,p.is_current
     ,p.npi
     ,p.tin
     ,p.last_name
     ,p.first_name
     ,p.middle_name
     ,p.phone_number
     ,p.address1
     ,p.address2
     ,p.city
     ,p.state
     ,p.zip_code
     ,p.practice_code
     ,p.practice_name
     ,p.provider_org_code
     ,p.provider_org_name
     ,p.provider_specialty_description
     ,p.taxonomy_code_1
     ,p.hp_specialty_code_1
     ,p.adv_provider_specialty_code_1
     ,p.taxonomy_code_2
     ,p.hp_specialty_code_2
     ,p.adv_provider_specialty_code_2
     ,p.taxonomy_code_3
     ,p.hp_specialty_code_3
     ,p.adv_provider_specialty_code_3
     ,p.taxonomy_code_4
     ,p.hp_specialty_code_4
     ,p.adv_provider_specialty_code_4
     ,p.taxonomy_code_5
     ,p.hp_specialty_code_5
     ,p.adv_provider_specialty_code_5
     ,p.is_prescribe_privilege
     ,p.provider_dea
     ,p.payer_id
     ,p.is_contracted
     ,p.provider_hai
     ,p.hospital_id
     ,p.is_excluded_from_provider_reporting
     ,p.alt_prov_reporting_1
     ,p.alt_prov_reporting_2
     ,p.alt_prov_reporting_3
     ,p.alt_prov_reporting_4
     ,p.alt_prov_reporting_5
     ,p.alt_prov_reporting_6
     ,p.alt_prov_reporting_7
     ,p.alt_prov_reporting_8
     ,p.alt_prov_reporting_9
     ,p.alt_prov_reporting_10
     ,p.program_type
     ,p.practice_targeted_status
     ,p.product_id
     ,p.provider_type
  FROM CombinedProvider p
)
SELECT * FROM FinalProvider;
