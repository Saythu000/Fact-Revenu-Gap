-- ======================================================================================
-- DDL: Silver Staging - silver_provider (Healthcare Provider Staging)
-- Standard: HL7 FHIR R4 (Practitioner / PractitionerRole Mapping)
-- Schema: claimsprocessing.silver.silver_provider
-- Description: Staging table for provider demographic and specialty profiles.
-- ======================================================================================

CREATE TABLE IF NOT EXISTS claimsprocessing.silver.silver_provider (
    bis_internal_person_id              STRING        COMMENT 'Internal person identifier across systems',
    unique_record                       STRING        COMMENT 'Unique record string identifier',
    client_id                           STRING        COMMENT 'Client identifier',
    file_id                             BIGINT        COMMENT 'Ingestion file control identifier',
    load_date_time                      TIMESTAMP     COMMENT 'Pipeline ingestion timestamp',
    file_layout_id                      STRING        COMMENT 'File layout format identifier',
    file_layout_description             STRING        COMMENT 'File layout format description',
    provider_id                         STRING        COMMENT 'Source provider identifier',
    last_name                           STRING        COMMENT 'Provider last / family name',
    middle_initial                      STRING        COMMENT 'Provider middle initial',
    first_name                          STRING        COMMENT 'Provider first / given name',
    taxonomy_code_1                     STRING        COMMENT 'Primary taxonomy code',
    hp_specialty_code_1                 STRING        COMMENT 'Primary health plan specialty code',
    adv_provider_specialty_code_1       STRING        COMMENT 'Primary advanced provider specialty code',
    taxonomy_code_2                     STRING        COMMENT 'Secondary taxonomy code',
    hp_specialty_code_2                 STRING        COMMENT 'Secondary health plan specialty code',
    adv_provider_specialty_code_2       STRING        COMMENT 'Secondary advanced provider specialty code',
    taxonomy_code_3                     STRING        COMMENT 'Tertiary taxonomy code',
    hp_specialty_code_3                 STRING        COMMENT 'Tertiary health plan specialty code',
    adv_provider_specialty_code_3       STRING        COMMENT 'Tertiary advanced provider specialty code',
    taxonomy_code_4                     STRING        COMMENT 'Quaternary taxonomy code',
    hp_specialty_code_4                 STRING        COMMENT 'Quaternary health plan specialty code',
    adv_provider_specialty_code_4       STRING        COMMENT 'Quaternary advanced provider specialty code',
    taxonomy_code_5                     STRING        COMMENT 'Quinary taxonomy code',
    hp_specialty_code_5                 STRING        COMMENT 'Quinary health plan specialty code',
    adv_provider_specialty_code_5       STRING        COMMENT 'Quinary advanced provider specialty code',
    npi                                 STRING        COMMENT '10-digit National Provider Identifier',
    prescribe_privilege                 STRING        COMMENT 'Prescribe privilege flag',
    dea                                 STRING        COMMENT 'Drug Enforcement Administration number',
    payor_id                            STRING        COMMENT 'Payer / Health Plan ID',
    contracted                          STRING        COMMENT 'Contracted network status flag',
    provider_hai                        STRING        COMMENT 'Hospital Affiliation Identifier',
    hospital_id                         STRING        COMMENT 'Primary hospital identifier',
    exclude_from_provider_reporting     STRING        COMMENT 'Exclusion from reporting flag',
    alt_prov_reporting_1                STRING        COMMENT 'Alternate reporting tier 1',
    alt_prov_reporting_2                STRING        COMMENT 'Alternate reporting tier 2',
    alt_prov_reporting_3                STRING        COMMENT 'Alternate reporting tier 3',
    alt_prov_reporting_4                STRING        COMMENT 'Alternate reporting tier 4',
    alt_prov_reporting_5                STRING        COMMENT 'Alternate reporting tier 5',
    alt_prov_reporting_6                STRING        COMMENT 'Alternate reporting tier 6',
    alt_prov_reporting_7                STRING        COMMENT 'Alternate reporting tier 7',
    alt_prov_reporting_8                STRING        COMMENT 'Alternate reporting tier 8',
    alt_prov_reporting_9                STRING        COMMENT 'Alternate reporting tier 9',
    alt_prov_reporting_10               STRING        COMMENT 'Alternate reporting tier 10',
    pmup                                STRING        COMMENT 'Primary Medical Group / Practice identifier',
    is_current_pmup                     INT           COMMENT 'Current PMUP indicator flag',
    hash_key                            STRING        COMMENT 'CDC Hash key'
) USING delta
COMMENT 'HL7 FHIR R4 Standardized Silver Staging Table for Provider Metadata';