-- ======================================================================================
-- DDL: Silver Staging - silver_provider_person_bridge (Provider Deduplication Bridge)
-- Standard: HL7 FHIR R4 (Practitioner / PractitionerRole Alignment)
-- Schema: claimsprocessing.silver.silver_provider_person_bridge
-- Description: Staging bridge table for deduplicating providers across files and systems.
-- ======================================================================================

CREATE TABLE IF NOT EXISTS claimsprocessing.silver.silver_provider_person_bridge (
    bis_internal_person_id       STRING        COMMENT 'Internal person identifier across datasets',
    is_current                   STRING        COMMENT 'Current record validity status',
    unique_record                STRING        COMMENT 'Unique record identifier string',
    file_layout_id               INT           COMMENT 'File layout format ID',
    file_id                      BIGINT        COMMENT 'Ingestion file control identifier',
    last_name                    STRING        COMMENT 'Provider last name',
    first_name                   STRING        COMMENT 'Provider first name',
    npi                          STRING        COMMENT 'National Provider Identifier (NPI)',
    dea                          STRING        COMMENT 'Drug Enforcement Administration number',
    payor_id                     STRING        COMMENT 'Payer / Health Plan ID',
    provider_id                  STRING        COMMENT 'Provider business identifier',
    hash_key                     STRING        COMMENT 'CDC Hash key',
    is_current_provider_id       BIGINT        COMMENT 'Indicator flag for current provider ID',
    is_current_npi               BIGINT        COMMENT 'Indicator flag for current NPI',
    is_original_provider_id      INT           COMMENT 'Indicator flag for original provider ID',
    pmup                         STRING        COMMENT 'Primary Medical Group / Practice identifier',
    is_current_pmup              INT           COMMENT 'Current PMUP indicator flag'
) USING delta
COMMENT 'HL7 FHIR R4 Standardized Silver Staging Table for Provider Person Bridge';