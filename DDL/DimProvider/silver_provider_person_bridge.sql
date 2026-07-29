-- DDL for silver.provider_person_bridge table
-- DDL for silver.provider_person_bridge table
--Staging bridge table for deduplicating providers
CREATE TABLE IF NOT EXISTS new.silver.provider_person_bridge (
    BISInternalPersonID string,
    IsCurrent string,
    UniqueRecord string,
    FileLayoutID int,
    FileId bigint,
    LastName string,
    FirstName string,
    NPI string,
    DEA string,
    PayorID string,
    ProviderID string,
    hashKey string,
    IsCurrentProviderID bigint,
    IsCurrentNPI bigint,
    IsOriginalProviderID int,
    PMUP string,
    IsCurrentPMUP int
) USING delta;