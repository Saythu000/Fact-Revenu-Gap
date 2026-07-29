CREATE TABLE IF NOT EXISTS claimsprocessing.silver.silver_member_group (
    SubscriberID        STRING,
    BeneficiaryID       STRING,
    CMSContractNumber   STRING,
    GroupNumber         STRING,
    GroupSuffix         STRING,
    StartDate           DATE,
    EndDate             DATE,
    SourceFileID        BIGINT,
    LoadDateTime        TIMESTAMP,
    hashKey             STRING
);
