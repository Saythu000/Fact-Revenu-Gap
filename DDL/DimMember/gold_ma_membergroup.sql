CREATE TABLE IF NOT EXISTS claimsprocessing.gold.gold_ma_membergroup (
    memberGroupKey      STRING,
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
