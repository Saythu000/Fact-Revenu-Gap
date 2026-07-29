# 🏛️ Healthcare Risk Adjustment Factor (RAF) Data Warehouse Architecture
## Master Enterprise Documentation — `factmemberrevenuegap` & Conformed Star Schema

> **Document Version**: 1.0.0 (Production Release)  
> **Enterprise Domain**: Medicare Advantage (MA) Risk Adjustment, Clinical Documentation Improvement (CDI), RAPS / MAO-004 Returns  
> **Platform Stack**: Databricks Unity Catalog, PySpark 3.5+, Python 3.12, Delta Lake, `pyedi 1.1.0`, `pyx12 4.0.0`

---

## 📋 Table of Contents
1. [Executive Overview & Business Mission](#1-executive-overview--business-mission)
2. [End-to-End System Architecture Blueprint](#2-end-to-end-system-architecture-blueprint)
3. [Medallion Data Architecture & Stage Isolation](#3-medallion-data-architecture--stage-isolation)
4. [Exhaustive Data Dictionaries for All 8 Dimensions (182 Columns)](#4-exhaustive-data-dictionaries-for-all-8-dimensions-182-columns)
   * [4.1 Member Dimension (`gold_dimmember` — 61 Columns)](#41-member-dimension-gold_dimmember--61-columns)
   * [4.2 Member Group Dimension (`gold_ma_membergroup` — 11 Columns)](#42-member-group-dimension-gold_ma_membergroup--11-columns)
   * [4.3 Provider Dimension (`gold_dimprovider` — 57 Columns)](#43-provider-dimension-gold_dimprovider--57-columns)
   * [4.4 Client Dimension (`gold_dimclient` — 5 Columns)](#44-client-dimension-gold_dimclient--5-columns)
   * [4.5 CMS-HCC Dimension (`gold_dimhcc` — 11 Columns)](#45-cms-hcc-dimension-gold_dimhcc--11-columns)
   * [4.6 Alert Group Dimension (`gold_dimalertgroup` — 6 Columns)](#46-alert-group-dimension-gold_dimalertgroup--6-columns)
   * [4.7 Daily Date Dimension (`gold_dimdate` — 24 Columns)](#47-daily-date-dimension-gold_dimdate--24-columns)
   * [4.8 Monthly Period Dimension (`gold_dimmonth` — 7 Columns)](#48-monthly-period-dimension-gold_dimmonth--7-columns)
5. [Silver Risk Adjustment Revenue Gap Engine Mechanics](#5-silver-risk-adjustment-revenue-gap-engine-mechanics)
   * [5.1 Suspected CDI Alert Ingestion](#51-suspected-cdi-alert-ingestion)
   * [5.2 RAPS & MAO-004 CMS Encounter Matching Algorithm](#52-raps--mao-004-cms-encounter-matching-algorithm)
   * [5.3 Gap Closure Decision Tree & Rule Engine](#53-gap-closure-decision-tree--rule-engine)
6. [Gold Fact Table Engine (`factmemberrevenuegap`) Mechanics](#6-gold-fact-table-engine-factmemberrevenuegap-mechanics)
   * [6.1 Fact Table Column Specifications (16 Columns)](#61-fact-table-column-specifications-16-columns)
   * [6.2 Star Schema Relational Join Engine (`factmemberrevenuegap_processing.sql`)](#62-star-schema-relational-join-engine-factmemberrevenuegap_processingsql)
   * [6.3 Delta Lake Atomic MERGE Strategy (`factmemberrevenuegap_update.sql`)](#63-delta-lake-atomic-merge-strategy-factmemberrevenuegap_updatesql)
7. [Production Deployment & Operational Workflow Guide](#7-production-deployment--operational-workflow-guide)

---

## 1. Executive Overview & Business Mission

Under Medicare Advantage (MA) risk-adjustment payment models, the Centers for Medicare & Medicaid Services (CMS) reimburses health plans based on the **Risk Adjustment Factor (RAF)** score of enrolled beneficiaries. Chronic medical conditions (e.g., Diabetes with complications, Congestive Heart Failure, COPD) must be documented in medical records and successfully submitted to CMS every calendar year via **RAPS (Risk Adjustment Processing System)** or **MAO-004 (Encounter Data System)** returns.

If a chronic condition is suspected from historical clinical data (Clinical Documentation Improvement - CDI alerts) but has not yet been submitted to or accepted by CMS for the current payment year, a **Revenue Gap** exists. 

### Core Objectives of this Data Warehouse:
1. **Automate Raw Ingestion**: Parse HIPAA 834 EDI enrollment, 837 claims, provider rosters, RAPS returns, MAO-004 returns, and CDI alerts into Delta Lake format.
2. **Master Entity Resolution**: Execute deterministic record linkage across member identities (`MemberPersonBridge`) and provider hierarchies (`ProviderHierarchy`).
3. **Calculate Revenue Gaps**: Match CDI suspected alerts against RAPS and MAO-004 CMS encounter returns to classify gap closure reasons.
4. **Populate Star Schema**: Join calculated revenue gaps against 8 conformed dimensions to produce an audit-ready, high-performance Gold Fact table (`factmemberrevenuegap`).

---

## 2. End-to-End System Architecture Blueprint

```text
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                   📥 1. RAW INGESTION LAYER (SOURCE FEEDS)                              │
├───────────────────────────────────┬──────────────────────────────────┬─────────────────────────────────┤
│  834 EDI Member Enrollment        │  837 Claims & Provider Rosters   │  RAPS / MAO-004 / CDI Alerts    │
│  (source/834/pending/*.txt)       │  (source/837/pending/*.txt)      │  (HCCRaps, MAO004, BasePrinted) │
└─────────────────┬─────────────────┴────────────────┬─────────────────┴────────────────┬────────────────┘
                  │                                  │                                  │
                  ▼                                  ▼                                  │
┌──────────────────────────────────────────────────────────────────────┐                │
│                 🥉 2. BRONZE LAYER (PARQUET & DELTA VOLUMES)         │                │
├───────────────────────────────────┬──────────────────────────────────┤                │
│  FilesToProcess -> Raw Parquet    │  LoopConsolidation -> Delta Lake │                │
│  (/Volumes/.../bronze/member)     │  (/Volumes/.../consolidated)     │                │
└─────────────────┬─────────────────┴────────────────┬─────────────────┘                │
                  │                                  │                                  │
                  ▼                                  ▼                                  ▼
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                    🥈 3. SILVER LAYER (ENTITY RESOLUTION)                               │
├───────────────────────────────────┬──────────────────────────────────┬─────────────────────────────────┤
│  MemberPersonBridge (Linkage)     │  ProviderHierarchy & MemberGroup │  MemberRevenueGaps Engine       │
│  (ESAIInternalPersonID)           │  (silver_provider_hierarchy)     │  (silver_member_revenue_gap)    │
└─────────────────┬─────────────────┴────────────────┬─────────────────┴────────────────┬────────────────┘
                  │                                  │                                  │
                  ▼                                  ▼                                  ▼
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                 🥇 4. GOLD LAYER (STAR-SCHEMA DIMENSIONS & FACT)                        │
├───────────────┬───────────────┬───────────────┬───────────────┬───────────────┬───────────────┬────────┤
│ gold_dimmember│gold_ma_member │gold_dimprov   │gold_dimclient │  gold_dimhcc  │dimalertgroup  │dimdate │
│ (61 Columns)  │(11 Columns)   │(57 Columns)   │(5 Columns)    │  (11 Columns) │(6 Columns)    │& month │
└───────────────┴───────────────┴───────────────┴───────┬───────┴───────────────┴───────────────┴────────┘
                                                        │
                                                        ▼
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                              🏆 5. GOLD FACT TABLE: factmemberrevenuegap                                │
│        (Joined via factmemberrevenuegap_pipeline.py across all 8 Conformed Gold Dimensions)            │
└────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Medallion Data Architecture & Stage Isolation

The pipeline strictly adheres to Databricks Medallion Architecture:

1. **Bronze Layer**: Raw file tracking (`FilesToProcess`) and file consolidation (`LoopConsolidation`). Ingests raw text and CSV feeds, appends metadata columns (`SourceFileID`, `LoadDateTime`, `fullRowHash`), and writes immutable Parquet/Delta tables.
2. **Silver Layer**: Cleanses data, enforces types, and executes deterministic entity resolution algorithms:
   * **`MemberPersonBridge`**: Links members across 4 deterministic rules (MBI match, Plan Member ID match, Unique Person Key match, Demographics match) to assign `ESAIInternalPersonID`.
   * **`MemberRevenueGaps`**: Matches CDI suspected condition alerts against RAPS claims & MAO-004 encounters.
3. **Gold Layer**: Conformed star-schema data warehouse optimized for analytical querying, risk score aggregation, BI dashboards, and CMS compliance reporting.

---

## 4. Exhaustive Data Dictionaries for All 8 Dimensions (182 Columns)

### 4.1 Member Dimension (`gold_dimmember` — 61 Columns)

* **Data Flow**: `source/834/pending/*.txt` $\rightarrow$ `pyedi` / `pyx12 4.0.0` $\rightarrow$ `bronze_member` $\rightarrow$ `silver_member` $\rightarrow$ `MemberPersonBridge.py` $\rightarrow$ `claimsprocessing.gold.gold_dimmember`.

| # | Column Name | Data Type | Source Mapping / Rule | Business Description |
| :--- | :--- | :--- | :--- | :--- |
| 1 | `memberKey` | BIGINT | Monotonically Increasing PK | Primary surrogate key linking enrollee to fact tables. |
| 2 | `ESAIInternalPersonID` | STRING | `MemberPersonBridge` Linkage | Enterprise master cross-system person index. |
| 3 | `uniquePersonKey` | STRING | 834 Loop INS02 / REF*17 | Client-assigned master person identifier. |
| 4 | `planMemberID` | STRING | 834 REF*23 / NM109 | Health plan member ID on insurance card. |
| 5 | `subscriberID` | STRING | 834 REF*0F | Primary policy subscriber identifier. |
| 6 | `beneficiaryID` | STRING | CMS Medicare MBI | CMS Medicare Beneficiary Identifier. |
| 7 | `lastName` | STRING | 834 NM103 | Enrollee legal last name. |
| 8 | `firstName` | STRING | 834 NM104 | Enrollee legal first name. |
| 9 | `middleInitial` | STRING | 834 NM105 | Enrollee middle initial. |
| 10 | `enrolleeUniqueID` | STRING | 834 REF*6O | Cross-file master enrollee identifier. |
| 11 | `dateofBirth` | DATE | 834 DMG02 (`yyyy-MM-dd`) | Enrollee legal date of birth. |
| 12 | `deceasedDate` | STRING | 834 DMG05 | Date of death for mortality reporting. |
| 13 | `gender` | STRING | 834 DMG03 (`M`/`F`/`U`) | Administrative gender code. |
| 14 | `permanentAddressLine1` | STRING | 834 N301 | Primary legal residence street address line 1. |
| 15 | `permanentAddressLine2` | STRING | 834 N302 | Primary legal residence suite/apartment line 2. |
| 16 | `permanentCity` | STRING | 834 N401 | Legal residence city name. |
| 17 | `permanentCounty` | STRING | 834 N406 | Legal residence county code. |
| 18 | `permanentState` | STRING | 834 N402 | Legal residence 2-letter state code. |
| 19 | `permanentZipCode` | STRING | 834 N403 | Legal residence 5-digit zip code. |
| 20 | `mailingAddressLine1` | STRING | 834 Mailing N301 | Secondary mailing street address line 1. |
| 21 | `mailingAddressLine2` | STRING | 834 Mailing N302 | Secondary mailing suite line 2. |
| 22 | `mailingCity` | STRING | 834 Mailing N401 | Secondary mailing city name. |
| 23 | `mailingState` | STRING | 834 Mailing N402 | Secondary mailing state code. |
| 24 | `mailingZipCode` | STRING | 834 Mailing N403 | Secondary mailing zip code. |
| 25 | `mailingCounty` | STRING | 834 Mailing N406 | Secondary mailing county code. |
| 26 | `phoneNumber` | STRING | 834 PER04 | Enrollee contact phone number. |
| 27 | `email` | STRING | 834 PER08 | Enrollee email address. |
| 28 | `medicaidID` | STRING | 834 REF*3H | State Medicaid program identifier. |
| 29 | `fax` | STRING | 834 PER Fax | Enrollee fax number. |
| 30 | `raceCode` | STRING | 834 DMG Race | Race classification code for health equity. |
| 31 | `raceDataSource` | STRING | 834 Race Source | Originating data source for race code. |
| 32 | `caretakerFirstName` | STRING | 834 NM1 Caretaker | Legal guardian first name. |
| 33 | `caretakerLastName` | STRING | 834 NM1 Caretaker | Legal guardian last name. |
| 34 | `caretakerMiddleInitial`| STRING | 834 NM1 Caretaker | Legal guardian middle initial. |
| 35 | `ethnicityCode` | STRING | 834 DMG Ethnicity | Hispanic/Latino ethnicity code. |
| 36 | `ethnicityDatasource` | STRING | 834 Ethnicity Source | Originating data source for ethnicity. |
| 37 | `spokenLanguage` | STRING | 834 LUI Code | Primary spoken language. |
| 38 | `spokenLanguagesourcecode`| STRING | 834 LUI Source | Spoken language source code. |
| 39 | `writtenLanguageCode` | STRING | 834 LUI Code | Primary written language. |
| 40 | `writtenLanguageSourcecode`| STRING | 834 LUI Source | Written language source code. |
| 41 | `otherLanguage` | STRING | 834 LUI Secondary | Secondary spoken language. |
| 42 | `otherLanguageSourcecode`| STRING | 834 LUI Source | Secondary language source code. |
| 43 | `isUSCitizen` | STRING | 834 DMG Citizenship | U.S. citizenship indicator flag (`Y`/`N`). |
| 44-53 | `alternateKey1`..`10` | STRING | 834 REF Custom | 10 custom slots for client-specific keys. |
| 54 | `maskedMemberID` | STRING | SHA-256 Hash | De-identified member key for research datasets. |
| 55 | `enrolleeEducation` | STRING | 834 DMG Education | Enrollee education level (SES attribute). |
| 56 | `enrolleeEmployment` | STRING | 834 DMG Employment | Enrollee employment status (SES attribute). |
| 57 | `effectiveStartDate` | DATE | SCD Type 2 Window | Effective validity start date. |
| 58 | `effectiveEndDate` | DATE | SCD Type 2 Window | Effective validity end date (`9999-12-31`). |
| 59 | `isCurrent` | BOOLEAN | SCD Type 2 Active | Active record flag (`True` = Current Active). |
| 60 | `ProductID` | STRING | 834 HD04 Product | Health plan benefit product code. |
| 61 | `LoadDateTime` | TIMESTAMP | Pipeline Ingestion | Timestamp when row was inserted into Gold. |

---

### 4.2 Member Group Dimension (`gold_ma_membergroup` — 11 Columns)

* **Data Flow**: 834 Subgroup Loops $\rightarrow$ `silver_member_group` $\rightarrow$ `gold_ma_membergroup`.

| # | Column Name | Data Type | Business Description |
| :--- | :--- | :--- | :--- |
| 1 | `memberGroupKey` | STRING | Primary surrogate key for enrollment subgroup contract. |
| 2 | `SubscriberID` | STRING | Primary policy subscriber identifier. |
| 3 | `BeneficiaryID` | STRING | CMS Medicare Beneficiary Identifier (MBI). |
| 4 | `CMSContractNumber` | STRING | CMS Medicare Advantage Contract ID (e.g., `H1234`). |
| 5 | `GroupNumber` | STRING | Employer group contract policy number. |
| 6 | `GroupSuffix` | STRING | Subgroup division / class suffix code. |
| 7 | `StartDate` | DATE | Benefit coverage effective start date. |
| 8 | `EndDate` | DATE | Benefit coverage effective end date. |
| 9 | `SourceFileID` | BIGINT | Audit key linking to raw ingestion file (`FilesToProcess`). |
| 10 | `LoadDateTime` | TIMESTAMP | Timestamp when row was inserted into Gold. |
| 11 | `hashKey` | STRING | SHA-256 hash key for change tracking. |

---

### 4.3 Provider Dimension (`gold_dimprovider` — 57 Columns)

* **Data Flow**: 837 Professional/Inst Claims & Provider Rosters $\rightarrow$ `ProviderHierarchy.py` $\rightarrow$ `silver_provider_hierarchy` $\rightarrow$ `gold_dimprovider`.

| # | Column Name | Data Type | Business Description |
| :--- | :--- | :--- | :--- |
| 1 | `providerKey` | INT | Surrogate primary key linking provider to fact table (Defaults `-99`). |
| 2 | `providerID` | STRING | Health plan internal provider ID (`PRV1001`). |
| 3 | `effectiveStartDate` | DATE | SCD Type 2 validity start date. |
| 4 | `effectiveEndDate` | DATE | SCD Type 2 validity end date (`9999-12-31`). |
| 5 | `isCurrent` | INT | SCD Type 2 active flag (`1` = Current Active). |
| 6 | `npi` | STRING | 10-digit National Provider Identifier. |
| 7 | `tin` | STRING | Taxpayer Identification Number (Tax ID). |
| 8-10 | `lastName`, `firstName`, `middleName` | STRING | Practitioner legal names. |
| 11-16 | `phoneNumber`, `address1`, `address2`, `city`, `state`, `zipCode` | STRING | Primary practice location contact & street address. |
| 17-18 | `practiceCode`, `practiceName` | STRING | Medical group practice identifier & commercial name. |
| 19-20 | `providerOrgCode`, `providerOrgName` | STRING | Parent health system network code & organization name. |
| 21 | `providerSpecialtyDescription` | STRING | Primary clinical specialty description (e.g. "Internal Medicine"). |
| 22-36 | `taxonomyCode1`..`5`, `hpSpecialtyCode1`..`5`, `advProviderSpecialtyCode1`..`5` | STRING | Primary, secondary, tertiary NUCC taxonomies & health plan specialty codes. |
| 37-38 | `isPrescribePrivilege`, `providerDEA` | STRING | Prescribing authority flag & Federal DEA registration number. |
| 39-40 | `payerID`, `isContracted` | STRING | Payer organization ID & network contracted status flag (`Y`/`N`). |
| 41-42 | `providerHAI`, `hospitalID` | STRING | Hospital admitting privileges ID & primary hospital ID. |
| 43 | `isExcludedFromProviderReporting` | STRING | Flag to exclude non-PCP providers from gap attribution. |
| 44-53 | `altProvReporting1`..`10` | STRING | 10 custom slots for client provider reporting metrics. |
| 54-55 | `programType`, `practiceTargetedStatus` | STRING | PCP engagement tier status (e.g., "Targeted PCP"). |
| 56-57 | `ProductID`, `ProviderType` | STRING | Health plan product ID & provider type (`Individual`/`Group`). |

---

### 4.4 Client Dimension (`gold_dimclient` — 5 Columns)

* **Data Flow**: `ref_client_metadata.csv` $\rightarrow$ `silver_client_metadata` $\rightarrow` `gold_dimclient`.

| # | Column Name | Data Type | Business Description |
| :--- | :--- | :--- | :--- |
| 1 | `clientKey` | BIGINT | Surrogate primary key linking client organization to fact table (Defaults `-99`). |
| 2 | `clientCode` | STRING | Unique health plan client business code (e.g., `CITY834N`). |
| 3 | `clientName` | STRING | Full legal health plan client organization name. |
| 4 | `subClientCode` | STRING | Secondary client division / regional operating unit code. |
| 5 | `subClientName` | STRING | Secondary client division / regional operating unit name. |

---

### 4.5 CMS-HCC Dimension (`gold_dimhcc` — 11 Columns)

* **Data Flow**: CMS Model Software & ICD-10 to HCC Crosswalks $\rightarrow$ `silver_hcc` $\rightarrow$ `gold_dimhcc`.

| # | Column Name | Data Type | Business Description |
| :--- | :--- | :--- | :--- |
| 1 | `hccKey` | INT | Surrogate primary key linking condition category to fact table. |
| 2 | `HCCNumber` | STRING | CMS Condition Category number (e.g., `HCC019`). |
| 3 | `HCCDescription` | STRING | Official CMS category description ("Diabetes with Chronic Complications"). |
| 4 | `HCCVersion` | STRING | CMS risk adjustment model version (`V24`, `V28`). |
| 5 | `HCCType` | STRING | Model community type (`COMM` = Community, `ESRD`, `RX`). |
| 6 | `IsChronic` | BOOLEAN | Flag indicating if condition requires annual recertification. |
| 7 | `EffectiveYear` | INT | Applicable CMS risk adjustment program year (e.g., 2026). |
| 8 | `EffectiveDateStart` | DATE | Release validity start date of the CMS model. |
| 9 | `EffectiveDateEnd` | DATE | Release validity end date of the CMS model. |
| 10 | `hashKey` | INT | Integer hash key for quick schema reconciliation. |
| 11 | `RiskWeight` | DOUBLE | Actuarial risk weight coefficient used for RAF calculation. |

---

### 4.6 Alert Group Dimension (`gold_dimalertgroup` — 6 Columns)

* **Data Flow**: CDI Clinical Rules Engine Classifier $\rightarrow$ `silver_alertgroup` $\rightarrow$ `gold_dimalertgroup`.

| # | Column Name | Data Type | Business Description |
| :--- | :--- | :--- | :--- |
| 1 | `alertGroupKey` | INT | Primary surrogate key linking alert category to fact table. |
| 2 | `alertGroupCode` | STRING | CDI alert classification code (e.g., `DIABETES`). |
| 3 | `alertGroupDescription` | STRING | Full clinical description of the alert category. |
| 4 | `displayText` | STRING | Text prompt rendered to physicians during EHR point-of-care alerts. |
| 5 | `sortOrder` | INT | Priority order index for displaying alerts in EHR dashboards. |
| 6 | `isActive` | BOOLEAN | Indicator flag confirming if alert category rule is currently active. |

---

### 4.7 Daily Date Dimension (`gold_dimdate` — 24 Columns)

* **Data Flow**: PySpark Daily Sequence Explosion (`1900-01-01` to `9999-12-31`) $\rightarrow$ `gold_dimdate`.

| # | Column Name | Data Type | Business Description |
| :--- | :--- | :--- | :--- |
| 1 | `dateKey` | INT | Integer primary key formatted as `yyyyMMdd` (e.g., `20260729`). |
| 2 | `date` | DATE | Standard PySpark DateType (`2026-07-29`). |
| 3 | `shortDateName` | STRING | Abbreviated formatted date string ("Jul 29, 2026"). |
| 4 | `longDateName` | STRING | Full long formatted date string ("July 29, 2026"). |
| 5-6 | `yearNumber`, `yearName` | INT / STR | Numeric calendar year (`2026`) and text year ("2026"). |
| 7-12 | `quarterKey`, `quarterNumber`, `quarterName`, `quarterOfYearNumber`, `quarterOfYearName` | INT / STR | Integer quarter key (`20263`), quarter index (`3`), & quarter text ("Q3"). |
| 13-18 | `monthKey`, `monthNumber`, `monthName`, `monthOfQuarterNumber`, `monthOfQuarterName`, `monthOfYearShortName` | INT / STR | Integer month key (`202607`), month index (`7`), & name ("July"). |
| 19-21 | `weekKey`, `weekNumber`, `weekName` | INT / STR | Integer week key (`202630`), week index (`30`), & week display name. |
| 22-24 | `dayOfWeekNumber`, `dayOfWeekName`, `dayOfYear`, `isWorkDay` | INT / STR / BOOL | Day index (1=Sun), day name ("Wednesday"), & workday flag (`True` for Mon-Fri). |

---

### 4.8 Monthly Period Dimension (`gold_dimmonth` — 7 Columns)

* **Data Flow**: PySpark Monthly Interval Sequence Explosion (`yyyyMM`) $\rightarrow$ `gold_dimmonth`.

| # | Column Name | Data Type | Business Description |
| :--- | :--- | :--- | :--- |
| 1 | `monthKey` | INT | Integer primary key formatted as `yyyyMM` (e.g., `202607`). |
| 2 | `monthNumber` | INT | Numeric month index of calendar year (1 through 12). |
| 3 | `monthName` | STRING | Full month display name ("July"). |
| 4 | `yearNumber` | INT | Numeric calendar year (`2026`). |
| 5 | `yearName` | STRING | Text calendar year name ("2026"). |
| 6 | `quarterNumber` | INT | Numeric quarter index of calendar year (1 through 4). |
| 7 | `quarterName` | STRING | Quarter display text ("Q3"). |

---

## 5. Silver Risk Adjustment Revenue Gap Engine Mechanics

The Revenue Gap Calculation Engine (`factrevenugap/Silver/Notebooks/MemberRevenueGaps.ipynb`) is the heart of the risk adjustment platform. It correlates suspected clinical condition alerts against CMS encounter return files to derive gap status.

```text
       📥 CDI Suspected Alerts              📥 RAPS Return Data              📥 MAO-004 Encounters
       (BasePrintedDiags)                    (HCCRaps)                        (MAO004Diagnosis)
               │                                 │                                    │
               └────────────────────────┬────────┴────────────────────────────────────┘
                                        │
                                        ▼
                   ┌──────────────────────────────────────────┐
                   │    🔍 Member & Condition Matching Engine │
                   │  Matches on:                             │
                   │  - Member Identifier (planMemberID)      │
                   │  - CMS Condition Category (hccNumber)    │
                   │  - CMS Model Version (HCCVersion V28)    │
                   │  - Service Year / Month (reportMonth)    │
                   └────────────────────┬─────────────────────┘
                                        │
                                        ▼
                   ┌──────────────────────────────────────────┐
                   │      ⚡ Gap Closure Decision Tree         │
                   ├──────────────────────────────────────────┤
                   │ 1. Found in RAPS return?                 │
                   │    ➔ "Closed by RAPS Claim"              │
                   │ 2. Found in MAO-004 return?              │
                   │    ➔ "Closed by MAO-004 Encounter"       │
                   │ 3. Physician confirmed in chart?         │
                   │    ➔ "Closed by Provider Confirmation"   │
                   │ 4. Physician rejected in chart?          │
                   │    ➔ "Closed by Provider Rejection"      │
                   │ 5. None of the above?                    │
                   │    ➔ "Open Gap" (Requires Outreach)      │
                   └──────────────────────────────────────────┘
```

### 5.1 Suspected CDI Alert Ingestion
Suspected chronic conditions are identified by analyzing historical claims, lab results, prescription history, and prior-year diagnostic data. These are ingested into `BasePrintedDiags` with `alertCategory`, `hccNumber`, `HCCVersion`, and target `reportMonth`.

### 5.2 RAPS & MAO-004 CMS Encounter Matching Algorithm
The engine joins `BasePrintedDiags` against CMS return files on:
1. `planMemberID` == `HCCRaps.planMemberID` (or `MAO004.planMemberID`)
2. `hccNumber` == `HCCRaps.hccNumber`
3. `HCCVersion` == `HCCRaps.HCCVersion` (e.g. `V28`)
4. `reportMonth` == `yyyyMM` of Service Start Date

### 5.3 Gap Closure Decision Tree & Rule Engine
When a suspected alert is processed, the engine evaluates rules in strict order:
1. **Rule 1 (RAPS Accept)**: If a matching diagnosis is present in accepted RAPS return data (`HCCRaps`), `closureReason` = `'Closed by RAPS Claim'` and `isHCCClosed` = `'Y'`.
2. **Rule 2 (MAO-004 Accept)**: If present in accepted MAO-004 encounter data (`MAO004Diagnosis`), `closureReason` = `'Closed by MAO-004 Encounter'` and `isHCCClosed` = `'Y'`.
3. **Rule 3 (Provider Chart Confirmation)**: If confirmed by physician during Annual Wellness Visit (AWV), `closureReason` = `'Closed by Provider Confirmation'` and `isHCCClosed` = `'Y'`.
4. **Rule 4 (Provider Rejection)**: If rejected by physician during chart review, `closureReason` = `'Closed by Provider Rejection'` and `isHCCClosed` = `'N'`.
5. **Rule 5 (Default Open Gap)**: If unaddressed, `closureReason` is `NULL` and `isHCCClosed` = `'N'`.

Output is stored in `claimsprocessing.silver.silver_member_revenue_gap`.

---

## 6. Gold Fact Table Engine (`factmemberrevenuegap`) Mechanics

### 6.1 Fact Table Column Specifications (16 Columns)

| # | Column Name | Data Type | Source Join / Derivation | Business Description |
| :--- | :--- | :--- | :--- | :--- |
| 1 | `pecYearMonthKey` | INT | `ifnull(dimMonth.monthKey, -99)` | Financial period year-month key (e.g., `202601`). |
| 2 | `clientKey` | INT | `ifnull(dimClient.clientKey, -99)` | Foreign key linking gap to health plan client. |
| 3 | `memberKey` | BIGINT | `ifnull(dimMember.memberKey, -99)` | Foreign key linking gap to enrollee persona. |
| 4 | `memberGroupKey` | STRING | `ifnull(dimMemberGroup.memberGroupKey, '-99')` | Foreign key linking gap to enrollment group contract. |
| 5 | `planID` | STRING | `ifnull(mrg.planID, '')` | Health plan benefit package ID (`PLAN001`). |
| 6 | `hccKey` | INT | `ifnull(dimHCC.hccKey, -99)` | Foreign key to CMS-HCC model category & risk weight. |
| 7 | `snapshotDateKey` | INT | `ifnull(dimDate1.dateKey, -99)` | Date key (`yyyyMMdd`) when gap snapshot was taken. |
| 8 | `planProviderKey` | INT | `ifnull(dimProvider.providerKey, -99)` | Foreign key to member's assigned PCP provider. |
| 9 | `alertGroupKey` | INT | `ifnull(dimAlertGroup.alertGroupKey, -99)` | Foreign key to clinical alert classification (`DIABETES`). |
| 10 | `isHCCClosed` | STRING | `case when mrg.closureReason is not null then 'Y' else 'N' end` | **Primary KPI Metric**: **'Y'** = Closed, **'N'** = Open Gap. |
| 11 | `lastDCConfirmedDateKey` | INT | `ifnull(dimDate2.dateKey, -99)` | Date key when provider confirmed diagnosis. |
| 12 | `lastPCPVisitDateKey` | INT | `ifnull(dimDate3.dateKey, -99)` | Date key of member's last PCP encounter. |
| 13 | `lastAWVDateKey` | INT | `ifnull(dimDate4.dateKey, -99)` | Date key of member's Annual Wellness Visit. |
| 14 | `factMemberRevenueGapHashKey` | STRING | `sha2(concat_ws('\|', pecYearMonthKey, memberKey, hccKey, clientKey), 256)` | Unique business entity hash key. |
| 15 | `fullRowHash` | STRING | `sha2(concat_ws('\|', isHCCClosed, planID, snapshotDateKey, ...), 256)` | Attribute change hash key for MERGE updates. |
| 16 | `loadDateKey` | INT | `ifnull(dim.dateKey, -99)` | Calendar date key (`yyyyMMdd`) when record was loaded into Gold. |

---

### 6.2 Star Schema Relational Join Engine (`factmemberrevenuegap_processing.sql`)

The relational transformation engine executes Left Outer Joins against all 8 conformed dimensions:

```sql
WITH sourceTbl AS
(
SELECT
  ifnull(dimMonth.monthKey, -99) as pecYearMonthKey
 ,ifnull(dimClient.clientKey, -99) as clientKey
 ,ifnull(dimMember.memberKey, -99) as memberKey
 ,ifnull(dimMemberGroup.memberGroupKey, '-99') as memberGroupKey
 ,ifnull(mrg.planID, '') as planID
 ,ifnull(dimHCC.hccKey, -99) as hccKey
 ,ifnull(dimDate1.dateKey, -99) as snapshotDateKey
 ,ifnull(dimProvider.providerKey, -99) as planProviderKey
 ,ifnull(dimAlertGroup.alertGroupKey, -99) as alertGroupKey
 ,case when mrg.closureReason is not null then 'Y' else 'N' end as isHCCClosed
 ,ifnull(dimDate2.dateKey, -99) as lastDCConfirmedDateKey
 ,ifnull(dimDate3.dateKey, -99) as lastPCPVisitDateKey
 ,ifnull(dimDate4.dateKey, -99) as lastAWVDateKey
 ,current_date()  as loadDate
from memberRevenueGap mrg
left join dimMonth
 on mrg.reportMonth = concat(dimMonth.yearNumber,lpad(dimMonth.monthNumber,2,'0'))
left join dimClient
 on upper(mrg.clientCode) = upper(dimClient.clientCode)
left join dimMember
 on mrg.planMemberID = dimMember.planMemberID
 and CAST(dimMember.isCurrent AS INT) = 1
left join dimMemberGroup
 on dimMember.subscriberID = dimMemberGroup.SubscriberID
left join dimHCC
 on mrg.hccNumber = dimHCC.HCCNumber
 and substring(mrg.reportMonth, 1,4) = dimHCC.EffectiveYear
 and mrg.HCCVersion = dimHCC.HCCVersion
 and upper(dimHCC.HCCType) in ('COMM', 'ESRD', 'RX')
left join dimProvider
 on mrg.providerID = dimProvider.providerID
 and CAST(dimProvider.isCurrent AS INT) = 1
left join dimAlertGroup
 on mrg.alertCategory = dimAlertGroup.alertGroupCode
left join dimDate dimDate1
 on mrg.snapshotDate = dimDate1.date
left join dimDate dimDate2
 on mrg.lastDCConfirmedDate = dimDate2.date
left join dimDate dimDate3
 on mrg.lastPCPVisitDate = dimDate3.date
left join dimDate dimDate4
 on mrg.lastAWVDate = dimDate4.date
)
SELECT 
 pecYearMonthKey, clientKey, memberKey, memberGroupKey, planID, hccKey,
 snapshotDateKey, planProviderKey, alertGroupKey, isHCCClosed,
 lastDCConfirmedDateKey, lastPCPVisitDateKey, lastAWVDateKey,
 sha2(concat_ws('|', pecYearMonthKey, memberKey, hccKey, clientKey), 256) as factMemberRevenueGapHashKey,
 sha2(concat_ws('|', isHCCClosed, planID, snapshotDateKey, planProviderKey, alertGroupKey, lastDCConfirmedDateKey, lastPCPVisitDateKey, lastAWVDateKey), 256) as fullRowHash,
 cast(date_format(loadDate, 'yyyyMMdd') as int) as loadDateKey
FROM sourceTbl;
```

---

### 6.3 Delta Lake Atomic MERGE Strategy (`factmemberrevenuegap_update.sql`)

To maintain ACID integrity and prevent duplicate records during incremental execution, the pipeline executes an atomic `MERGE INTO`:

```sql
MERGE INTO claimsprocessing.gold.factmemberrevenuegap AS target
USING temp_sourceTbl AS source
ON target.factMemberRevenueGapHashKey = source.factMemberRevenueGapHashKey
WHEN MATCHED AND target.fullRowHash <> source.fullRowHash THEN
  UPDATE SET
    target.pecYearMonthKey = source.pecYearMonthKey,
    target.clientKey = source.clientKey,
    target.memberKey = source.memberKey,
    target.memberGroupKey = source.memberGroupKey,
    target.planID = source.planID,
    target.hccKey = source.hccKey,
    target.snapshotDateKey = source.snapshotDateKey,
    target.planProviderKey = source.planProviderKey,
    target.alertGroupKey = source.alertGroupKey,
    target.isHCCClosed = source.isHCCClosed,
    target.lastDCConfirmedDateKey = source.lastDCConfirmedDateKey,
    target.lastPCPVisitDateKey = source.lastPCPVisitDateKey,
    target.lastAWVDateKey = source.lastAWVDateKey,
    target.fullRowHash = source.fullRowHash,
    target.loadDateKey = source.loadDateKey
WHEN NOT MATCHED THEN
  INSERT (
    pecYearMonthKey, clientKey, memberKey, memberGroupKey, planID, hccKey,
    snapshotDateKey, planProviderKey, alertGroupKey, isHCCClosed,
    lastDCConfirmedDateKey, lastPCPVisitDateKey, lastAWVDateKey,
    factMemberRevenueGapHashKey, fullRowHash, loadDateKey
  )
  VALUES (
    source.pecYearMonthKey, source.clientKey, source.memberKey, source.memberGroupKey, source.planID, source.hccKey,
    source.snapshotDateKey, source.planProviderKey, source.alertGroupKey, source.isHCCClosed,
    source.lastDCConfirmedDateKey, source.lastPCPVisitDateKey, source.lastAWVDateKey,
    source.factMemberRevenueGapHashKey, source.fullRowHash, source.loadDateKey
  );
```

---

## 7. Production Deployment & Operational Workflow Guide

### Workflow Execution Order in Databricks:
1. **Task 1 (`FilesToProcess`)**: Ingest raw 834/837/RAPS files and populate Bronze volumes.
2. **Task 2 (`MemberPersonBridge` & `ProviderHierarchy`)**: Run Silver entity resolution pipelines.
3. **Task 3 (`MemberRevenueGaps`)**: Execute Silver Revenue Gap matching engine.
4. **Task 4 (`Dimensions`)**: Refresh Gold conformed dimensions (`gold_dimmember`, `gold_dimprovider`, `gold_dimclient`, `gold_dimhcc`, `gold_dimalertgroup`, `gold_dimdate`, `gold_dimmonth`).
5. **Task 5 (`factmemberrevenuegap_pipeline`)**: Run `factmemberrevenuegap_pipeline.py` to populate Gold Fact table.

### Local $\rightarrow$ GitHub $\rightarrow$ Databricks Synchronization:
```bash
# 1. Stage updated README
git add README.md

# 2. Commit documentation release
git commit -m "docs: master enterprise documentation for Healthcare Risk Adjustment Data Warehouse"

# 3. Push to GitHub
git push origin master

# 4. In Databricks Workspace Repos UI: Click 'Git Pull' to pull updated README.md
```

---
*© 2026 Healthcare Data Engineering & Risk Analytics Practice. All Rights Reserved.*
