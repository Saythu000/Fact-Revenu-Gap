"""
Comprehensive member mapping - CORRECTED PATHS based on actual structured_json
Verified against EDI parser output structure
"""

MAPPINGS = {
    "name": "Member Comprehensive Schema Mapping",
    "mapping_type": "only_mapped",
    
    "expressions": {
        # ============================================================
        # CORE IDENTIFIERS
        # ============================================================
        "UNIQUEPERSONKEY": "detail.transaction_set_header_loop.transaction_set_header_NM1[0].insured_id",
        "BENEFICIARYID": "detail.transaction_set_header_loop.transaction_set_header_NM1[0].insured_id",
        "MEDICAIDID": "detail.transaction_set_header_loop.transaction_set_header_REF[0].subluxation_documentation",
        "PLANMEMBERID": "detail.transaction_set_header_loop.transaction_set_header_REF[1].group_number",
        "SUBSCRIBERID": "detail.transaction_set_header_loop.transaction_set_header_REF[2].reference_value",
        "ENROLLEEUNIQUEID": "detail.transaction_set_header_loop.transaction_set_header_REF[3].reference_value",
        "MASKEDMEMBERID": "detail.transaction_set_header_loop.transaction_set_header_REF[4].reference_value",
        
        # Alternate Keys (REF[5] through REF[14])
        "ALTERNATEKEY1": "detail.transaction_set_header_loop.transaction_set_header_REF[5].reference_value",
        "ALTERNATEKEY2": "detail.transaction_set_header_loop.transaction_set_header_REF[6].reference_value",
        "ALTERNATEKEY3": "detail.transaction_set_header_loop.transaction_set_header_REF[7].reference_value",
        "ALTERNATEKEY4": "detail.transaction_set_header_loop.transaction_set_header_REF[8].reference_value",
        "ALTERNATEKEY5": "detail.transaction_set_header_loop.transaction_set_header_REF[9].reference_value",
        "ALTERNATEKEY6": "detail.transaction_set_header_loop.transaction_set_header_REF[10].reference_value",
        "ALTERNATEKEY7": "detail.transaction_set_header_loop.transaction_set_header_REF[11].reference_value",
        "ALTERNATEKEY8": "detail.transaction_set_header_loop.transaction_set_header_REF[12].reference_value",
        "ALTERNATEKEY9": "detail.transaction_set_header_loop.transaction_set_header_REF[13].reference_value",
        "ALTERNATEKEY10": "detail.transaction_set_header_loop.transaction_set_header_REF[14].reference_value",
        
        # Education
        "ENROLLEEEDUCATION": "detail.transaction_set_header_loop.transaction_set_header_REF[15].reference_value",
        
        # ============================================================
        # MEMBER NAME (NM1[0] - Member)
        # ============================================================
        "LASTNAME": "detail.transaction_set_header_loop.transaction_set_header_NM1[0].insured_last_name",
        "FIRSTNAME": "detail.transaction_set_header_loop.transaction_set_header_NM1[0].insured_first_name",
        "MIDDLEINITIAL": "detail.transaction_set_header_loop.transaction_set_header_NM1[0].insured_middle_name",
        "TITLE": "detail.transaction_set_header_loop.transaction_set_header_NM1[0].insured_prefix",
        "SUFFIX": "detail.transaction_set_header_loop.transaction_set_header_NM1[0].insured_suffix",
        
        # ============================================================
        # DEMOGRAPHICS (DMG[0])
        # ============================================================
        "DATEOFBIRTH": "detail.transaction_set_header_loop.transaction_set_header_DMG[0].prior_incorrect_insured_birth_date",
        "GENDER": "detail.transaction_set_header_loop.transaction_set_header_DMG[0].prior_incorrect_insured_gender_code",
        
        # Race and Ethnicity
        "RACECODE": "detail.transaction_set_header_loop.transaction_set_header_DMG[0].dmg05_05",
        "RACEDATASOURCE": "detail.transaction_set_header_loop.transaction_set_header_DMG[0].citizenship_status_code",
        "ETHNICITYCODE": "detail.transaction_set_header_loop.transaction_set_header_DMG[0].basis_of_verification_code",
        "ETHNICITYDATASOURCE": "detail.transaction_set_header_loop.transaction_set_header_DMG[0].quantity_09",
        
        # Citizenship
        "USCITIZEN": "detail.transaction_set_header_loop.transaction_set_header_DMG[0].citizenship_status_code",
        
        # Deceased Date (from DTP with qualifier 360, not in current data)
        "DECEASEDDATE": "detail.transaction_set_header_loop.transaction_set_header_DTP[0].date_value",
        
        # ============================================================
        # PERMANENT ADDRESS (N3[0], N4[0])
        # ============================================================
        "PERMANENTADDRESSLINE1": "detail.transaction_set_header_loop.transaction_set_header_N3[0].address_line_1",
        "PERMANENTADDRESSLINE2": "detail.transaction_set_header_loop.transaction_set_header_N3[0].address_line_2",
        "PERMANENTCITY": "detail.transaction_set_header_loop.transaction_set_header_N4[0].city",
        "PERMANENTSTATE": "detail.transaction_set_header_loop.transaction_set_header_N4[0].state",
        "PERMANENTZIPCODE": "detail.transaction_set_header_loop.transaction_set_header_N4[0].zip_code",
        "PERMANENTCOUNTY": "detail.transaction_set_header_loop.transaction_set_header_N4[0].location_identifier",
        
        # ============================================================
        # MAILING ADDRESS (N3[1], N4[1])
        # ============================================================
        "MAILINGADDRESSLINE1": "detail.transaction_set_header_loop.transaction_set_header_N3[1].address_line_1",
        "MAILINGADDRESSLINE2": "detail.transaction_set_header_loop.transaction_set_header_N3[1].address_line_2",
        "MAILINGCITY": "detail.transaction_set_header_loop.transaction_set_header_N4[1].city",
        "MAILINGSTATE": "detail.transaction_set_header_loop.transaction_set_header_N4[1].state",
        "MAILINGZIPCODE": "detail.transaction_set_header_loop.transaction_set_header_N4[1].zip_code",
        "MAILINGCOUNTY": "detail.transaction_set_header_loop.transaction_set_header_N4[1].location_identifier",
        
        # ============================================================
        # CONTACT INFORMATION (PER[0])
        # ============================================================
        "PHONENUMBER": "detail.transaction_set_header_loop.transaction_set_header_PER[0].communication_number_04",
        "EMAIL": "detail.transaction_set_header_loop.transaction_set_header_PER[0].communication_number_06",
        "FAX": "detail.transaction_set_header_loop.transaction_set_header_PER[0].communication_number_08",
        
        # ============================================================
        # LANGUAGE INFORMATION (LUI array)
        # LUI[0] = First language (Spanish, spoken)
        # LUI[1] = Second language (English, spoken)
        # LUI[2] = Written language (English, written)
        # ============================================================
        "SPOKENLANGUAGE": "detail.transaction_set_header_loop.transaction_set_header_LUI[0].language_code",
        "SPOKENLANGUAGESOURCECODE": "detail.transaction_set_header_loop.transaction_set_header_LUI[0].language_use_indicator_04",
        
        "WRITTENLANGUAGECODE": "detail.transaction_set_header_loop.transaction_set_header_LUI[2].language_code",
        "WRITTENLANGUAGESOURCECODE": "detail.transaction_set_header_loop.transaction_set_header_LUI[2].language_use_indicator_04",
        
        "OTHERLANGUAGE": "detail.transaction_set_header_loop.transaction_set_header_LUI[1].language_code",
        "OTHERLANGUAGESOURCECODE": "detail.transaction_set_header_loop.transaction_set_header_LUI[1].language_use_indicator_04",
        
        # ============================================================
        # INSURANCE INFORMATION (INS segment)
        # ============================================================
        "SUBSCRIBERFLAG": "detail.transaction_set_header_loop.transaction_set_header_INS.member_indicator",
        "RELATIONSHIPCODE": "detail.transaction_set_header_loop.transaction_set_header_INS.individual_relationship_code",
        "MAINTENANCETYPECODE": "detail.transaction_set_header_loop.transaction_set_header_INS.maintenance_type_code",
        "MAINTENANCEREASONCODE": "detail.transaction_set_header_loop.transaction_set_header_INS.maintenance_reason_code",
        "BENEFITSTATUSCODE": "detail.transaction_set_header_loop.transaction_set_header_INS.benefit_status_code",
        "EMPLOYMENTSTATUSCODE": "detail.transaction_set_header_loop.transaction_set_header_INS.employment_status_code",
        
        # ============================================================
        # COVERAGE DATES (DTP array - by qualifier)
        # DTP[0] = 303 (File date)
        # DTP[1] = 336 (DOB)
        # DTP[2] = 348 (Eligibility start)
        # DTP[3] = 349 (Eligibility end)
        # ============================================================
        "ELIGIBILITYSTARTDATE": "detail.transaction_set_header_loop.transaction_set_header_DTP[2].date_value",
        "ELIGIBILITYENDDATE": "detail.transaction_set_header_loop.transaction_set_header_DTP[3].date_value",
        
        # ============================================================
        # HEALTH COVERAGE (HD array)
        # HD[0] = HLT coverage
        # HD[1] = DEN coverage
        # ============================================================
        "PRODUCTID": "detail.transaction_set_header_loop.transaction_set_header_HD[0].plan_coverage_description_04",
        
        # ============================================================
        # EMPLOYMENT INFORMATION (EC segment)
        # ============================================================
        "ENROLLEEEMPLOYMENT": "detail.transaction_set_header_loop.transaction_set_header_EC.employment_class_code",
        
        # ============================================================
        # CARETAKER/GUARDIAN (NM1[1])
        # ============================================================
        "CARETAKERFIRSTNAME": "detail.transaction_set_header_loop.transaction_set_header_NM1[1].first_name",
        "CARETAKERLASTNAME": "detail.transaction_set_header_loop.transaction_set_header_NM1[1].name",
        "CARETAKERMIDDLEINITIAL": "detail.transaction_set_header_loop.transaction_set_header_NM1[1].middle_name",
    }
}
