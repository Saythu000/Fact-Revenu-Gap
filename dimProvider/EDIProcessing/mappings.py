"""
Provider Dimension Unified Mapping Definitions
Maps both raw EDI 274 (Directory/Hierarchy) and raw EDI 837 (Claims/Professional) 
loops to flat CSV structures using a single MAPPINGS definition.
Supports both Solo Practitioner and Multi-Level Organizational Hierarchies.
"""

MAPPINGS = {
    "name": "Unified Provider Dimension Declarative Schema Mapping",
    "mapping_type": "only_mapped",
    "expressions": {
        # =====================================================================
        # 0. COMMON FIELDS
        # =====================================================================
        "TEMPLATE": " 'TEMPLATE' ",

        # =====================================================================
        # 1. EDI 274 HIERARCHY LAYOUT FIELDS (UPPERCASE KEYS)
        # =====================================================================
        "PROVIDERID":       "detail.nm1[entity_identifier_code='1P' and entity_type_qualifier='1'].(identifier ? identifier : nm111_111)",
        "PROVIDERLASTNAME": "detail.nm1[entity_identifier_code='1P' and entity_type_qualifier='1'].name",
        "PROVIDERNPI":      "detail.nm1[entity_identifier_code='1P' and entity_type_qualifier='1'].(identifier ? identifier : nm111_111)",
        
        "LOCATIONGROUPID":  "detail.nm1[entity_identifier_code='41'].submitter_id ? detail.nm1[entity_identifier_code='41'].submitter_id : detail.nm1[entity_identifier_code='85'].(billing_provider_id ? billing_provider_id : identifier)",
        "LOCATIONRANKING":  "detail.nm1 ? '1' : null",
        "LOCATIONIDTYPE":   "detail.nm1[entity_identifier_code='1P' and entity_type_qualifier='1'].entity_identifier_code",
        "LOCATIONID":       "detail.nm1[entity_identifier_code='1P' and entity_type_qualifier='1'].(identifier ? identifier : nm111_111)",
        "LOCATIONDESC":     "detail.nm1[entity_identifier_code='1P' and entity_type_qualifier='1'].(entity_type_qualifier='1' ? ($join([first_name, middle_name, name][$ != null], ' ')) : name)",
        "LOCATIONTIN":      "detail.ref[0].(employer_id ? employer_id : champus_id)",
        
        "LOCATIONADDRESS1": "[detail.n3[address_line_2 != null].address_line_1, (detail.n3[address_line_1 != null].address_line_1)[0], detail.n3[0].billing_provider_address_line_1][$ != null][0]",
        "LOCATIONADDRESS2": "detail.n3[address_line_2 != null].address_line_2",
        "LOCATIONCITY":     "[(detail.n4[city != null].city)[0], detail.n4[0].billing_provider_city][$ != null][0]",
        "LOCATIONSTATE":    "[(detail.n4[city != null].state)[0], detail.n4[0].billing_provider_state][$ != null][0]",
        "LOCATIONZIP":      "[(detail.n4[city != null].zip_code)[0], detail.n4[0].billing_provider_zip_code][$ != null][0]",
        
        "COUNTYCODE":       "(detail.n4[city != null and n406_406 != null].n406_406)[0]",
        "PHONENUMBER":      "[(detail.per[per01='AJ' or per01='IC'].(per03_03='TE' ? per04_04 : (per05_05='TE' ? per06_06 : (per07_07='TE' ? per08_08 : null))))[1], (detail.per[per01='AJ' or per01='IC'].(per03_03='TE' ? per04_04 : (per05_05='TE' ? per06_06 : (per07_07='TE' ? per08_08 : null))))[0]][$ != null][0]",
        "FAXNUMBER":        "[(detail.per[per01='AJ' or per01='IC'].(per03_03='FX' ? per04_04 : (per05_05='FX' ? per06_06 : (per07_07='FX' ? per08_08 : null))))[1], (detail.per[per01='AJ' or per01='IC'].(per03_03='FX' ? per04_04 : (per05_05='FX' ? per06_06 : (per07_07='FX' ? per08_08 : null))))[0]][$ != null][0]",
        "CONTACTPERSON":    "[(detail.per[per01='AJ' or per01='IC'].per02_02)[1], (detail.per[per01='AJ' or per01='IC'].per02_02)[0]][$ != null][0]",
        "DONOTCHASE":        "detail.unmapped",
        
        "TIER2IDTYPE":      "detail.nm1[entity_identifier_code='1P' and entity_type_qualifier='2'].id_qualifier ? detail.nm1[entity_identifier_code='1P' and entity_type_qualifier='2'].id_qualifier : detail.nm1[entity_identifier_code='85'].billing_provider_id_qualifier",
        "TIER2ID":          "detail.nm1[entity_identifier_code='1P' and entity_type_qualifier='2'].identifier ? detail.nm1[entity_identifier_code='1P' and entity_type_qualifier='2'].identifier : detail.nm1[entity_identifier_code='85'].billing_provider_id",
        "TIER2DESC":        "detail.nm1[entity_identifier_code='1P' and entity_type_qualifier='2'].name ? detail.nm1[entity_identifier_code='1P' and entity_type_qualifier='2'].name : detail.nm1[entity_identifier_code='85'].billing_provider_name",
        "TIER2ADDRESS1":    "(detail.n3[address_line_1 != null].address_line_1)[0]",
        "TIER2ADDRESS2":    "(detail.n3[address_line_2 = null and address_line_1 != null].address_line_2)[0]",
        "TIER2CITY":        "(detail.n4[city != null].city)[0]",
        "TIER2STATE":       "(detail.n4[city != null].state)[0]",
        "TIER2ZIP":         "(detail.n4[city != null].zip_code)[0]",
        
        "TIER3IDTYPE":      "detail.unmapped",
        "TIER3ID":          "detail.unmapped",
        "TIER3DESC":        "detail.unmapped",
        "TIER3ADDRESS1":    "detail.unmapped",
        "TIER3ADDRESS2":    "detail.unmapped",
        "TIER3CITY":        "detail.unmapped",
        "TIER3STATE":       "detail.unmapped",
        "TIER3ZIP":         "detail.unmapped",
        
        "TIER4IDTYPE":      "detail.nm1[entity_identifier_code='85'].billing_provider_id_qualifier",
        "TIER4ID":          "detail.nm1[entity_identifier_code='85'].billing_provider_id",
        "TIER4DESC":        "detail.nm1[entity_identifier_code='85'].billing_provider_name",
        "TIER4ADDRESS1":    "detail.n3[billing_provider_address_line_1 != null].billing_provider_address_line_1[0]",
        "TIER4ADDRESS2":    "detail.n3[billing_provider_address_line_2 != null].billing_provider_address_line_2[0]",
        "TIER4CITY":        "detail.n4[billing_provider_city != null].billing_provider_city[0]",
        "TIER4STATE":       "detail.n4[billing_provider_city != null].billing_provider_state[0]",
        "TIER4ZIP":         "detail.n4[billing_provider_city != null].billing_provider_zip_code[0]",
        
        "STARTDATE":        "detail.nm1 ? (detail.dtp[date_qualifier='007'].date_value ? detail.dtp[date_qualifier='007'].date_value : heading.bht.bht04_04) : null",
        "ENDDATE":          "detail.nm1 ? (detail.dtp[date_qualifier='008'].date_value) : null",

        # =====================================================================
        # 2. EDI 837 CLAIM LAYOUT FIELDS (CamelCase KEYS)
        # =====================================================================
        "ProviderID":                "detail.nm1[entity_identifier_code='82'].rendering_provider_id ? detail.nm1[entity_identifier_code='82'].rendering_provider_id : detail.submitter_NM1_loop.transaction_set_header_NM1[entity_identifier_code='82'].rendering_provider_id",
        "LastName":                  "detail.nm1[entity_identifier_code='82'].rendering_provider_last_name ? detail.nm1[entity_identifier_code='82'].rendering_provider_last_name : detail.submitter_NM1_loop.transaction_set_header_NM1[entity_identifier_code='82'].rendering_provider_last_name",
        "FirstName":                 "detail.nm1[entity_identifier_code='82'].rendering_provider_first_name ? detail.nm1[entity_identifier_code='82'].rendering_provider_first_name : detail.submitter_NM1_loop.transaction_set_header_NM1[entity_identifier_code='82'].rendering_provider_first_name",
        "MiddleInitial":             "detail.nm1[entity_identifier_code='82'].rendering_provider_middle_name ? detail.nm1[entity_identifier_code='82'].rendering_provider_middle_name : detail.submitter_NM1_loop.transaction_set_header_NM1[entity_identifier_code='82'].rendering_provider_middle_name",
        
        "TaxonomyCode1":             "detail.prv[0].provider_taxonomy_code ? detail.prv[0].provider_taxonomy_code : (detail.submitter_NM1_loop.transaction_set_header_PRV[provider_code='PE'].provider_taxonomy_code)[0]",
        "HpSpecialtyCode1":          "detail.unmapped",
        "ADVProviderSpecialtyCode1": "detail.unmapped",
        
        "TaxonomyCode2":             "detail.prv[1].provider_taxonomy_code ? detail.prv[1].provider_taxonomy_code : (detail.submitter_NM1_loop.transaction_set_header_PRV[provider_code='PE'].provider_taxonomy_code)[1]",
        "HpSpecialtyCode2":          "detail.unmapped",
        "ADVProviderSpecialtyCode2": "detail.unmapped",
        
        "TaxonomyCode3":             "detail.prv[2].provider_taxonomy_code ? detail.prv[2].provider_taxonomy_code : (detail.submitter_NM1_loop.transaction_set_header_PRV[provider_code='PE'].provider_taxonomy_code)[2]",
        "HpSpecialtyCode3":          "detail.unmapped",
        "ADVProviderSpecialtyCode3": "detail.unmapped",
        
        "TaxonomyCode4":             "detail.prv[3].provider_taxonomy_code ? detail.prv[3].provider_taxonomy_code : (detail.submitter_NM1_loop.transaction_set_header_PRV[provider_code='PE'].provider_taxonomy_code)[3]",
        "HpSpecialtyCode4":          "detail.unmapped",
        "ADVProviderSpecialtyCode4": "detail.unmapped",
        
        "TaxonomyCode5":             "detail.prv[4].provider_taxonomy_code ? detail.prv[4].provider_taxonomy_code : (detail.submitter_NM1_loop.transaction_set_header_PRV[provider_code='PE'].provider_taxonomy_code)[4]",
        "HpSpecialtyCode5":          "detail.unmapped",
        "ADVProviderSpecialtyCode5": "detail.unmapped",
        
        "NPI":                       "detail.nm1[entity_identifier_code='82'].rendering_provider_id ? detail.nm1[entity_identifier_code='82'].rendering_provider_id : detail.submitter_NM1_loop.transaction_set_header_NM1[entity_identifier_code='82'].rendering_provider_id",
        "PrescribePrivilege":        "detail.unmapped",
        "DEA":                       "(detail.ref[subluxation_documentation != null].subluxation_documentation)[0] ? (detail.ref[subluxation_documentation != null].subluxation_documentation)[0] : detail.submitter_NM1_loop.transaction_set_header_REF.subluxation_documentation",
        "PayorID":                   "(detail.ref[reference_qualifier='PQ'].reference_value)[0] ? (detail.ref[reference_qualifier='PQ'].reference_value)[0] : detail.submitter_NM1_loop.transaction_set_header_REF.reference_value",
        "Contracted":                "detail.unmapped",
        "ProviderHAI":               "detail.unmapped",
        "HospitalID":                "detail.unmapped",
        "ExcludeFromProviderReporting": "detail.unmapped",
        
        "AltProvReporting1":         "(detail.ref[blue_cross_provider_id != null].blue_cross_provider_id)[0] ? (detail.ref[blue_cross_provider_id != null].blue_cross_provider_id)[0] : detail.submitter_NM1_loop.transaction_set_header_REF.blue_cross_provider_id",
        "AltProvReporting2":         "(detail.ref[blue_shield_provider_id != null].blue_shield_provider_id)[0] ? (detail.ref[blue_shield_provider_id != null].blue_shield_provider_id)[0] : detail.submitter_NM1_loop.transaction_set_header_REF.blue_shield_provider_id",
        "AltProvReporting3":         "detail.unmapped",
        "AltProvReporting4":         "detail.unmapped",
        "AltProvReporting5":         "detail.unmapped",
        "AltProvReporting6":         "detail.unmapped",
        "AltProvReporting7":         "detail.unmapped",
        "AltProvReporting8":         "detail.unmapped",
        "AltProvReporting9":         "detail.unmapped",
        "AltProvReporting10":        "detail.unmapped"
    }
}
