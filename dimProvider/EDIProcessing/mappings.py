"""
Provider Hierarchy Mapping Definitions — Configured for declarative JSONata mapping.
Maps raw EDI 274 loops directly to a flat CSV structure in a 1-to-1 fashion, 
supporting both Solo Practitioner and Multi-Level Organizational Hierarchies.
Includes all 46 fields required by the validation schema in the exact ordinal sequence.
"""

MAPPINGS = {
    "name": "Provider Hierarchy 7.12 Declarative Schema Mapping",
    "mapping_type": "only_mapped",
    "expressions": {
        # 0. TEMPLATE
        "TEMPLATE":         "'TEMPLATE'",
        
        # 1-3. Core Doctor Identifiers (Isolate entity_type_qualifier='1' for Practitioner)
        "PROVIDERID":       "detail.nm1[entity_identifier_code='1P' and entity_type_qualifier='1'].(identifier ? identifier : nm111_111)",
        "PROVIDERLASTNAME": "detail.nm1[entity_identifier_code='1P' and entity_type_qualifier='1'].(entity_type_qualifier='1' ? ($join([first_name, middle_name, name][$ != null], ' ')) : name)",
        "PROVIDERNPI":      "detail.nm1[entity_identifier_code='1P' and entity_type_qualifier='1'].(identifier ? identifier : nm111_111)",
        
        # 4-9. Core Location Fields
        "LOCATIONGROUPID":  "detail.nm1[entity_identifier_code='41'].submitter_id ? detail.nm1[entity_identifier_code='41'].submitter_id : detail.nm1[entity_identifier_code='85'].(billing_provider_id ? billing_provider_id : identifier)",
        "LOCATIONRANKING":  "1",
        "LOCATIONIDTYPE":   "detail.nm1[entity_identifier_code='1P' and entity_type_qualifier='1'].entity_identifier_code",
        "LOCATIONID":       "detail.nm1[entity_identifier_code='1P' and entity_type_qualifier='1'].(identifier ? identifier : nm111_111)",
        "LOCATIONDESC":     "detail.nm1[entity_identifier_code='1P' and entity_type_qualifier='1'].(entity_type_qualifier='1' ? ($join([first_name, middle_name, name][$ != null], ' ')) : name)",
        "LOCATIONTIN":      "detail.ref[0].(employer_id ? employer_id : champus_id)",
        
        # 10-14. Core Location Address Fields (Filter-coalesce for practitioner office address)
        "LOCATIONADDRESS1": "[detail.n3[address_line_2 != null].address_line_1, (detail.n3[address_line_1 != null].address_line_1)[0], detail.n3[0].billing_provider_address_line_1][$ != null][0]",
        "LOCATIONADDRESS2": "detail.n3[address_line_2 != null].address_line_2",
        "LOCATIONCITY":     "[(detail.n4[city != null].city)[0], detail.n4[0].billing_provider_city][$ != null][0]",
        "LOCATIONSTATE":    "[(detail.n4[city != null].state)[0], detail.n4[0].billing_provider_state][$ != null][0]",
        "LOCATIONZIP":      "[(detail.n4[city != null].zip_code)[0], detail.n4[0].billing_provider_zip_code][$ != null][0]",
        
        # 15-19. Optional / Unmapped Location Attributes
        "COUNTYCODE":        "(detail.n4[city != null and n406_406 != null].n406_406)[0]",
        "PHONENUMBER":      "[(detail.per[per01='AJ' or per01='IC'].(per03_03='TE' ? per04_04 : (per05_05='TE' ? per06_06 : (per07_07='TE' ? per08_08 : null))))[1], (detail.per[per01='AJ' or per01='IC'].(per03_03='TE' ? per04_04 : (per05_05='TE' ? per06_06 : (per07_07='TE' ? per08_08 : null))))[0]][$ != null][0]",
        "FAXNUMBER":        "[(detail.per[per01='AJ' or per01='IC'].(per03_03='FX' ? per04_04 : (per05_05='FX' ? per06_06 : (per07_07='FX' ? per08_08 : null))))[1], (detail.per[per01='AJ' or per01='IC'].(per03_03='FX' ? per04_04 : (per05_05='FX' ? per06_06 : (per07_07='FX' ? per08_08 : null))))[0]][$ != null][0]",
        "CONTACTPERSON":    "[(detail.per[per01='AJ' or per01='IC'].per02_02)[1], (detail.per[per01='AJ' or per01='IC'].per02_02)[0]][$ != null][0]",
        "DONOTCHASE":        "detail.unmapped",
        
        # 20-27. Tier 2 (Clinic Group - Multi-Level Organizational Hierarchy)
        "TIER2IDTYPE":      "detail.nm1[entity_identifier_code='1P' and entity_type_qualifier='2'].id_qualifier ? detail.nm1[entity_identifier_code='1P' and entity_type_qualifier='2'].id_qualifier : detail.nm1[entity_identifier_code='85'].billing_provider_id_qualifier",
        "TIER2ID":          "detail.nm1[entity_identifier_code='1P' and entity_type_qualifier='2'].identifier ? detail.nm1[entity_identifier_code='1P' and entity_type_qualifier='2'].identifier : detail.nm1[entity_identifier_code='85'].billing_provider_id",
        "TIER2DESC":        "detail.nm1[entity_identifier_code='1P' and entity_type_qualifier='2'].name ? detail.nm1[entity_identifier_code='1P' and entity_type_qualifier='2'].name : detail.nm1[entity_identifier_code='85'].billing_provider_name",
        "TIER2ADDRESS1":    "(detail.n3[address_line_1 != null].address_line_1)[0]",
        "TIER2ADDRESS2":    "(detail.n3[address_line_2 = null and address_line_1 != null].address_line_2)[0]",
        "TIER2CITY":        "(detail.n4[city != null].city)[0]",
        "TIER2STATE":       "(detail.n4[city != null].state)[0]",
        "TIER2ZIP":         "(detail.n4[city != null].zip_code)[0]",
        
        # 28-35. Tier 3 (Health System - Populated via reference table join in Silver)
        "TIER3IDTYPE":       "detail.unmapped",
        "TIER3ID":          "detail.unmapped",
        "TIER3DESC":        "detail.unmapped",
        "TIER3ADDRESS1":    "detail.unmapped",
        "TIER3ADDRESS2":    "detail.unmapped",
        "TIER3CITY":        "detail.unmapped",
        "TIER3STATE":       "detail.unmapped",
        "TIER3ZIP":         "detail.unmapped",
        
        # 36-43. Tier 4 (Submitter/Network level - NM1*85 segment address)
        "TIER4IDTYPE":      "detail.nm1[entity_identifier_code='85'].billing_provider_id_qualifier",
        "TIER4ID":          "detail.nm1[entity_identifier_code='85'].billing_provider_id",
        "TIER4DESC":        "detail.nm1[entity_identifier_code='85'].billing_provider_name",
        "TIER4ADDRESS1":    "detail.n3[billing_provider_address_line_1 != null].billing_provider_address_line_1[0]",
        "TIER4ADDRESS2":    "detail.n3[billing_provider_address_line_2 != null].billing_provider_address_line_2[0]",
        "TIER4CITY":        "detail.n4[billing_provider_city != null].billing_provider_city[0]",
        "TIER4STATE":       "detail.n4[billing_provider_city != null].billing_provider_state[0]",
        "TIER4ZIP":         "detail.n4[billing_provider_city != null].billing_provider_zip_code[0]",
        
        # 44-45. Relationship Dates
        "STARTDATE":        "detail.dtp[date_qualifier='007'].date_value ? detail.dtp[date_qualifier='007'].date_value : heading.bht.bht04_04",
        "ENDDATE":          "detail.dtp[date_qualifier='008'].date_value"
    }
}
