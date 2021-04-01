# D3b REDCap Warehouser

## Purpose

1. Extracts clinical data from REDCap
1. De-identifies it via BRP-eHB
1. (NOT YET IMPLEMENTED) Extracts specimen information from Nautilus
1. Stores everything into a warehouse database.

## Known invocations


### Oligo Nation, REDCap project 27084, BRP protocol 159, eHB organization 102

`python warehouse_project.py REDCAP_TOKEN_27084 BRP_TOKEN 159
D3B_WAREHOUSE_DB_URL CID_MAGIC_NUMBER --redcap_api https://redcap.chop.edu/api/
--redcap_organization_override_value 102 --redact_redcap_field description_of_chemotherap
--redact_redcap_field other_rad_treat --redact_redcap_field describe_predis`
