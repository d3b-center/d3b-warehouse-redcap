# D3b REDCap Warehouser

## Purpose

1. Extracts clinical data from REDCap
1. De-identifies it via BRP-eHB
1. Stores everything into a warehouse database.

## CLI Help

`python warehouse_project.py --help`

## Known invocations

Where:

* `REDCAP_TOKEN_#####` is an environment key storing the API token for that REDCap project
* `BRP_TOKEN` is an environment key storing the user API token for the BRP
* `CID_MAGIC_NUMBER` is an environment key storing the magic number used for generating CIDs from eHB IDs
* `D3B_WAREHOUSE_DB_URL` is an environment key storing an authenticated URL (protocol://user:password@address:port/db) for the D3b warehouse

### Oligo Nation, REDCap project 27084, BRP protocol 159, eHB organization 102

`python warehouse_project.py REDCAP_TOKEN_27084 BRP_TOKEN 159 CID_MAGIC_NUMBER D3B_WAREHOUSE_DB_URL --redcap_organization_override_value 102 --redact description_of_chemotherap --redact other_rad_treat --redact describe_predis`

### DGD, REDCap project 33723, BRP protocol 95, (only if CID exists)

`python warehouse_project.py REDCAP_TOKEN_33723 BRP_TOKEN 95 CID_MAGIC_NUMBER D3B_WAREHOUSE_DB_URL --redcap_id_within_organization_field mrn --only_warehouse_if_CID_already_exists --fillmask diagnosis_id=dgd_diagnosis=d3b_event_identifiers`
