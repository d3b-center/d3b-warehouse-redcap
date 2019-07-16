# d3b-redcap
Holder for redcap project code and formats


### Exporting redcap+brp/ehb data

Set environment variables for your REDCap and BRP tokens, and then do:

`> python oligo_export.py REDCAP_TOKEN_ENV_KEY BRP_TOKEN_ENV_KEY BRP_PROTOCOL NAUTILUS_IRB_NUMBER`

The test BRP protocol is 108.
NAUTILUS_IRB_NUMBER is the registered IRB protocol number for a study. For example, CBTTC has 7316
