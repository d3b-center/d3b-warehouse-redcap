# d3b-redcap
Holder for redcap project code and formats

#Running the Docker in local machine
docker run --name d3b-warehouse-pg -p 5432:5432 -d postgres:9.5

#create local d
docker exec d3b-warehouse-pg psql -U postgres -c "CREATE DATABASE dev;"

### Exporting redcap+brp/ehb data

Set environment variables for your REDCap and BRP tokens, and then do:

`> python oligo_export.py REDCAP_TOKEN_ENV_KEY BRP_TOKEN_ENV_KEY BRP_PROTOCOL NAUTILUS_IRB_NUMBER`

The test BRP protocol is 108.
NAUTILUS_IRB_NUMBER is the registered IRB protocol number for a study. For example, CBTTC has 7316
