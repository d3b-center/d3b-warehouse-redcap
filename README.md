# D3b Data Warehouse

The D3b Data Warehouse extracts clinical data from REDCap, de-identifies it via BRP, extracts specimen information from Nautilus, and store them all into PostgreSQL.
Metabase is also available for custom query and dashboard creation, running against PostgreSQL. 

## Getting Started

1. Create a `compose.env` using `.env.schema` with corresponding credentials. 
2. The test BRP protocol is preset to 108. To change this, modify `./scripts/run.sh` with a proper protocol.
3. Make sure Docker and Docker Compose are installed, then run: `docker-compose up --build`
4. Once spun up, Metabase is available at `localhost:3000`.
