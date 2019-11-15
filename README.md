# D3b Warehouse REDCap

The D3b Warehouse REDCAP extracts clinical data from REDCap, de-identifies it via BRP, extracts specimen information from Nautilus, and store them all into PostgreSQL.
Metabase is also available for custom query and dashboard creation, running against PostgreSQL. 

## Getting Started

1. Create a `compose.env` using `.env.schema` with corresponding credentials. 
2. Make sure Docker and Docker Compose are installed, then run: `docker-compose up --build`
3. Once spun up, Metabase is available at `localhost:3000`.
