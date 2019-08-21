FROM python:3.7-slim
LABEL maintainer="d3b-center"

WORKDIR /app

# Setup deps
RUN apt-get update && \
    apt-get install -y postgresql-client && \
    pip install --upgrade pip

# Install requirements
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

# Setup ETL
COPY d3b_warehouse_etl/ d3b_warehouse_etl
COPY scripts/wait-for-db.sh wait-for-db.sh
COPY scripts/run.sh run.sh

# Run ETL
CMD ["./run.sh"]
