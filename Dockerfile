# build on base image
FROM python:3.7-stretch
LABEL maintainer="d3b-center"

# set working directory
WORKDIR /app

# setup deps
RUN apt-get update && \
    apt-get install -y gcc libpq-dev && \
    pip install --upgrade pip

# copy necessary files
COPY requirements.txt ./
COPY ./d3b_warehouse_redcap ./d3b_warehouse_redcap
COPY warehouse_project.py ./
COPY scripts/entrypoint.sh entrypoint.sh

# install dependencies
RUN pip install -r requirements.txt

# run on container start
CMD ["./entrypoint.sh"]