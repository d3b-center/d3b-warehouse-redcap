#!/usr/bin/python
import os


nautilus_config = {
    'user': os.environ.get('NAUTILUS_DB_USR_NAME', 'postgres'),
    'password': os.environ.get('NAUTILUS_DB_USR_PWD', ''),
    'host': os.environ['NAUTILUS_HOST'],
    'port': os.environ.get('NAUTILUS_PORT', 5432),
    'dbname': os.environ['NAUTILUS_DB_NAME'],
}


datawarehouse_config = {
    'user': os.environ.get('PG_USER', 'postgres'),
    'password': os.environ.get('PG_PASS', ''),
    'host': os.environ.get('PG_HOST', 'localhost'),
    'port': os.environ.get('PG_PORT', 5432),
    'dbname': os.environ.get('PG_NAME', 'dev'),
}
