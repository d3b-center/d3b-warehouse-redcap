#!/usr/bin/python
import os


def config():
    db = {}
    db['host'] = os.environ.get('NAUTILUS_HOST')
    db['user'] = os.environ.get(
        'NAUTILUS_DB_USR_NAME', 'postgres')
    db['password'] = os.environ.get('NAUTILUS_DB_USR_PWD', '')
    db['port'] = os.environ.get('NAUTILUS_PORT', 5432)
    db['database'] = os.environ.get('NAUTILUS_DB_NAME')
    return db
