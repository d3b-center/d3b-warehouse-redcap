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


def datawarehouseconfig():
    PG_HOST = os.environ.get('PG_HOST', 'localhost')
    PG_PORT = os.environ.get('PG_PORT', 5432)
    PG_NAME = os.environ.get('PG_NAME', 'dev')
    PG_USER = os.environ.get('PG_USER', 'postgres')
    PG_PASS = os.environ.get('PG_PASS', '')
    DB_URI = 'postgres://{}:{}@{}:{}/{}'.format(
        PG_USER, PG_PASS, PG_HOST, PG_PORT, PG_NAME)
    return DB_URI
