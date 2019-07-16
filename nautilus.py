#!/usr/bin/python
import psycopg2
import pandas as pd


def get_nautilus_data(params, nautilus_irb):
    """ Connect to the Nautilus PostgreSQL datawarehouse database """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()

        cur.execute(
            "SELECT * from eig_nautilus_aliquot "
            "where study_name='{}'".format(nautilus_irb))

        names = [x[0] for x in cur.description]
        sample_information = pd.DataFrame(cur.fetchall(), columns=names)

        # close the communication with the PostgreSQL
        cur.close()
        return sample_information
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')
