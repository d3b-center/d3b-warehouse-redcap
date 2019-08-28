#!/usr/bin/python
import pandas as pd
import psycopg2
from config import nautilus_config


def get_nautilus_data(nautilus_irb):
    """
    Get aliquot data from the Nautilus PostgreSQL database for the given study
    """
    with psycopg2.connect(**nautilus_config) as conn:
        return pd.read_sql(
            "SELECT * from eig_nautilus_aliquot "
            f"where study_name='{nautilus_irb}'"
        )
