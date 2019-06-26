#!/usr/bin/env python

# python redcap_export.py REDCAP_TOKEN_27084

import argparse
import logging
import os
from collections import defaultdict
from pprint import pprint

import numpy
import pandas
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from urllib3 import connectionpool

connectionpool.log.setLevel(logging.INFO)
SEP = ' :: '


# Taken from the kf ingest lib. We should have a central place for this.
def requests_retry_session(
        session=None, total=10, read=10, connect=10, status=10,
        backoff_factor=5, status_forcelist=(500, 502, 503, 504)
):
    """
    Send an http request and retry on failures or redirects

    See urllib3.Retry docs for details on all kwargs except `session`
    Modified source: https://www.peterbe.com/plog/best-practice-with-retries-with-requests # noqa E501

    :param session: the requests.Session to use
    :param total: total retry attempts
    :param read: total retries on read errors
    :param connect: total retries on connection errors
    :param status: total retries on bad status codes defined in
    `status_forcelist`
    :param backoff_factor: affects sleep time between retries
    :param status_forcelist: list of HTTP status codes that force retry
    """
    session = session or requests.Session()

    retry = Retry(
        total=total,
        read=read,
        connect=connect,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        method_whitelist=False
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    return session


def get_records(token):
    data = {
        'token': token,
        'content': 'record',
        'format': 'json',
        'type': 'eav',
        'rawOrLabel': 'label',
        'rawOrLabelHeaders': 'label',
        'exportCheckboxLabel': 'true',
        'exportSurveyFields': 'true',
        'exportDataAccessGroups': 'false',
        'returnFormat': 'json'
    }
    return requests_retry_session().post(
        'https://redcap.chop.edu/api/', data=data
    ).json()


def build_store_from_eav_records(records):
    store = defaultdict(
        lambda: defaultdict(          # entities
            lambda: defaultdict(      # subjects
                lambda: defaultdict(  # instances
                    set               # values
                )
            )
        )
    )
    for r in records:
        entity = r['redcap_repeat_instrument'] or r['redcap_event_name']
        subject = r['record']
        # the API will return 1, '2', for repeat instances.
        # Note that 1 is an int and 2 is a str. *eyeroll*
        instance = str(r['redcap_repeat_instance'] or '1')
        field = r['field_name']
        value = r['value']
        if field != 'study_id':
            store[entity][subject][instance][field].add(value)
    return store


def clean(df):
    return df.replace(numpy.nan, '').astype(str)


def to_df(entities, instance=False):
    acc = []
    for p, es in entities.items():
        for i, e in es.items():
            thing = {'subject': p, 'instance': i}
            for k, v in e.items():
                thing[k] = SEP.join(sorted(v))
            acc.append(thing)

    df = pandas.DataFrame.from_records(acc)

    if not instance:
        del df['instance']

    return clean(df)


def link(left, right, left_on, right_on):
    return clean(
        left.merge(
            right, how='left',
            left_on=['subject', left_on], right_on=['subject', right_on]
        )
        .sort_values(by=['subject', left_on])
        .set_index('subject').reset_index()
    )


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'token_env_key',
        help='Env key for fetching the REDCap token (e.g. REDCAP_TOKEN_27084)'
    )
    args = parser.parse_args()

    records = get_records(os.getenv(args.token_env_key))
    store = build_store_from_eav_records(records)

    def link_to_diagnosis(left, left_on):
        df = link(left, diagnosis_df, left_on, 'instance')
        df['linked_diagnosis'] = (
            df['date_of_initial_diagnosis']
            + SEP + df['diagnosis_type']
            + SEP + df['init_path_diag']
        ).replace(f'^{SEP}{SEP}$', '', regex=True)
        return df[['linked_diagnosis'] + list(left.columns)]

    subject_df = to_df(
        {**store['Enrollment'], **store['Demographics']}
    )
    diagnosis_df = to_df(store['Diagnosis Form'], instance=True)
    treatment_df = link_to_diagnosis(
        to_df(store['Treatment Form']), 'tx_dx_link'
    )
    # update_df = link_to_diagnosis(
    #     to_df(store['Updates Data Form']), 'ux_dx_link'
    # )
    # specimen_df = link_to_diagnosis(
    #     to_df(store['Specimen Only']), 'sx_dx_link'
    # )

    breakpoint()
