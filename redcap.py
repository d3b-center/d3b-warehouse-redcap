from collections import defaultdict

import numpy
import pandas

from session import requests_retry_session

REDCAP_API = 'https://redcap.chop.edu/api/'


def get_eav_records(redcap_token):
    data = {
        'token': redcap_token,
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
        REDCAP_API, data=data
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
        # The API will return 1, '2', for repeat instances.
        # Note that 1 is an int and 2 is a str.
        instance = str(r['redcap_repeat_instance'] or '1')
        field = r['field_name']
        value = r['value']
        if field != 'study_id':
            store[entity][subject][instance][field].add(value)
    return store


def clean(df):
    return df.replace(numpy.nan, '').astype(str)


def to_df(store, key, del_cols=None):
    if isinstance(del_cols, str):
        del_cols = [del_cols]

    acc = []
    for p, es in store[key].items():
        for i, e in es.items():
            thing = {'subject': p, 'instance': i}
            for k, v in e.items():
                thing[k] = '+'.join(sorted(v))
            acc.append(thing)

    df = pandas.DataFrame.from_records(acc)

    for c in del_cols or []:
        del df[c]

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


def new_column_from_linked(
    left, right, left_on, right_on, new_col, from_cols, separator
):
    df = link(left, right, left_on, right_on)

    nc = None
    for c in from_cols:
        if nc is None:
            nc = df[c]
        else:
            nc = nc + separator + df[c]
    df[new_col] = nc.replace(f'^{separator}{separator}$', '', regex=True)

    return df[[new_col] + list(left.columns)]
