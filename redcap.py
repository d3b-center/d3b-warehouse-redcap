from collections import defaultdict

import numpy
import pandas

from session import requests_retry_session

REDCAP_API = 'https://redcap.chop.edu/api/'


class RedcapStudy:
    def __init__(self, api_token):
        self.api_token = api_token

    def _get_json(self, type, extra_args_dict=None):
        params = {
            'token': self.api_token,
            'content': type,
            'format': 'json',
            'returnFormat': 'json'
        }
        if extra_args_dict:
            params.update(extra_args_dict)
        return requests_retry_session().post(
            REDCAP_API, data=params
        ).json()

    def _get_repeat_instruments(self):
        return {v['form_name'] for v in self._get_json('repeatingFormsEvents')}

    def _get_events(self):
        return self._get_json('event')

    def _get_instruments(self):
        return self._get_json('instrument')

    def _get_instrument_event_mappings(self):
        """
        Return a mapping from instruments to the events they're in.
        """
        return{
            v['form']: v['unique_event_name']
            for v in self._get_json('formEventMapping')
        }

    def _get_metadata(self):
        return self._get_json('metadata')

    def _get_selector_mapping_tree(self):
        """
        Get a tree of selector mappings for translating raw record values.
        It looks like:
        {
            <instrument>: {
                <field_name>: {
                    index: readable_value,
                    ...
                },
                ...
            },
            ...
        }
        """
        metadata = self._get_metadata()
        repeat_instruments = self._get_repeat_instruments()
        ie_mappings = self._get_instrument_event_mappings()
        store = defaultdict(  # forms
                dict          # field value maps
        )
        for m in metadata:
            instrument = m['form_name']
            field_name = m['field_name']
            field_type = m['field_type']
            if field_type in {'dropdown', 'radio', 'checkbox'}:
                choices = m['select_choices_or_calculations']
                store[entity][field_name] = dict(
                    map(lambda x: x.split(', ', 1), choices.split(' | '))
                )
        return store

    def _get_eav_records(self, raw=False):
        """
        This can optionally retrieve raw instead of labels, because two
        different instruments could be given the same name which are meant to
        be interpreted based on context. That may mean that we couldn't
        differentiate between the two, so we should defer translating headers
        until the very end.

        Unfortunately there's no way to independently ask for translated
        selector values (e.g. "Female" instead of "1") without also asking for
        translated headers, so asking for raw means doing a lot more work
        selectively digging through project metadata to map the selectors.
        This is made more difficult by the fact that the REDCap project
        metadata uniformly categorizes fields by their instrument name, but the
        records API doesn't report the instrument name for records that come
        from instruments that aren't repeating.
        """
        return self._get_json(
            'record',
            {
                'type': 'eav',
                'rawOrLabel': 'raw' if raw else 'label',
                'exportSurveyFields': 'false',
                'exportDataAccessGroups': 'false',
            }
        )

    def get_records_tree(self):
        store = defaultdict(                  # events
            lambda: defaultdict(              # instrument
                lambda: defaultdict(          # subjects
                    lambda: defaultdict(      # instances
                        lambda: defaultdict(  # field names
                            set               # values
                        )
                    )
                )
            )
        )
        for r in self._get_eav_records():
            event = r['redcap_event_name']
            instrument = r['redcap_repeat_instrument']
            subject = r['record']
            # The API will return 1, '2', for repeat instances.
            # Note that 1 was an int and 2 was a str.
            instance = str(r['redcap_repeat_instance'] or '1')
            field = r['field_name']
            value = r['value']
            if field != 'study_id':
                store[event][instrument][subject][instance][field].add(value)

        return store


def clean(df):
    return df.replace(numpy.nan, '').astype(str)


def to_df(instrument):
    acc = []
    for p, es in instrument.items():
        for i, e in es.items():
            thing = {'subject': p, 'instance': i}
            for k, v in e.items():
                thing[k] = '+'.join(sorted(v))
            acc.append(thing)

    return clean(pandas.DataFrame.from_records(acc))


def df_link(left, right, left_on, right_on):
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
    df = df_link(left, right, left_on, right_on)

    nc = None
    for c in from_cols:
        if nc is None:
            nc = df[c]
        else:
            nc = nc + separator + df[c]
    df[new_col] = nc.replace(f'^{separator}{separator}$', '', regex=True)

    return df[[new_col] + list(left.columns)]
