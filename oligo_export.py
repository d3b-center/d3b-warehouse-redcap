#!/usr/bin/env python

# python oligo_export.py REDCAP_TOKEN_27084 BRP_TOKEN 108

import argparse
import os
from pprint import pprint

import redcap
from brp import BRP
from config import config
from nautilus import get_nautilus_data

# DUMMY VALUES
ORG = 52


def instrument_df(store, event, instrument, drop_instance=False):
    df = redcap.to_df(store[event][instrument])

    if df.empty:
        print(f"No data from: '{event}' / '{instrument}'")
        exit(1)

    if drop_instance:
        df = df.drop(columns='instance', errors='ignore')

    return df


def redcap_export(redcap_token):
    store = redcap.RedcapStudy(redcap_token).get_records_tree()

    enrollment_df = instrument_df(store, 'Enrollment', '', True)
    demographics_df = instrument_df(store, 'Demographics', '', True)
    diagnosis_df = instrument_df(store, 'Diagnoses', 'Diagnosis Form')
    treatment_df = instrument_df(store, 'Diagnoses', 'Treatment Form', True)
    update_df = instrument_df(store, 'Diagnoses', 'Updates Data Form', True)
    specimen_df = instrument_df(store, 'Specimen Only', 'Specimen Only', True)
    subject_df = redcap.df_link(
        enrollment_df, demographics_df, 'subject', 'subject'
    )

    def link_to_diagnosis(left, left_on):
        return redcap.new_column_from_linked(
            left, diagnosis_df, left_on, 'instance', 'linked_diagnosis',
            ['date_of_initial_diagnosis', 'diagnosis_type', 'init_path_diag'],
            ' :: '
        )

    # maybe do these in the db instead
    treatment_df = link_to_diagnosis(treatment_df, 'tx_dx_link')
    update_df = link_to_diagnosis(update_df, 'update_to_which_dx')
    specimen_df = link_to_diagnosis(specimen_df, 'sx_dx_link')

    return {
        'subjects': subject_df,
        'diagnoses': diagnosis_df,
        'treatments': treatment_df,
        'updates': update_df,
        'specimens': specimen_df
    }


def get_ehb_subjects(brp_token, redcap_subject_df):
    brp = BRP(brp_token)

    ehb_subjects = {
        (s['organization'], s['organization_subject_id']): s['id']
        for s in brp.get_subjects(args.brp_protocol)
    }

    study_subjects = {}
    for i, s in redcap_subject_df.iterrows():
        ident = (s.get('organization', ORG), s.get('mrn'))
        subj = f'Subject {s["subject"]} {ident}'
        if s.get('enrollment_complete') == 'Complete':
            if ident in ehb_subjects:
                print(f'{subj} already in BRP with id: {ehb_subjects[ident]}')
                study_subjects[ident] = ehb_subjects[ident]
            else:
                print(f'Submitting {subj} to BRP... ⏳')
                created = brp.create_subject(
                    args.brp_protocol,
                    s.get('organization', ORG), s.get('mrn'),
                    s.get('first_name'), s.get('last_name'), s.get('dob')
                )
                pprint(created)
                created = created['response']
                if created[0]:
                    study_subjects[ident] = created[1]['id']
        else:
            print(f'{subj} ENROLLMENT NOT COMPLETE')
        print('-' * 80)

    return study_subjects


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'redcap_token_env_key',
        help=(
            'Env key for fetching the REDCap study API token'
            ' (e.g. REDCAP_TOKEN_27084)'
        )
    )
    parser.add_argument(
        'brp_token_env_key',
        help='Env key for fetching the BRP API token (e.g. BRP_TOKEN)'
    )
    parser.add_argument(
        'brp_protocol',
        help='BRP protocol number for the study'
    )
    parser.add_argument(
        'naut_irb_protocol',
        help='IRB protocol number for the study in '
        'the Nautilus (e.g. NAUTILUS_IRB_NUMBER)'
    )
    args = parser.parse_args()

    redcap_token = os.getenv(args.redcap_token_env_key)
    brp_token = os.getenv(args.brp_token_env_key)
    nautilus_irb = os.getenv(args.naut_irb_protocol)

    rc_data = redcap_export(redcap_token)
    ehb_subjects = get_ehb_subjects(brp_token, rc_data['subjects'])
    print('rc_data dict keys: ' + str(rc_data.keys()), flush=True)

    # Read connection Parameters
    params = config()
    sample_information = get_nautilus_data(params, nautilus_irb)
    print(sample_information.count())

    breakpoint()
