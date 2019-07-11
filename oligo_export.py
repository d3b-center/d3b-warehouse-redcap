#!/usr/bin/env python

# python oligo_export.py REDCAP_TOKEN_APITEST BRP_TOKEN 108

import argparse
import os
from pprint import pprint

import brp
import redcap

# DUMMY VALUES
ORG = 2


def redcap_export(redcap_token):
    records = redcap.get_eav_records(redcap_token)
    store = redcap.build_store_from_eav_records(records)

    enrollment_df = redcap.to_df(
        store, 'Enrollment', 'instance'
    )
    demographics_df = redcap.to_df(
        store, 'Demographics', 'instance'
    )
    subject_df = redcap.link(
        enrollment_df, demographics_df, 'subject', 'subject'
    )
    diagnosis_df = redcap.to_df(store, 'Diagnosis Form')

    def link_to_diagnosis(left, left_on):
        return redcap.new_column_from_linked(
            left, diagnosis_df, left_on, 'instance', 'linked_diagnosis',
            ['date_of_initial_diagnosis', 'diagnosis_type', 'init_path_diag'],
            ' :: '
        )

    treatment_df = link_to_diagnosis(
        redcap.to_df(store, 'Treatment Form', 'instance'), 'tx_dx_link'
    )
    update_df = link_to_diagnosis(
        redcap.to_df(store, 'Updates Data Form', 'instance'),
        'update_to_which_dx'
    )
    specimen_df = link_to_diagnosis(
        redcap.to_df(store, 'Specimen Only', 'instance'), 'sx_dx_link'
    )
    return {
        'subjects': subject_df,
        'diagnoses': diagnosis_df,
        'treatments': treatment_df,
        'updates': update_df,
        'specimens': specimen_df
    }


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
    args = parser.parse_args()

    redcap_token = os.getenv(args.redcap_token_env_key)
    brp_token = os.getenv(args.brp_token_env_key)

    rc = redcap_export(redcap_token)

    for i, s in rc['subjects'].iterrows():
        print(f'Subject {s["subject"]}')
        if s.get('enrollment_complete') == 'Complete':
            print("Submitting... ⏳")
            pprint(
                brp.create_subject(
                    brp_token,
                    args.brp_protocol,
                    s.get('organization', ORG), s.get('mrn'),
                    s.get('first_name'), s.get('last_name'), s.get('dob')
                )
            )
        else:
            print('ENROLLMENT NOT COMPLETE')
        print('-' * 80)

    print("rc dict keys: " + str(rc.keys()), flush=True)
    breakpoint()
