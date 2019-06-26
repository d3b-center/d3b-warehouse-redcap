#!/usr/bin/env python

# python oligo_export.py REDCAP_TOKEN_APITEST BRP_TOKEN

import argparse
import os
from pprint import pprint

import brp
import redcap

# DUMMY VALUES
BRP_PROTOCOL = 108
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
    # update_df = link_to_diagnosis(
    #     redcap.to_df(store, 'Updates Data Form', 'instance'), 'ux_dx_link'
    # )
    # specimen_df = link_to_diagnosis(
    #     redcap.to_df(store, 'Specimen Only', 'instance'), 'sx_dx_link'
    # )

    return {
        'subjects': subject_df,
        'diagnoses': diagnosis_df,
        'treatments': treatment_df,
        # 'updates': update_df,
        # 'specimens': specimen_df
    }


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'redcap_token_env_key',
        help='Env key for fetching the REDCap token (e.g. REDCAP_TOKEN_27084)'
    )
    parser.add_argument(
        'brp_token_env_key',
        help='Env key for fetching the BRP token (e.g. BRP_TOKEN)'
    )
    args = parser.parse_args()

    redcap_token = os.getenv(args.redcap_token_env_key)
    brp_token = os.getenv(args.brp_token_env_key)

    rc = redcap_export(redcap_token)

    for i, s in rc['subjects'].iterrows():
        pprint(
            brp.create_subject(
                brp_token,
                BRP_PROTOCOL,
                ORG, s['mrn'], s['first_name'], s['last_name'], s['dob']
            )
        )
        print()
