#!/usr/bin/env python

# python oligo_export.py REDCAP_TOKEN_27084 BRP_TOKEN 108

import argparse
import os
from pprint import pprint

import redcap
from brp import BRP
from config import config, datawarehouseconfig
from nautilus import get_nautilus_data
import pandas as pd
from sqlalchemy import create_engine

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
    project = redcap.RedcapStudy(redcap_token)._get_project_info()
    project = pd.DataFrame(list(project.values()),
                           index=list(project.keys())).transpose()

    enrollment_df = instrument_df(store, 'Enrollment', '', True)
    demographics_df = instrument_df(store, 'Demographics', '', True)
    diagnosis_df = instrument_df(store, 'Diagnoses', 'Diagnosis Form')
    treatment_df = instrument_df(store, 'Diagnoses', 'Treatment Form', True)
    update_df = instrument_df(store, 'Diagnoses', 'Updates Data Form', True)
    specimen_df = instrument_df(store, 'Specimen', 'Specimen', True)
    '''
    subject_df = redcap.df_link(
        enrollment_df, demographics_df, 'subject', 'subject'
    )
    '''

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
        'project_info': project,
        'subjects': enrollment_df,
        'demographics': demographics_df,
        'diagnoses': diagnosis_df,
        'treatments': treatment_df,
        'updates': update_df,
        'specimens': specimen_df
    }


def get_ehb_subjects(brp_token, redcap_subject_df):
    brp = BRP(brp_token)
    print(redcap_subject_df.count())
    ehb_subjects = {
        (s['organization'], s['organization_subject_id']): s['id']
        for s in brp.get_subjects(brp_protocol)
    }

    study_subjects = {}
    for i, s in redcap_subject_df.iterrows():
        ident = (s.get('organization', ORG), s.get('mrn'))
        subj = f'Subject {s["subject"]} {ident}'
        if s.get('enrollment_complete') == 'Complete':
            if ident in ehb_subjects:
                print(f'{subj} already in BRP with id: {ehb_subjects[ident]}')
                study_subjects[s["subject"]] = ehb_subjects[ident]
            else:
                print(f'Submitting {subj} to BRP... ⏳')
                created = brp.create_subject(
                    brp_protocol,
                    s.get('organization', ORG), s.get('mrn'),
                    s.get('first_name'), s.get('last_name'), s.get('dob')
                )
                pprint(created)
                created = created['response']
                if created[0]:
                    study_subjects[s["subject"]] = created[1]['id']

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
        help='BRP protocol number for the study (e.g. BRP_PROTOCOL)'
    )
    parser.add_argument(
        'naut_irb_protocol',
        help='IRB protocol number for the study in '
        'the Nautilus (e.g. NAUTILUS_IRB_NUMBER)'
    )
    args = parser.parse_args()

    redcap_token = os.getenv(args.redcap_token_env_key)
    brp_token = os.getenv(args.brp_token_env_key)
    brp_protocol = os.getenv(args.brp_protocol)
    nautilus_irb = os.getenv(args.naut_irb_protocol)

    rc_data = redcap_export(redcap_token)

    ehb_subjects = get_ehb_subjects(brp_token, rc_data['subjects'])
    ehb_subjects = pd.DataFrame(list(ehb_subjects.items()),
                                columns=['subject', 'ehb_id'])
    print('rc_data dict keys: ' + str(rc_data.keys()), flush=True)

    # Read connection Parameters
    params = config()
    sample_information = get_nautilus_data(params, nautilus_irb)

    # Writing to datawarehouse
    uri = datawarehouseconfig()
    engine = create_engine(uri)

    # ehb subjects
    rc_data['project_info'].to_sql('project_info', engine,  if_exists='append',
                                   index=False)
    project_id = rc_data['project_info'].project_id.unique()[0]
    # ehb subjects
    ehb_subjects.to_sql('{}_ehb_subject'.format(project_id), engine,
                        if_exists='replace',
                        index=False)
    # demographics
    rc_data['demographics'].to_sql('{}_demographics'.format(project_id),
                                   engine,
                                   if_exists='replace',
                                   index=False)
    # Diagnoses
    rc_data['diagnoses'].to_sql('{}_diagnosis'.format(project_id),
                                engine,  if_exists='replace',
                                index=False)
    rc_data['updates'].to_sql('{}_update'.format(project_id),
                              engine,  if_exists='replace',
                              index=False)
    rc_data['treatments'].to_sql('{}_treatment'.format(project_id),
                                 engine,  if_exists='replace',
                                 index=False)
    rc_data['specimens'].to_sql('{}_specimen'.format(project_id),
                                engine,  if_exists='replace',
                                index=False)
    sample_information.to_sql('sample_information',
                              engine, if_exists='append', index=False)
