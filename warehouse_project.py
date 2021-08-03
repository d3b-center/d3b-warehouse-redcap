#!/usr/bin/env python3
import argparse
import os
import sys
from datetime import datetime

from d3b_redcap_api.df_utils import all_dfs
from d3b_redcap_api.redcap import REDCapStudy
from dateutil import relativedelta
from numpy import isnan
from pandas import DataFrame, notnull, to_datetime
from sqlalchemy import create_engine, schema

from d3b_warehouse_redcap.brp import BRP

# defaults
RC_ENROLLMENT_FORM = "enrollment"
RC_FIRSTNAME_FIELD = "first_name"
RC_LASTNAME_FIELD = "last_name"
RC_DOB_FIELD = "dob"
RC_ORG_ID_FIELD = "external_id"
RC_ORG_FIELD = "organization"
RC_ORG_OVERRIDE = None
CID_MAGIC_NUMBER = None


def redcap_subjects_to_CIDs(redcap_dfs, brp_api_url, brp_token, brp_protocol, create_if_new=True):
    """Replace REDCap DataFrame subject IDs with CIDs from the BRP-eHB"""
    subject_fields = {
        RC_ORG_FIELD,
        RC_ORG_ID_FIELD,
        f"{RC_ENROLLMENT_FORM}_complete",
    }

    if RC_ORG_OVERRIDE is not None:
        subject_fields.remove(RC_ORG_FIELD)

    if create_if_new:
        subject_fields |= {RC_FIRSTNAME_FIELD, RC_LASTNAME_FIELD, RC_DOB_FIELD}

    rc_subjects = {}
    found_fields = set()
    try:
        for df in redcap_dfs.values():
            for field in subject_fields:
                if field in df:
                    for r in df[["subject", field]].to_records(index=False):
                        rc_subjects.setdefault(r[0], {})[field] = r[1]
                    found_fields.add(field)
                    if found_fields == subject_fields:
                        raise StopIteration
    except StopIteration:
        pass

    assert found_fields == subject_fields, (
        "We can't use the BRP-eHB API without the right REDCap enrollment"
        " fields. Are these correct?\n"
        f"\t{subject_fields}"
    )

    brp = BRP(brp_api_url, brp_token)

    ehb_subjects = {
        (bs["organization"], bs["organization_subject_id"]): bs["id"]
        for bs in brp.get_subjects(brp_protocol)
    }

    # Build mapping from redcap subject to CID
    CID_map = {}
    for subject, r in rc_subjects.items():
        ident = (int(r.get(RC_ORG_FIELD, RC_ORG_OVERRIDE)), r.get(RC_ORG_ID_FIELD))
        if (None not in ident) and (
            r.get(f"{RC_ENROLLMENT_FORM}_complete") == "Complete"
        ):
            if ident in ehb_subjects:  # Subject already in BRP-eHB
                print(f'Subject {subject} already in BRP-eHB')
                id = ehb_subjects[ident]
                CID_map[subject] = f"C{CID_MAGIC_NUMBER*int(id)}"
            elif create_if_new:  # Subject not yet in BRP-eHB -> submit to BRP-eHB
                print(f'Submitting subject {subject} to BRP-eHB... ⏳')
                created = brp.create_subject(
                    brp_protocol,
                    int(r.get(RC_ORG_FIELD, RC_ORG_OVERRIDE)),
                    r.get(RC_ORG_ID_FIELD),
                    r.get(RC_FIRSTNAME_FIELD),
                    r.get(RC_LASTNAME_FIELD),
                    r.get(RC_DOB_FIELD),
                )
                created = created["response"]
                if created[0]:
                    id = created[1]["id"]
                    CID_map[subject] = f"C{CID_MAGIC_NUMBER*int(id)}"
                else:
                    print("Error?", vars(created))
            else:
                print(f'Subject {subject} not found in BRP-eHB. Subject will not be warehoused.')

        else:
            print(f'SUBJECT {subject} ENROLLMENT NOT COMPLETE')

    # Map subject to CID
    for df in redcap_dfs.values():
        df["CID"] = df["subject"].map(CID_map)
        # Remove subjects without CIDs
        df.dropna(subset=["CID"], inplace=True)


def redcap_safe_dates(redcap_dfs, date_fields):
    """For subjects younger than 90, extract just years from REDCap DataFrame
    date fields and also convert to ages in days using enrollment DOB."""
    dob_df = None
    for df in redcap_dfs.values():
        if RC_DOB_FIELD in df:
            dob_df = df
            break

    dobs = {
        e["subject"]: to_datetime(e[RC_DOB_FIELD], errors="coerce")
        for e in dob_df[["subject", RC_DOB_FIELD]].to_dict(orient="records")
    }

    def date_to_age_days(birthdate, date):
        days = (date - birthdate).days
        if not isnan(days):
            return f"{days} days"
        else:
            return None

    def discard_if_too_old(birthdate, x):
        if relativedelta.relativedelta(datetime.now(), birthdate).years < 90:
            return x
        else:
            return None

    for df in redcap_dfs.values():
        for f in date_fields:
            if f in df:
                df[f] = to_datetime(df[f], errors="coerce")
                df[f] = df.apply(
                    lambda r: discard_if_too_old(dobs[r["subject"]], r[f]),
                    axis=1,
                )
                df[f + "_year"] = df[f].apply(lambda x: x.year)
                df[f + "_as_age"] = df.apply(
                    lambda r: date_to_age_days(dobs[r["subject"]], r[f]), axis=1
                )


def submit_to_warehouse(warehouse_url, schema_name, dfs):
    """Send our DataFrames to the warehouse DB"""
    db_engine = create_engine(warehouse_url)

    if not db_engine.dialect.has_schema(db_engine, schema_name):
        # requires schema creation privilege
        db_engine.execute(schema.CreateSchema(schema_name))

    for name, df in dfs.items():
        df.to_sql(
            name,
            db_engine,
            index=False,
            if_exists="replace",
            schema=schema_name,
            method="multi",
            chunksize=10000,
        )


if __name__ == "__main__":

    class MyParser(argparse.ArgumentParser):
        def error(self, message):
            sys.stderr.write(f"error: {message}\n\n")
            self.print_help()
            sys.exit(2)

    parser = MyParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    # required arguments
    parser.add_argument(
        "redcap_token_env_key",
        help=(
            'Environment key storing the REDCap study API token (e.g. "REDCAP_TOKEN_27084")'
        ),
    )
    parser.add_argument(
        "brp_token_env_key",
        help='Environment key storing the BRP API token (e.g. "BRP_TOKEN")',
    )
    parser.add_argument(
        "brp_protocol", help="BRP protocol number for the study"
    )
    parser.add_argument(
        "cid_magic_number_env_key",
        help="Environment key storing magic number for generating CIDs",
    )
    parser.add_argument(
        "warehouse_url_env_key",
        help='Environment key storing authenticated warehouse url (e.g. "D3B_WAREHOUSE_DB_URL")',
    )

    # optional arguments
    parser.add_argument(
        "--redcap_api_url",
        required=False,
        default="https://redcap-api.chop.edu/api/",
        help=(
            "REDCap API url. redcap-api.chop.edu only"
            " works inside the CHOP network, but"
            " redcap.chop.edu is less reliable."
        ),
    )
    parser.add_argument(
        "--brp_api_url",
        required=False,
        default="https://brp.research.chop.edu/api/",
        help="BRP API url",
    )
    parser.add_argument(
        "--redcap_enrollment_form",
        required=False,
        default=RC_ENROLLMENT_FORM,
        help="REDCap form that contains subject enrollment details",
    )
    parser.add_argument(
        "--redcap_firstname_field",
        required=False,
        default=RC_FIRSTNAME_FIELD,
        help="REDCap enrollment field that contains subject's first name",
    )
    parser.add_argument(
        "--redcap_lastname_field",
        required=False,
        default=RC_LASTNAME_FIELD,
        help="REDCap enrollment field that contains subject's last name",
    )
    parser.add_argument(
        "--redcap_dob_field",
        required=False,
        default=RC_DOB_FIELD,
        help="REDCap enrollment field that contains subject's date of birth",
    )
    parser.add_argument(
        "--redcap_organization_field",
        required=False,
        default=RC_ORG_FIELD,
        help="REDCap enrollment field that contains the subject's identifying organization",
    )
    parser.add_argument(
        "--redcap_organization_override_value",
        required=False,
        help="BRP-eHB code for the identifying organization if not present in REDCap",
    )
    parser.add_argument(
        "--redcap_id_within_organization_field",
        required=False,
        default=RC_ORG_ID_FIELD,
        help="REDCap enrollment field that contains the subject's identifier within the identifying organization",
    )
    parser.add_argument(
        "--redact_redcap_field",
        required=False,
        action="append",
        help="Redact this REDCap field (this argument is repeatable)",
    )
    parser.add_argument(
        "--only_warehouse_if_CID_already_exists",
        required=False,
        action="store_true",
        help="Only warehouse subjects that already have CIDs",
    )

    args = parser.parse_args()

    create_if_new = not args.only_warehouse_if_CID_already_exists
    redcap_api_url = args.redcap_api_url
    redcap_token = os.getenv(args.redcap_token_env_key)
    brp_api_url = args.brp_api_url
    brp_token = os.getenv(args.brp_token_env_key)
    brp_protocol = args.brp_protocol
    CID_MAGIC_NUMBER = int(os.getenv(args.cid_magic_number_env_key))
    warehouse_url = os.getenv(args.warehouse_url_env_key)

    RC_ENROLLMENT_FORM = args.redcap_enrollment_form
    RC_FIRSTNAME_FIELD = args.redcap_firstname_field
    RC_LASTNAME_FIELD = args.redcap_lastname_field
    RC_DOB_FIELD = args.redcap_dob_field
    RC_ORG_FIELD = args.redcap_organization_field
    if args.redcap_organization_override_value is not None:
        RC_ORG_OVERRIDE = int(args.redcap_organization_override_value)
    RC_ORG_ID_FIELD = args.redcap_id_within_organization_field

    # read from redcap

    rs = REDCapStudy(redcap_api_url, redcap_token)
    records_tree, errors = rs.get_records_tree()

    if errors:
        print(errors)
        sys.exit()

    redcap_dfs = all_dfs(records_tree)

    # de-identify and redact
    data_dictionary = rs.get_data_dictionary()

    # The BRP wants raw org values, not readable ones, so we need to swap those.
    raw2org = rs.get_selector_choice_map().get(RC_ORG_FIELD, {})
    org2raw = {v: k for k, v in raw2org.items()}
    if org2raw:
        for df in redcap_dfs.values():
            if RC_ORG_FIELD in df:
                df[RC_ORG_FIELD] = df[RC_ORG_FIELD].map(org2raw)

    # Send new subjects to the BRP to get their CIDs.
    redcap_subjects_to_CIDs(
        redcap_dfs, brp_api_url, brp_token, brp_protocol, create_if_new=create_if_new
    )

    # Now swap the orgs back in case we change our mind about redacting them later.
    if raw2org:
        for df in redcap_dfs.values():
            if RC_ORG_FIELD in df:
                df[RC_ORG_FIELD] = df[RC_ORG_FIELD].map(raw2org)

    identifier_fields = [
        f["field_name"] for f in data_dictionary if f["identifier"]
    ]

    date_fields = [
        d["field_name"]
        for d in data_dictionary
        if "date" in d["text_validation_type_or_show_slider_number"]
    ]

    note_fields = [
        d["field_name"] for d in data_dictionary if d["field_type"] == "notes"
    ]

    redcap_safe_dates(redcap_dfs, date_fields)

    fields_to_redact = set(
        identifier_fields
        + date_fields
        + note_fields
        + (args.redact_redcap_field or [])
        + [
            RC_FIRSTNAME_FIELD,
            RC_LASTNAME_FIELD,
            RC_DOB_FIELD,
            RC_ORG_ID_FIELD,
            RC_ORG_FIELD,
        ]
    )

    redactions = []
    for field in fields_to_redact:
        for instrument in redcap_dfs:
            if field in redcap_dfs[instrument]:
                redactions.append(f"Redacting {instrument}.{field}")
                del redcap_dfs[instrument][field]

    for red in sorted(redactions):
        print(red)

    for k, df in redcap_dfs.items():
        redcap_dfs[k] = df.where(notnull(df), None).convert_dtypes()

    # submit data to warehouse

    project_info = rs.get_project_info()
    db_schema_name = f"redcap_{project_info['project_id']}"
    redcap_dfs["redcap_project_info"] = DataFrame.from_dict([project_info])
    submit_to_warehouse(warehouse_url, db_schema_name, redcap_dfs)
