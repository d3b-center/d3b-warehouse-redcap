import argparse
import os

from session import requests_retry_session

BRP_API = 'https://brp.research.chop.edu/api/'


def create_subject(
    brp_token,
    protocol, organization, organization_subject_id, first_name, last_name, dob
):
    data = {
        "first_name": first_name,
        "last_name": last_name,
        "organization_subject_id": organization_subject_id,
        "dob": dob,
        "organization": organization
    }

    resp = None
    for v in data.values():
        if not v:
            resp = [False, {}, ['This subject is missing required values.']]

    if not resp:
        resp = requests_retry_session().post(
            BRP_API + 'protocols/' + str(protocol) + '/subjects/create',
            headers={
                'Content-Type': 'application/json',
                'Authorization': 'token ' + brp_token
            },
            json=data
        ).json()

    return {"sent": data, "response": resp}
