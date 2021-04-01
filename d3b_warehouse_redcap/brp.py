import requests
from d3b_utils.requests_retry import Session, BetterHTTPError
import pprint

BRP_API = "https://brp.research.chop.edu/api/"


class BRP:
    def __init__(self, api_token):
        self.api_token = api_token

    def request(self, type, endpoint, json=None):
        while endpoint[0] == "/":
            endpoint = endpoint[1:]
        req = requests.Request(
            type,
            BRP_API + endpoint,
            json=json,
            headers={
                "Content-Type": "application/json",
                "Authorization": "token " + str(self.api_token),
            },
        ).prepare()
        resp = Session(status_forcelist=(502, 503, 504)).send(req)

        try:
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise BetterHTTPError(e).with_traceback(e.__traceback__) from None

        return resp.json()

    def get_subjects(self, protocol):
        resp = self.request("get", "protocols/" + str(protocol) + "/subjects/")
        return resp

    def create_subject(
        self,
        protocol,
        organization,
        organization_subject_id,
        first_name,
        last_name,
        dob,
    ):
        data = {
            "first_name": first_name,
            "last_name": last_name,
            "organization_subject_id": organization_subject_id,
            "dob": dob,
            "organization": organization,
        }

        resp = None
        for v in data.values():
            if not v:
                resp = [False, {}, ["This subject is missing required values."]]

        if not resp:
            resp = self.request(
                "post",
                "protocols/" + str(protocol) + "/subjects/create",
                json=data,
            )

        return {"sent": data, "response": resp}
