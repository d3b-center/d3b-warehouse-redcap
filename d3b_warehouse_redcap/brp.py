from requests.exceptions import HTTPError
from d3b_utils.requests_retry import Session, BetterHTTPError


class BRP:
    def __init__(self, api_url, api_token):
        self.api_url = api_url
        self.api_token = api_token

    def request(self, type, endpoint, json=None):
        resp = Session(status_forcelist=(502, 503, 504)).request(
            type,
            f'{self.api_url.rstrip("/")}/{endpoint.lstrip("/")}',
            json=json,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"token {self.api_token}",
            },
        )

        try:
            resp.raise_for_status()
        except HTTPError as e:
            raise BetterHTTPError(e).with_traceback(e.__traceback__) from None

        return resp.json()

    def get_subjects(self, protocol):
        resp = self.request("GET", f"protocols/{protocol}/subjects/")
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
                "POST",
                f"protocols/{protocol}/subjects/create",
                json=data,
            )

        return {"sent": data, "response": resp}
