import requests
from session import requests_retry_session
import pprint

BRP_API = 'https://brp.research.chop.edu/api/'


class RequestException(Exception):
    def __init__(self, msg, req, resp):
        super().__init__(
            '\n\n' + msg + '\n' + pprint.pformat({
                "sent": vars(req), "received": vars(resp)
            })
        )


class BRP:
    def __init__(self, api_token):
        self.api_token = api_token

    def request(self, type, endpoint, json=None):
        while endpoint[0] == '/':
            endpoint = endpoint[1:]
        req = requests.Request(
            type,
            BRP_API + endpoint,
            json=json,
            headers={
                'Content-Type': 'application/json',
                'Authorization': 'token ' + str(self.api_token)
            }
        ).prepare()
        resp = requests_retry_session().send(req)
        if resp.status_code not in {200, 201}:
            raise RequestException('Status not 200/201', req, resp)
        return resp.json()

    def get_subjects(self, protocol):
        resp = self.request('get', 'protocols/' + str(protocol) + '/subjects/')
        return resp

    def create_subject(
        self, protocol,
        organization, organization_subject_id, first_name, last_name, dob
    ):
        data = {
            'first_name': first_name,
            'last_name': last_name,
            'organization_subject_id': organization_subject_id,
            'dob': dob,
            'organization': organization
        }

        resp = None
        for v in data.values():
            if not v:
                resp = [
                    False, {}, ['This subject is missing required values.']
                ]

        if not resp:
            resp = self.request(
                'post', 'protocols/' + str(protocol) + '/subjects/create',
                json=data
            )

        return {'sent': data, 'response': resp}
