from session import requests_retry_session

BRP_API = 'https://brp.research.chop.edu/api/'


class BRP:
    def __init__(self, api_token):
        self.api_token = api_token

    def request(self, type, endpoint, json=None):
        while endpoint[0] == '/':
            endpoint = endpoint[1:]
        return requests_retry_session().request(
            type,
            BRP_API + endpoint,
            json=json,
            headers={
                'Content-Type': 'application/json',
                'Authorization': 'token ' + self.api_token
            }
        ).json()

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
