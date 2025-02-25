import logging
import json
from pprint import pformat
from d3b_warehouse_redcap.io import send_request


class BRP:
    def __init__(self, api_url, api_token):
        self.api_url = api_url
        self.api_token = api_token

    def __request(self, method, endpoint, body=None):
        """
        Returns a JSON object of the HTTP request result.

            Parameters:
                method (str): Method for the request object
                endpoint (str): Endpoint for the request object
            Returns:
                resp (object): Response object
        """
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"token {self.auth_token}",
        }

        resp = send_request(
            method,
            f"{self.api_url.rstrip('/')}/{endpoint.lstrip('/')}",
            headers=headers,
            json=body,
            timeout=120,
            ignore_status_codes=[SUBJECT_ALREADY_EXISTS_ERROR_CODE],
        )
        try:
            body = pformat(resp.json())
        except json.JSONDecodeError:
            body = resp.text

        # log body and status code, http method
        logging.info(
            "Response from BRP %s %s: %s",
            resp.request.method,
            resp.status_code,
            body,
        )
        return resp

    def get_subjects(self, protocol_id):
        """
        Returns a list of subjects within a specific protocol.

            Parameters:
                protocol_id (int): Protocol ID
            Returns:
                subject (list): Subjects associated with the protocol ID
        """
        try:
            subjects = self.__request(
                "GET", f"/protocols/{protocol_id}/subjects/"
            ).json()
        except json.JSONDecodeError:
            subjects = None
        return subjects

    def create_subject(
        self,
        protocol_id,
        first_name,
        last_name,
        organization_subject_id,
        dob,
        organization,
    ):
        """
        Creates a new subject and adds them to a protocol.

            Parameters:
                protocol_id (int): Protocol ID
                first_name (str):
                last_name (str):
                organization_subject_id (str):
                dob (str):
                organization (str):
            Returns:
                created (bool):
                body (dict):
                _ (list): Placeholder
        """
        data = {
            "first_name": first_name,
            "last_name": last_name,
            "organization_subject_id": organization_subject_id,
            "dob": dob,
            "organization": organization,
        }

        try:
            brp_response_raw = self.__request(
                "POST", f"/protocols/{protocol_id}/subjects/create", data
            ).json()
            created, body, message = extract_brp_create_subj_response(
                brp_response_raw
            )
        except json.JSONDecodeError:
            created = False
            body = {}
            message = []
        return {"sent": data, "response": [created, body, message]}


def extract_brp_create_subj_response(response: dict) -> tuple:
    if (
        isinstance(response, list)
        and len(response) == 3
        and isinstance(response[0], bool)
        and isinstance(response[1], dict)
        and isinstance(response[2], list)
    ):
        created, body, message = response
        logging.info(
            " Create Subject matches expected structure, %s %s %s",
            created,
            pformat(body),
            pformat(message),
        )
        return created, body, message

    logging.warning(
        "Create subject response doesn't match expected structure %s",
        pformat(response),
    )
    return False, {}, []
