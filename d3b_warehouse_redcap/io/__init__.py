from __future__ import annotations
import logging
import requests
import json
from pprint import pformat

TIMEOUT_INFINITY = -1


def send_request(
    method: str,
    *args: any,
    ignore_status_codes: list[str] = None,
    timeout=TIMEOUT_INFINITY,
    **kwargs: any,
) -> requests.Response:
    """
    Send http request. Ignore any status codes that the user provides. Any
    other status codes >300 will result in an HTTPError

    Arguments:
        method: name of HTTP request method (i.e. get, post, put)
        *args: positional arguments passed to request method
        ignore_status_codes: list of HTTP status codes to ignore in the
        response
        **kwargs:

    Returns:
        Resonse object from HTTP method called by requests

    Raises:
        requests.exceptions.HTTPError
    """
    if isinstance(ignore_status_codes, str):
        ignore_status_codes = [ignore_status_codes]

    # NOTE: Set timeout so requests don't hang
    # See https://requests.readthedocs.io/en/latest/user/advanced/#timeouts
    if not timeout:
        # connect timeout, read timeout
        kwargs["timeout"] = (6.05, 120)

    # If timeout is negative, set to None so there is no timeout limit
    elif timeout == TIMEOUT_INFINITY:
        kwargs["timeout"] = None

    logging.info(
        "⌚️ Applying timeout: %s (connect, read)" " seconds to request", timeout
    )

    # Get http method
    requests_op = getattr(requests, method.lower())
    status_code = 0
    try:
        resp = requests_op(*args, **kwargs)
        status_code = resp.status_code
        resp.raise_for_status()
    except requests.exceptions.HTTPError as e:
        # User said to ignore this status code so pass
        kwargs["headers"]["Authorization"] = "Token ****"

        if ignore_status_codes and (status_code in ignore_status_codes):
            pass
        # Error that we need to log and raise
        else:
            body = "No request body found"
            try:
                body = pformat(resp.json())
            except json.JSONDecodeError:
                body = resp.text

            raise requests.exceptions.HTTPError(
                f"❌ Problem sending {method} request to server\n"
                f"{str(e)}\n"
                f"args: {args}\n"
                f"kwargs: {pformat(kwargs)}\n"
                f"{body}\n"
            ) from e

    return resp
