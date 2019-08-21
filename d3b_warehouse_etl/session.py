import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from urllib3 import connectionpool


# Taken from the kf ingest lib. We should have a central place for this.
def requests_retry_session(
        session=None, total=10, read=10, connect=10, status=10,
        backoff_factor=5, status_forcelist=(500, 502, 503, 504)
):
    """
    Send an http request and retry on failures or redirects

    See urllib3.Retry docs for details on all kwargs except `session`
    Modified source: https://www.peterbe.com/plog/best-practice-with-retries-with-requests # noqa E501

    :param session: the requests.Session to use
    :param total: total retry attempts
    :param read: total retries on read errors
    :param connect: total retries on connection errors
    :param status: total retries on bad status codes defined in
    `status_forcelist`
    :param backoff_factor: affects sleep time between retries
    :param status_forcelist: list of HTTP status codes that force retry
    """
    session = session or requests.Session()

    retry = Retry(
        total=total,
        read=read,
        connect=connect,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        method_whitelist=False
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    return session
