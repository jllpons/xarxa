#!/usr/bin/env python3

"""
This module contains functions to fetch and handle data from different database
APIs.
"""


import logging

import requests
from requests.adapters import HTTPAdapter, Retry


logger = logging.getLogger(__name__)


def make_request_with_retries(url: str,
                              method: str = "GET",
                              retries: int = 3,
                              delay: float = 1,
                              timeout: int = 10,
                              **kwargs) -> requests.Response:
        """
        Make a request to a given URL with retries in case of failure.

        Parameters:
            url (str): The URL to send the request to.
            method (str): The request method (default: "GET").
            retries (int): The maximum number of retries in case of failure (default: 3).
            delay (int): The delay in seconds between retries. Increses exponentially.(default: 1).
            timeout (int): The time in seconds to wait for a response (default: 10).
            **kwargs: Additional keyword arguments to be passed to the request method.
                      They are passed directly to the `request` method to allow for
                      additional flexibility (e.g. params for GET, json for POST...).

        Returns:
            requests.Response: The response object.

        Raises:
            ValueError: If the request method is not "GET" or "POST".

        Note:
            The exponential backoff factor determines the delay between retries.
            The delay formula is (2 ^ retry_number) * backoff_factor,
            making the delay increase exponentially with each retry.
        """

        # A retry is initiated if the request method is in `allowed_methods`
        # and the response status code is in `status_forcelist`
        retry_strategy = Retry(total=retries,
                               status_forcelist=[429, # Too Many Requests
                                                 500, # Internal Server Error
                                                 502, # Bad Gateway
                                                 503, # Service Temporarily Unavailable
                                                 504], # Gateway Timeout
                               allowed_methods=["GET", "POST"],
                               backoff_factor=delay)

        adapter = HTTPAdapter(max_retries=retry_strategy)
        http = requests.Session()
        http.mount("https://", adapter)


        if method == "GET":
            response = http.get(url, timeout=timeout, **kwargs)
        elif method == "POST":
            response = http.post(url, timeout=timeout)
        else:
            raise ValueError()

        return response


def fetch_data_from_url_api(url: str, log_context: str) -> str:
    """
    Fetch data from a given API using the provided URL.

    Args:
        url: The URL of the API endpoint.
        log_context: The context to be used in the log message.

    Returns:
        The data fetched from the API endpoint.

    Raises:
        requests.exceptions.RequestException: If the request to the API fails.
    """

    logger.debug(f"Sending request for {log_context}. URL: '{url}'")
    response = make_request_with_retries(url)

    if response.status_code != 200:
        logger.error(f"Request for {log_context} failed with status code {response.status_code}")
        raise requests.exceptions.RequestException

    logger.debug(f"Request for {log_context} successful")

    data = response.text
    nrows = data.count("\n")
    logger.debug(f"Received {nrows} rows of data for {log_context}")

    return data

