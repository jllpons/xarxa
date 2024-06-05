#!/usr/bin/env python

"""
Fetches all UniProt entries for a specified taxonomy ID.

Output Format:
- Generates a JSON output including all entries for the given taxonomy ID.

Usage:
- Direct output to a file for further processing:
    $ python script_name.py <taxid> > uniprot_data.json
"""

import argparse
import logging
import re
import sys
import requests
from requests.adapters import HTTPAdapter, Retry
from typing import Generator, Tuple

from lib.cli import CustomHelpFormatter, setup_logger

# Setup regular expression for extracting the next link from headers
re_next_link = re.compile(r'<(.+)>; rel="next"')

# Configure retries for the requests session
retries = Retry(total=5, backoff_factor=0.25, status_forcelist=[500, 502, 503, 504])
session = requests.Session()
session.mount("https://", HTTPAdapter(max_retries=retries))


def setup_argparse() -> argparse.ArgumentParser:
    """
    Creates a custom ArgumentParser instance and sets up the command line arguments.

    Returns:
        argparse.ArgumentParser: The ArgumentParser instance with the command line arguments.
    """
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=CustomHelpFormatter,
    )

    parser.add_argument("taxid",
                        metavar="<taxid>",
                        type=str,
                        help="Taxonomy ID for which to fetch UniProt data.")

    parser.add_argument("--log",
                        metavar="STR",
                        type=str,
                        default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        help="Set the logging level (default: INFO).")

    return parser


def get_next_link(headers: dict) -> str | None:
    """
    Extracts the next link from the response headers if present.

    Args:
        headers (dict): Response headers.

    Returns:
        str: The next link URL or None if not present.
    """
    if "Link" in headers:
        match = re_next_link.match(headers["Link"])
        if match:
            return match.group(1)
    return None


def get_batch(batch_url: str) -> Generator[Tuple[requests.Response, int], None, None]:
    """
    Fetches data from the batch URL, handling pagination.

    Args:
        batch_url (str): The initial URL to fetch data from.

    Yields:
        Tuple[requests.Response, int]: The response and total number of results.
    """
    while batch_url:
        response = session.get(batch_url)
        response.raise_for_status()
        total = int(response.headers.get("x-total-results", 0))
        yield response, total
        batch_url = get_next_link(response.headers)


def main():
    """
    The main function that sets up argument parsing, logging, and fetches data.
    """
    parser = setup_argparse()
    args = parser.parse_args()

    logger = setup_logger(args.log)
    logger.info(f"Arguments: {vars(args)}")
    logger.info(f"Fetching UniProt data for taxonomy ID: {args.taxid}")

    url = f"https://rest.uniprot.org/uniprotkb/search?format=json&query=%28%28taxonomy_id%3A{args.taxid}%29%29&size=500"

    try:
        progress = 0
        for batch, total in get_batch(url):
            lines = batch.text.splitlines()
            if not progress:
                print(lines[0], flush=True)  # Print the header
            for line in lines[1:]:
                print(line, flush=True)  # Print each entry
            progress += len(lines) - 1
    except requests.RequestException as e:
        logger.error(f"Failed to fetch data: {e}")
        sys.exit(1)

    logger.info(f"Finished fetching UniProt data for taxonomy ID: {args.taxid}")


if __name__ == "__main__":
    main()

