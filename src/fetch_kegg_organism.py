#!/usr/bin/env python

"""
Fetches all KEGG entries for a specified organism.

Output Format:
- Generates a tab-separated file including:
  1. KEGG Accession
  2. Pathways (associated with the entry, separated by a semicolon)
  3. KEGG Orthology (KO) numbers 
"""

import argparse
import logging
import sys
import time
from typing import List

from requests.exceptions import RequestException

from lib.api_url import KEGG_API
from lib.cli import CustomHelpFormatter, setup_logger
from lib.generic_row import GenericRow, parse_tsv
from lib.request_data import fetch_data_from_url_api


def setup_argparse() -> argparse.ArgumentParser:
    """
    Creates a custom ArgumentParser instance and sets up the command line
    arguments.

    Parameters
        None

    Returns
        argparse.ArgumentParser: The ArgumentParser instance with the command line arguments.
    """

    fmt = lambda prog: CustomHelpFormatter(prog)

    parser = argparse.ArgumentParser(
        formatter_class=fmt,
        add_help=False,
        usage="%(prog)s [options] <organism>",
        description=__doc__,
        epilog=r"""
Examples:
  Fetch KEGG data for a specific organism:
    $ python %(prog)s sml

  Direct output to a file for further processing:
    $ python %(prog)s sml > kegg_data.txt

  Chain fetching with data analysis:
    $ python %(prog)s sml | python parse_kegg_ids_data.py

For more information, see: <TODO: add link to documentation>
    """,)

    parser._positionals.title = "Arguments"
    parser.add_argument("organism",
                        metavar="<organism>",
                        type=str,
                        help="KEGG organism code for which to fetch data.")

    parser._optionals.title = "Options"
    parser.add_argument("-h", "--help",
                        action="help",
                        default=argparse.SUPPRESS,
                        help="Display this help message and exit.")
    parser.add_argument("--log",
                        metavar="STR",
                        type=str,
                        default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        help="Set the logging level (default: INFO).")

    return parser


def format_data(tab_data: str) -> List[GenericRow]:
    """
    Given a tab-separated string, this function will parse the data and
    structure it into a list of `GenericRow` structures.

    Parameters:
        tab_data (str): A tab-separated string containing the data.

    Returns:
        List[GenericRow]: A list of `GenericRow` structures containing the structured data. 

    Raises:
        ValueError: If the data does not match the expected schema.
    """

    schema = {
        "kegg_accession": str,
        "type": str,
        "position": str,
        "description": str,
    }

    sep1 = parse_tsv(tab_data, schema)

    records = []
    for row in sep1:
        records.append(
                GenericRow(kegg_accession=row.kegg_accession,
                    pathways=None,
                    ko=None
                )
        )

    return records


def add_pathway(rows: List[GenericRow],
                start: int,
                end: int,
                logger: logging.Logger) -> None:
    """
    This function adds pathway information to the records from `start` to `end`.

    Args:
        rows (List[KeggOrganismIDsRow]): The rows to add pathway information to.
        start (int): The index of the first row to add pathway information to.
        end (int): The index of the last row to add pathway information to.
        logger (logging.Logger): The logger instance to log messages to.

    Returns:
        None, records are updated by reference.
    """

    logger.debug(f"Adding pathway information to the rows from {start} to {end}")

    to_request = [i.kegg_accession for i in rows[start:end]]

    try:
        query = f"/link/pathway/{'+'.join(to_request)}"
        url = KEGG_API.format(query=query)
        response = fetch_data_from_url_api(url, "KEGG pathway information")
    except RequestException:
        logger.debug(f"Failed to fetch pathway information for KEGG accessions '{to_request}'")
        return None
    response_map = hashmap_from_kegg_tsv(response)

    for row in rows[start:end]:
        if row.kegg_accession in response_map:
            if row.pathways is None:
                row.pathways = response_map[row.kegg_accession]
            else:
                row.pathway.extend(response_map[row.kegg_accession])


def hashmap_from_kegg_tsv(data: str) -> dict:
    """
    This function processes the response from the KEGG API into a dictionary
    where the keys are the first column of the response and the values are
    lists of the second column of the response.

    Args:
        data: str
            The response from the KEGG API.

    Returns:
        dict
            A dictionary where the keys are the first column of the response
            and the values are lists of the second column of the response.
    """

    hashmap = {}

    for row in data.split("\n")[:-1]:
        values = row.split("\t")

        if not values[0] or not values[1]:
            continue

        if values[0] in hashmap and values[1] not in hashmap[values[0]]:
            hashmap[values[0]].append(values[1])
        else:
            hashmap[values[0]] = [values[1]]

    return hashmap


def add_ko(rows: List[GenericRow],
           start: int,
           end: int,
           logger: logging.Logger) -> None:
    """
    This function adds KO information to the records from `start` to `end`.

    Args:
        rows (List[KeggOrganismIDsRow]): The rows to add KO information to.
        start (int): The index of the first row to add KO information to.
        end (int): The index of the last row to add KO information to.
        logger (logging.Logger): The logger instance to log messages to.

    Returns:
        None
    """

    logger.debug(f"Adding KO information to the rows from {start} to {end}")

    to_request = [i.kegg_accession for i in rows[start:end]]

    try:
        query = f"/link/ko/{'+'.join(to_request)}"
        url = KEGG_API.format(query=query)
        response = fetch_data_from_url_api(url, "KEGG KO information")
    except RequestException:
        logger.debug(f"Failed to fetch KO information for KEGG accessions '{to_request}'")
        return None
    response_map = hashmap_from_kegg_tsv(response)

    for row in rows[start:end]:
        if row.kegg_accession in response_map:
            if row.ko is None:
                row.ko = response_map[row.kegg_accession]
            else:
                row.ko.extend(response_map[row.kegg_accession])


def main():

    parser = setup_argparse()
    args = parser.parse_args(args=None if sys.argv[1:] else ['--help'])

    logger = setup_logger(args.log)


    logger.info(f"Arguments: {vars(args)}")
    logger.info(f"Fetching KEGG information for organism: {args.organism}")
    try:
        query = f"/list/{args.organism}"
        url = KEGG_API.format(query=query)
        data = fetch_data_from_url_api(url, "all KEGG entries")
    except RequestException:
        sys.exit(1)

    records = format_data(data)

    n_records = len(records)
    logger.info(f"Fetching KEGG Orthology data for {n_records} entries. It may take a while...")
    for i in range(0, n_records, 10):

        if (i % 1000) == 0:
            logger.info(f"Processed {i} of {n_records} entries")

        try:
            add_pathway(records, i, i+10, logger)
            time.sleep(0.5) # be nice to the server

        except RequestException:
            continue

        try:
            add_ko(records, i,  i+10, logger)
            time.sleep(0.5)

        except RequestException:
            continue

    logger.info(f"Finished fetching KEGG data for organism: {args.organism}")

    logger.info("Printing to stdout...")
    try:
        for record in records:
            print(record, flush=True)
    except BrokenPipeError:
        sys.stdout = None
        logger.error("Broken Pipe Error caught. Terminating program.")
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
