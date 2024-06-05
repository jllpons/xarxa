#!/usr/bin/env python3

"""
Given a KEGG organism code:
1. Fetches the KGML file for each pathway belonging to the organism.
2. Extracts all the relations for each pathway.

Output Format:
- Generates a tab-separated file including:
  1. Source (KEGG accession)
  2. Target (KEGG accession)
  3. Pathway (KEGG pathway)
  4. Relation type
  5. Relation subtypes (if multiple, separated by ';')
  6. Relation subtype values (if multiple, separated by ';')
"""

import argparse
import logging
import sys
import time
from typing import List, Tuple

from requests.exceptions import RequestException
from xml.etree import ElementTree

from lib.api_url import KEGG_API
from lib.cli import (
        CustomHelpFormatter,
        setup_logger,
        )
from lib.generic_row import GenericRow
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
        usage="%(prog)s [options] <kegg_organism>",
        description=__doc__,
        add_help=False,
        epilog="""
Examples:
  Process a file:
    $ %(prog)s sml

  Save the output to a file:
    $ %(prog)s sml > output.tsv

For more detailed information, see the 'docs' directory.
    """,
    )

    parser._positionals.title = "Arguments"
    parser.add_argument("organism",
                        metavar="<kegg_organism>",
                        type=str,
                        help="The KEGG organism code.")

    parser._optionals.title = "Options"
    parser.add_argument("-h", "--help",
                        action="help",
                        default=argparse.SUPPRESS,
                        help="Display this help message and exit.")
    parser.add_argument("--log",
                        metavar="<level>",
                        type=str,
                        default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        help="Set the logging level (INFO by default).")

    return parser


def setup_config() -> Tuple[argparse.Namespace, logging.Logger]:
    """
    Setup the configuration for the script.

    Parameters
        None

    Returns
        Tuple[argparse.Namespace, logging.Logger]: A tuple containing the command line arguments and the logger.
    """

    parser = setup_argparse()
    args = parser.parse_args()

    logger = setup_logger(args.log)

    if len(sys.argv) == 1 and sys.stdin.isatty():
        parser.print_help()
        sys.exit(1)

    logger.info(f"Arguments: {vars(args)}")

    return args, logger


def fetch_pathways_data(organism: str) -> str:
    """
    Fetches the list of pathways for a given organism.

    Parameters:
        organism (str): KEGG organism code to fetch the pathways for

    Returns:
        str: the file returned by the kegg api (as it is)

    Raises:
        RequestException: If an excection is raised during the data retreaving process
        ValueError: If the response from the KEGG API is empty
    """

    query = f"/list/pathway/{organism}"
    url = KEGG_API.format(query=query)
    data = fetch_data_from_url_api(url, "all KEGG entries")

    if not data:
        raise ValueError

    return data


def format_pathway_data(data: str) -> List[str]:
    """
    Extracts the pathways from the file returned by the KEGG API

    Parameters:
        data (str): the file returned by the kegg api (as it is)

    Returns:
        List[str]: a list of KEGG pathways codes found in the input file
    """

    pathways = []
    for line in data.splitlines():
        pathways.append(line.split('\t')[0])

    return pathways


def extract_relations(pathway: str, logger: logging.Logger) -> List[GenericRow]:

    try:
        kgml_str = fetch_kgml(pathway)
        time.sleep(0.4) # Be nice to the KEGG server
    except RequestException as e:
        logger.warning(f"Failed to fetch KGML file for pathway {pathway}: {e}")
        raise e

    kgml_root = ElementTree.fromstring(kgml_str)

    relations = extract_relations_from_kgml(kgml_root, logger)

    return relations


def fetch_kgml(pathway: str) -> str:
    """
    Access the KEGG API to get the KGML for a given pathway.
    Returns the KGML as a string.

    Parameters
        pathway: (str) The KEGG pathway ID.

    Returns
        str: The KGML for the given pathway as a raw string.

    Raises
        RequestException: If the request to the KEGG API fails.
    """

    url = KEGG_API.format(query=f"/get/{pathway}/kgml")

    return fetch_data_from_url_api(url, f"KEGG KGML for pathway {pathway}")


def extract_relations_from_kgml(root: ElementTree.Element,
                               logger: logging.Logger
                               ) -> List[GenericRow]:
    """
    Parse a KGML file and extract the relations:

    Parameters:
        kgml_str (str): KGML string
        logger (logging.Logger): The logger object

    Returns:
        List[KeggRelationsRow]: A list of KeggRelationsRow objects
    """


    pathway = root.get("name")
    logger.debug(f"Extracting relations in KGML file for pathway {pathway}")


    # Hashmap with the entry ID as the key and the entry name as the value
    # So we can avoid multiple iterations over the entry tags
    entry_map = mk_entry_map(root)

    # Here we will store the relations that involve the query accession number
    relations = []
    for relation in root.findall("relation"):
        entry1_id, entry2_id = relation.get("entry1"), relation.get("entry2")
        entry1_name, entry2_name = entry_map.get(entry1_id), entry_map.get(entry2_id)


        # Prepare relation details
        rel_type = relation.get("type")
        # Get the subtype names and values for this relation
        subtypes = [(subtype.get("name"), subtype.get("value")) for subtype in relation.findall("subtype")]
        # By default, the subtype names and values are empty
        subtype_names, subtype_values = [], []
        if subtypes:
            for name, value in subtypes:
                # The name is as it appears in the name attribute
                subtype_names.append(name)
                # But the value attribute in the subtype tag is an entry ID
                # So we need to get the name of the entry
                if name == "compound":
                    subtype_values.append(entry_map.get(value))
                else:
                    subtype_values.append(value)

        for source in str(entry1_name).split(" "):
            for target in str(entry2_name).split(" "):

                # Append to relations list
                relation = GenericRow(
                    source=source,
                    target=target,
                    pathway=pathway,
                    relation_type=rel_type,
                    relation_subtype_names=subtype_names,
                    relation_subtype_values=subtype_values
                    )
                logger.debug(f"Successfully extracted relation for {source} in KGML file for pathway {pathway}")
                logger.debug([attr for attr in relation.__dict__.items() if not attr[0].startswith("_")])

                relations.append(relation)

    return relations


def mk_entry_map(root: ElementTree.Element) -> dict:
    """
    Helper function for `extract_relations_from_kgml` to create a hashmap with the entry ID as the key
    and the entry name as the value.

    Parameters
        root (ET.Element): The root element of the KGML file.

    Returns
        dict: A hashmap with the entry ID as the key and the entry name as the value.
    """

    return {entry.get("id"): entry.get("name") for entry in root.findall("entry")}


def main():

    args, logger = setup_config()

    try:
        organism_pathways_file = fetch_pathways_data(args.organism)
    except RequestException as e:
        logger.error(f"Failed to fetch pathways for organism {args.organism}: {e}")
        sys.exit(1)
    except ValueError:
        logger.error("KEGG response is empty")
        sys.exit(1)

    pathways = format_pathway_data(organism_pathways_file)

    relations = []
    for index, pathway in enumerate(pathways):

        if index % 100 == 0:
            logger.info(f"{index}/{len(pathways)} pathways processed")

        pathway_relations = extract_relations(pathway, logger)

        relations.extend(pathway_relations)

    logger.info(f"Succesfully fetched {len(relations)} relations")

    logger.info("Writing to stdout...")
    for relation in relations:
        try:
            print(relation, flush=True)
        except BrokenPipeError:
            sys.stdout = None
            logger.error("Pipe was broken. Terminating...")
            sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
