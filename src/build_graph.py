#!/usr/bin/nev python

"""
Given a list of  identifiers, this script will build a graph representing the
relationships where the identifiers are involved.

Relationships are defined by the following rules:

    1. If the query identifier acts as a source in any of the KEGG relationships,
       the relationship is added to the graph as a directed edge.
    2. If the query identifier is found as source in any of the STRING relationships,
       the combined score is higher than a given threshold and the relationship
       is not already present in the graph, the relationship is added to the graph
       as an undirected edge.

The degree of neighbors for which the graph is built can be controlled with the
'-d / --depth' option. By default, the graph will only contain the query identifier
and the relationships in which it is involved (depth=1). A depth of 2 will include
the second degree neighbors or, in other words, the neighbors of the neighbors of the
query identifier(s).

Supported identifiers are:
  - UniProt Accession
  - RefSeq Locus Tag
  - Locus Tag
  - KEGG Accession
  - RefSeq Protein ID
"""


import argparse
import logging
from typing import Tuple
import sys

import psycopg2

from lib.cli import (
    CustomHelpFormatter,
    setup_logger,
    get_database_connection_string,
    read_input
)
from lib.db_operations import connect_to_db
from lib.graph import run_build_graph

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

    # Main parser
    parser = argparse.ArgumentParser(
        formatter_class=fmt,
        usage="%(prog)s [options] <file>",
        add_help=False,
        description=__doc__,
        epilog="""
Examples:
    """)

    required = parser.add_argument_group("Arguments")
    required.add_argument("file",
                          metavar="<file>",
                          nargs="?",
                          type=str,
                          default="-",
                          help="File containing a list of identifiers, or '-' to read from stdin. Defaults to stdin if omitted.")

    parser._optionals.title = "Options"
    parser.add_argument("-d", "--depth",
                        metavar="<int>",
                        type=int,
                        default=1,
                        help="Depth of neighbors to include in the graph. Default: 1")
    parser.add_argument("-S", "--string-threshold",
                        metavar="<float>",
                        type=int,
                        default=800,
                        help="Threshold for the STRING combined score. Range: 0-1000. Default: 800")
    parser.add_argument("--db",
                        metavar="<conn>",
                        type=str,
                        help="Database connection string. Reads from 'config/database.yaml' if unspecified.")
    parser.add_argument("--log",
                        metavar="<level>",
                        type=str,
                        default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        help="Set the logging level. Default: INFO")
    parser.add_argument("-h", "--help",
                        action="help",
                        default=argparse.SUPPRESS,
                        help="Display this help message and exit.")

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

    if not args.db:
        try:
            args.db = get_database_connection_string()
        except (FileNotFoundError, KeyError) as e:
            raise e


    if args.string_threshold < 0 or args.string_threshold > 1000:
        logger.error("The STRING threshold must be within the range 0-1000")
        raise KeyError

    return args, logger



def main():

    try:
        args, logger = setup_config()
    except (FileNotFoundError, KeyError):
        sys.exit(1)


    logger.debug(f"Reading data from: {args.file}")
    try:
        in_data = read_input(args.file)
    except FileNotFoundError:
        sys.exit(1)


    id_list = in_data.splitlines()


    try:
        conn = connect_to_db(args.db)
    except psycopg2.Error:
        sys.exit(1)


    graph = run_build_graph(conn, id_list, args.depth, args.string_threshold)


    for relation in graph:
        print(relation)


if __name__ == "__main__":
    main()
