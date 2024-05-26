#!/usr/bin/env python

"""
Print tables, columns or values from the database.

Specify the table name and columns to print and will connect to the database
and print the values of the columns in the table.

Beware data will be printed in tabular formatting. This means that, for example,
if a column is a list, it will be printed as a string separated by commas, and
NoneType values will be printed as 'NULL'.
"""

import argparse
import sys

from lib.cli import (
        CustomHelpFormatter,
        setup_logger,
        get_database_connection_string,
        )
from lib.db_operations import (
        connect_to_db,
        get_all_tables,
        get_table_columns,
        get_records,
        table_already_exists,
        )


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
        usage="%(prog)s [options] <table> <columns>",
        add_help=False,
        description=__doc__,
        epilog="""
Examples:

  Display all tables in the database:
    $ python %(prog)s list

  Display all columns from the 'uniprot_protein' table:
    $ print_values.py uniprot list

  Fetch all 'uniprot_accession' from 'uniprot_protein':
    $ python %(prog)s uniprot uniprot_accession

  Retrieve 'uniprot_accession', 'go_function', and 'go_process' from 'uniprot_protein':
    $ python %(prog)s uniprot uniprot_accession,go_term,keywords

For more detailed information, see the `docs` directory.
""",
    )


    parser._positionals.title = "Arguments"
    parser.add_argument(
            "table",
            metavar="<table>",
            type=str,
            help="Name of the database table to query. Use 'list' to display all tables.")
    parser.add_argument(
            "columns",
            metavar="<columns>",
            type=str,
            default="list",
            nargs="?",
            help="Specific columns to display. Use commas for separating multiple columns. Use 'list' to display all columns. ")

    parser._optionals.title = "Options"
    parser.add_argument("-h", "--help",
                        action="help",
                        default=argparse.SUPPRESS,
                        help="Display this help message and exit.")
    parser.add_argument("--db",
                        metavar="<conn>",
                        type=str,
                        help="Database connection string. Reads from 'config/database.yaml' if unspecified.")
    parser.add_argument("--log",
                        metavar="<level>",
                        type=str,
                        default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        help="Logging level (default: INFO).")

    return parser


def main():

    parser = setup_argparse()
    args = parser.parse_args(args=None if sys.argv[1:] else ["--help"])

    logger = setup_logger(args.log)
    logger.debug(f"Arguments: {vars(args)}")

    if not args.db:
        try:
            args.db = get_database_connection_string()
        except FileNotFoundError:
            sys.exit(1)

    conn = connect_to_db(args.db, quiet=True)

    if args.table == "list":
        all_tables = get_all_tables(conn)
        logger.info("Tables in the database:")
        print("\n".join([table for table in all_tables]))
        sys.exit(0)


    # Validate table
    if not table_already_exists(args.table, conn):
        logger.error(f"Table '{args.table}' does not exist.")
        sys.exit(1)


    all_columns = get_table_columns(args.table, conn)

    if args.columns == "list":
        logger.info(f"Columns in table '{args.table}':")
        print("\n".join([column for column in all_columns]))
        sys.exit(0)

    args.columns = args.columns.split(",")

    for column in args.columns:
        if column not in all_columns:
            logger.error(f"Column '{column}' does not exist in table '{args.table}'.")
            sys.exit(1)


    records = get_records(args.table, args.columns, conn)

    for record in records:
        print(record)

    conn.close()


if __name__ == "__main__":
    main()
