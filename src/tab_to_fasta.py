#!/usr/bin/env python3

"""
Parse a tab-delimited containing identifiers and sequences and output each record as a FASTA entry.

Input:
- A tab-delimited file containing two fields:
    1. Identifier
    2. Sequence

Output:
- A FASTA file containing the same records.
>Identifier
Sequence

If an identifier or a sequence is missing or is 'NULL', the record is skipped.
"""

import argparse
import sys

from lib.cli import CustomHelpFormatter, setup_logger, read_input


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
        usage="%(prog)s [options] <file>",
        add_help=False,
        description=__doc__,
        epilog="""
Examples:

  Parse a tab-delimited file and output a FASTA file:
    $ %(prog)s input.tsv > output.fasta

  Parse a tab-delimited file from standard input and output a FASTA file:
    $ cat input.tsv | %(prog)s > output.fasta

  Print values from a table in the database and generate a FASTA file:
    $ src/printval.py genbank_refseq composite_id,translation | %(prog)s > output.fasta

For more detailed information, see: <TODO: link to documentation>
""",
    )


    parser._positionals.title = "Arguments"
    parser.add_argument("file",
                        metavar="<list>",
                        nargs="?",
                        type=str,
                        default="-",
                        help="file containing identifiers to convert or '-' to read from stdin. defaults to stdin if omitted.")


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


def main():

    parser = setup_argparse()
    args = parser.parse_args()

    if len(sys.argv) == 1 and sys.stdin.isatty():
        parser.print_help()
        sys.exit(1)

    logger = setup_logger(args.log)

    try:
        input_file = read_input(args.file)
    except FileNotFoundError:
        sys.exit(1)

    for line in input_file.splitlines():
        identifier, sequence = line.strip().split("\t")

        if identifier == "NULL" or sequence == "NULL":
            continue

        print(f">{identifier}\n{sequence}", flush=True)

if __name__ == "__main__":
    main()
