#!/usr/bin/env python

"""
Read a TSV file, validate, and upsert the data into a table in a database.
Table is created if it does not exist.

TSV file format:
- The file may contain multiple columns, but no header. 
- Columns may have different values. The format of these values must be as follows:
  - If value is a string, integer, or float, it must be displayed as is.
  - If value is a list, it must be displayed as a string with the elements separated by semicolons.
  - If value is a dictionary, it must be displayed as a conventional JSON string.
  - If the value is None, empty, or missing, it must be displayed as 'NULL'.
"""

import argparse
import logging
import sys
from typing import Tuple

import psycopg2

from lib.cli import (
    CustomHelpFormatter,
    setup_logger,
    read_input,
    get_database_connection_string,
)
from lib.db_operations import connect_to_db


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
        usage="%(prog)s <table_type> [options]",
        add_help=False,
        description=__doc__,
        epilog="""
Examples:

  Upserting data from a file to the 'uniprot' table in the database:
    $ upsert_table.py uniprot data/uniprot.tsv
    """)

    parser._optionals.title = "Options"

    # Add subparsers for the different table types
    table_types = parser.add_subparsers(
                    title="Table Types",
                    dest="table_type",
                    metavar="<table_type>",
                    )


    table_types.add_parser("id_mapper",
                           help="",
                           usage="upsert_table.py id_mapper [options] <file>",
                           description="""
The 'id_mapper' table allows for the mapping of different identifiers.

Input Format:
  - The data file should be tab-separated with the following columns:
    1. UniProt Accession
    2. RefSeq Locus Tag
    3. Locus Tag
    4. KEGG Accession
    5. RefSeq Protein ID
                            """,
                           formatter_class=fmt,
                           epilog="""
Example:

    Upserting data from a file:
    $ upsert_table.py uniprot data/id_mapper.tsv

    Same operation but piping the data from a command:
    $ map_ids.py | upsert_table.py id_mapper
                           """)


    table_types.add_parser("uniprot",
                           help="",
                           usage="upsert_table.py uniprot [options] <file>",
                           description="""
The 'uniprot' table stores information retrieved from the UniProt database for
every entry associated with the organism.

Keywords, GO Terms, EC Numbers, and Post-translational Modifications are stored
in different tables and are referenced by the 'uniprot' table.

Input Format:
  - The data file should be tab-separated with the following columns:
    1. UniProt Accession
    2. Locus Tag (if multiple values, separated by ';')
    3. ORF Name (if multiple values, separated by ';')
    4. Gene Name
    5. KEGG Accession (if multiple values, separated by ';')
    6. EMBL Protein ID
    7. RefSeq Accession (if multiple values, separated by ';')
    8. Keywords (if multiple values, separated by ';')
    9. Protein Name
    10. Protein Existence
    11. Sequence
    12. GO Terms (if multiple values, separated by ';')
    13. EC Number (if multiple values, separated by ';')
    14. Post-translational Modifications (valid JSON array)

See `src/fetch_uniprot_organism_json.py` and `src/process_uniprot_json_entry.py`
for details on generating a compatible data file.
                            """,
                           formatter_class=fmt,
                           epilog="""
Example:

  Upserting data from a file to the 'uniprot' table in the database:
    $ upsert_table.py uniprot data/uniprot.tsv

    Same operation but piping the data from a command:
    $ generate_data.py | upsert_table.py uniprot
    """)


    table_types.add_parser("refseq",
                           help="",
                           usage="upsert_table.py refseq [options] <file>",
                           description="""
The 'refseq' table stores the retrived information from a RefSeq annotated genome.

Input Format:
  - The data file should be tab-separated with the following columns:
    1. RefSeq Locus Tag
    2. Locus Tag (if multiple values, separated by a semicolon)
    3. RefSeq Protein ID (if multiple values, separated by a semicolon)
    4. Strand Location
    5. Start Position
    6. End Position
    7. Translated Protein Sequence

See `src/fetch_refseq_genome.py` for more details on how to generate a compatible data file.
                            """,
                           formatter_class=fmt,
                           epilog="""
Example:

  Upsert data from a file to the 'refseq' table in the database:
    $ upsert_table.py refseq data/refseq.tsv

  Same operation but piping the data from a command:
    $ generate_data.py | upsert_table.py refseq
    """)


    table_types.add_parser("kegg",
                           help="",
                           usage="upsert_table.py kegg [options] <file>",
                           description="""
The 'kegg' table stores the KEGG accession, involved pathways, KEGG Orthology,
for all of the KEGG entries for a given organism.

Input Format:
  - The data file should be tab-separated with the following columns:
    1. KEGG Accession
    2. KEGG Pathways (if multiple values, separated by a semicolon)
    3. KEGG Orthology (if multiple values, separated by a semicolon)

See `src/fetch_kegg_organism.py` for more details on how to generate a compatible data file.
                           """,
                           formatter_class=fmt,
                           epilog="""
Example:

    Upsert data from a file to the 'kegg' table in the database:
    $ upsert_table.py kegg data/kegg.tsv

    Same operation but piping the data from a command:
    $ generate_data.py | upsert_table.py kegg
    """)


    table_types.add_parser("kegg_relations",
                           help="",
                           usage="upsert_table.py kegg_relations [options] <file>",
                           description="""
The 'kegg_relations' table stores the any relationship where a query KEGG accession
may be involved within a specific pathway.

Input Format:
- The data file should be tab-separated with the following columns:
  1. Source KEGG accession
  2. Target KEGG accession
  3. Pathway ID
  4. Relation type
  5. Relation subtypes (if multiple, separated by ';')
  6. Relation subtype values (if multiple, separated by ';')

See `src/fetch_kegg_relations.py` for more details on how to generate a compatible data file.
    """,
                            formatter_class=fmt,
                            epilog="""
Example:

    Upsert data from a file to the 'kegg_relations' table in the database:
    $ upsert_table.py kegg_relations data/kegg_relations.tsv

    Same operation but piping the data from a command:
    $ generate_data.py | upsert_table.py kegg_relations
    """)


    table_types.add_parser("string_interactions",
                           help="",
                           formatter_class=fmt,
                           usage="upsert_table.py string_interactions [options] <file>",
                           description="""
The 'string_interactions' table stores the protein-protein interactions from the STRING database.

Input Format:
- The data file should be tab-separated with the following columns:
    1. Protein A
    2. Protein B
    3. Neighborhood Score
    4. Neighborhood Transferred Score
    5. Fusion Score
    6. Phylogenetic Co-occurrence Score
    7. Homology Score
    8. Coexpression Score
    9. Coexpression Transferred Score
    10. Experimental Score
    11. Experimental Transferred Score
    12. Database Score
    13. Database Transferred Score
    14. Textmining Score
    15. Textmining Transferred Score
    16. Combined Score
    """,
                           epilog="""
Example:

  Upsert data from a file to the 'string_interactions' table in the database:
    $ cat data/string_interactions_formatted.tsv | upsert_table.py string_interactions
    """)


    table_types.add_parser("experimental_condition",
                           help="",
                           formatter_class=fmt,
                           usage="upsert_table.py experimental_condition [options] <file>",
                           description="""
The 'experimental_condition' table stores the experimental conditions of the different
experimental results that will be stored in the database.

Input Format:
- The data file should be tab-separated with the following columns:
    1. Experimental Condition Name
    2. Experimental Condition Description
    3. Experimental Condition Type
                           """,
                           epilog="""
Example:
    Upsert specific experimental conditions to the 'experimental_condition' table in the database:
    $ upsert_table.py experimental_condition data/experimental_conditions.tsv
    """)


    table_types.add_parser("transcriptomics",
                           help="",
                           formatter_class=fmt,
                           usage="upsert_table.py transcriptomics [options] <file> <condition_a> <condition_b>",
                           description="""
The 'transcriptomics' table stores the differential expression data between two experimental conditions.

Input Format:
- The data file should be tab-separated with the following columns:
    1. Experimental ID
    2. Condition A
    3. Condition B
    4. Log2 Fold Change
    5. P-value
    6. Adjusted P-value
                           """,
                           epilog="""
Example:
  Upsert data from a file to the 'transcriptomics' table in the database:
    $ upsert_table.py transcriptomics data/transcriptomics.tsv control treatment
    """)


    table_types.add_parser("transcriptomics_counts",
                           help="",
                           formatter_class=fmt,
                           usage="upsert_table.py transcriptomics_counts [options] <file> <experimental_condition> <replicate>",
                           description="""
The 'transcriptomics_counts' table stores the raw and normalized counts for each gene
in a given experimental condition and replicate.
                           """,
                           epilog="""
Example:
Upsert data from a file to the 'transcriptomics_counts' table in the database:
    $ upsert_table.py transcriptomics_counts data/transcriptomics_counts.tsv control 1
    """)

    table_types.add_parser("proteomics",
                           help="",
                           formatter_class=fmt,
                           usage="upsert_table.py proteomics [options] <file> <condition_a> <condition_b>",
                           description="""
The 'proteomics' table stores the differential expression data between two experimental conditions.

Input Format:
- The data file should be tab-separated with the following columns:
    1. Experimental ID
    2. Condition A
    3. Condition B
    4. Peptide Sequence
    5. Peptide Positions
    6. Log2 Fold Change
    7. P-value
    8. Adjusted P-value
                           """,
                           epilog="""
Example:
Upsert data from a file to the 'proteomics' table in the database:
    $ upsert_table.py proteomics data/proteomics.tsv control treatment
    """)


    table_types.add_parser("proteomics_replicates",
                           help="",
                           formatter_class=fmt,
                           usage="upsert_table.py proteomics_replicates [options] <file> <experimental_condition> <replicate>",
                           description="""
The 'proteomics_replicates' table stores the raw intensity values for each protein in a given
experimental condition and replicate.

Input Format:
- The data file should be tab-separated with the following columns:
    1. Experimental ID
    2. Experimental Condition
    3. Replicate
    4. Intensity
                           """,
                           epilog="""
Example:
Upsert data from a file to the 'proteomics_replicates' table in the database:
    $ upsert_table.py proteomics_replicates data/proteomics_replicates.tsv control 1
    """)

    # Add arguments based on specific subparser (table type) we are using
    for subparser in table_types.choices.values():

        required = subparser.add_argument_group("Arguments")
        required.add_argument("file",
                              metavar="<file>",
                              nargs="?",
                              type=str,
                              default="-",
                              help="File containing data to parse, or '-' to read from stdin. Defaults to stdin if omitted.")


        if subparser in [
            table_types.choices["transcriptomics_counts"],
            table_types.choices["proteomics_replicates"],
        ]:
            required.add_argument("experimental_condition",
                                  metavar="<experimental_condition>",
                                  type=str,
                                  help="Name of the experimental condition. E.g. 'control', 'treatment', etc.")

        if subparser in [
                table_types.choices["transcriptomics"],
                table_types.choices["proteomics"],
        ]:

            required.add_argument("condition_a",
                                  metavar="<condition_a>",
                                  type=str,
                                  help="Name of the first experimental condition. E.g. 'control', 'treatment', etc.")

            required.add_argument("condition_b",
                                  metavar="<condition_b>",
                                  type=str,
                                  help="Name of the second experimental condition. E.g. 'control', 'treatment', etc.")

        if subparser in [
            table_types.choices["transcriptomics_counts"],
            table_types.choices["proteomics_replicates"],
        ]:
            required.add_argument("replicate",
                                  metavar="<replicate>",
                                  type=int,
                                  help="Replicate number for the experimental condition. E.g. 1, 2, 3, etc.")


        # Add optional arguments present in all subparsers
        subparser._optionals.title = "Options"
        subparser.add_argument("--db",
                               metavar="<conn>",
                               type=str,
                               help="Database connection string. Reads from 'config/database.yaml' if unspecified.")
        subparser.add_argument("--log",
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
    args = parser.parse_args(args=None if sys.argv[1:] else ["-h"])

    logger = setup_logger(args.log)
    logger.info(f"Arguments: {vars(args)}")

    if not args.db:
        try:
            args.db = get_database_connection_string()
        except (FileNotFoundError, KeyError) as e:
            raise e

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

    try:
        conn = connect_to_db(args.db)
    except psycopg2.Error:
        sys.exit(1)


    match args.table_type:

        case "id_mapper":

            from lib.table_id_mapper import run_upsert_id_mapper
            run_upsert_id_mapper(in_data, conn)

        case "uniprot":

            from lib.table_uniprot import run_upsert_uniprot
            run_upsert_uniprot(in_data, conn)

        case "refseq":

            from lib.table_refseq import run_upsert_refseq
            run_upsert_refseq(in_data, conn)

        case "kegg":

            from lib.table_kegg import run_upsert_kegg
            run_upsert_kegg(in_data, conn)

        case "kegg_relations":

            from lib.table_kegg_relations import run_upsert_kegg_relations
            run_upsert_kegg_relations(in_data, conn)

        case "string_interactions":

            from lib.table_string_interactions import run_upsert_string_interactions
            run_upsert_string_interactions(in_data, conn)

        case "experimental_condition":

            from lib.table_experimental_condition import run_upsert_experimental_condition
            run_upsert_experimental_condition(in_data, conn)

        case "transcriptomics":

            from lib.table_transcriptomics import run_upsert_transcriptomics
            run_upsert_transcriptomics(in_data, args.condition_a, args.condition_b, conn)

        case "transcriptomics_counts":

            from lib.table_transcriptomics_counts import run_upsert_transcriptomics_counts
            run_upsert_transcriptomics_counts(in_data, args.experimental_condition, args.replicate, conn)

        case "proteomics":

            from lib.table_proteomics import run_upsert_proteomics
            run_upsert_proteomics(
                in_data, args.condition_a, args.condition_b, conn
            )

        case "proteomics_replicates":

            from lib.table_proteomics_replicates import run_upsert_proteomics_replicates
            run_upsert_proteomics_replicates(in_data, args.experimental_condition, args.replicate, conn)


    conn.close()
    logger.info("Data upserted successfully. Connection closed.")

    sys.exit(0)


if __name__ == "__main__":
    main()
