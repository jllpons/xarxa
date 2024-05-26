#!/usr/bin/env python

"""
Extracts data from a RefSeq annotated genome in GenBank format. Formats
extracted data and prints it in tabular format to stdout for downstream processing.

Output:
  - Generates a tab-separated file including:
    1. RefSeq Locus Tag
    2. Locus Tag (if multiple, separated by semicolon)
    3. RefSeq Accession
    4. Strand Location
    5. Start Position
    6. End Position
    7. Translated Protein Sequence
"""

import argparse
from io import StringIO
import logging
import sys
from typing import List

from Bio import SeqIO, SeqFeature

from lib.cli import (
        CustomHelpFormatter,
        setup_logger,
        read_input
)
from lib.generic_row import GenericRow


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
        usage="%(prog)s [options] <file>",
        description=__doc__,
        epilog="""
Examples:
  Direct file processing:
    $ python %(prog)s myAssembly.gbff > myAssembly_refseq_data.tsv

  Stream processing from stdin:
    $ cat myAssembly.gbff | python %(prog)s | grep "myFavoriteLocusTag"

  Pipeline processing:
    $ python %(prog)s | python parse_data.py

For more information, see the `docs` directory.
""")

    required = parser.add_argument_group("Arguments")
    required.add_argument("file",
                          metavar="<file>",
                          nargs="?",
                          type=str,
                          default="-",
                          help="GeneBank file to process or '-' for stdin. Defaults to stdin if not specified.")

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
                        help="Logging level (default: INFO).")

    return parser


def format_refseq_genbank_data(record: SeqIO.SeqRecord,
                               logger: logging.Logger) -> List[GenericRow]:
    """
    Extracts and formats data from a RefSeq annotated genome in GenBank format.

    Args:
        record (SeqIO.SeqRecord): The SeqRecord object containing the GenBank data.
        logger (logging.Logger): The logger object to log messages.

    Returns:
        List[GenericRow]: A list of GenericRow objects containing the formatted data.
    """

    # Will have the RefSeq Locus Tag as the key and a GenericRow object as the value
    features_hashmap = {}

    for feature in record.features:
        if "locus_tag" in feature.qualifiers:
            if feature.qualifiers["locus_tag"][0] not in features_hashmap:
                add_feature_to_hashmap(feature, features_hashmap, logger)
            else:
                update_feature_in_hashmap(feature, features_hashmap, record.seq)

    records = []
    for refseq_ltag in features_hashmap:
        records.append(features_hashmap[refseq_ltag])

    return records


def add_feature_to_hashmap(feature: SeqFeature.SeqFeature,
                           features_hashmap: dict,
                           logger: logging.Logger) -> None:
    """
    Helper function for `format_refseq_genbank_data`. Adds a feature to the
    features hashmap.

    Args:
        feature (SeqFeature.SeqFeature): The SeqFeature object to add to the hashmap.
        features_hashmap (dict): The dictionary to add the feature to.
        logger (logging.Logger): The logger object to log messages.

    Returns:
        None
    """

    refseq_ltag = feature.qualifiers["locus_tag"][0]
    if len(feature.qualifiers["locus_tag"]) > 1:
        logger.warning(
            f"Multiple locus tags found for RefSeq Locus Tag: {refseq_ltag}. "
            + "Only the first locus tag will be used."
        )

    features_hashmap[refseq_ltag] = GenericRow(
        refseq_locus_tag=refseq_ltag,
        locus_tag=feature.qualifiers.get("old_locus_tag", []),
        refseq_accession=feature.qualifiers.get("protein_id", []),
        strand_location="+" if feature.location.strand == 1 else "-",
        start_position=str(feature.location.start),
        end_position=str(feature.location.end),
        protein_sequence=feature.qualifiers.get("translation", [None])[0]
    )


def update_feature_in_hashmap(feature: SeqFeature.SeqFeature,
                              features_hashmap: dict,
                              sequence: str) -> None:
    """
    Helper function for `format_refseq_genbank_data`. Updates a feature in the
    features hashmap.

    Args:
        feature (SeqFeature.SeqFeature): The SeqFeature object to update in the hashmap.
        features_hashmap (dict): The dictionary to update the feature in.
        sequence (str): The sequence of the record.

    Returns:
        None
    """

    refseq_ltag = feature.qualifiers["locus_tag"][0]

    for value in feature.qualifiers.get("old_locus_tag", []):
        if value not in features_hashmap[refseq_ltag].locus_tag:
            features_hashmap[refseq_ltag].locus_tag.append(value)

    for value in feature.qualifiers.get("protein_id", []):
        if value not in features_hashmap[refseq_ltag].refseq_accession:
            features_hashmap[refseq_ltag].refseq_accession.append(value)

    if features_hashmap[refseq_ltag].protein_sequence is None:

        if "pseudo" in feature.qualifiers:
            # TODO: See if :
            #  - codon table should be considered <https://biopython.org/docs/1.75/api/Bio.SeqFeature.html#Bio.SeqFeature.SeqFeature.translate>
            #  - if the a full translation should be attempted <https://biopython.org/docs/1.75/api/Bio.Seq.html#Bio.Seq.Seq.translate>
            features_hashmap[refseq_ltag].protein_sequence = str(feature.extract(sequence).translate(to_stop=True))

        else:
            features_hashmap[refseq_ltag].protein_sequence = feature.qualifiers.get("translation", [None])[0]


def main():

    parser = setup_argparse()
    args = parser.parse_args()

    logger = setup_logger(args.log)

    if len(sys.argv) == 1 and sys.stdin.isatty():
        parser.print_help()
        sys.exit(1)

    logger.info(f"Arguments: {vars(args)}")

    try:
        data = read_input(args.file)
    except FileNotFoundError:
        sys.exit(1)

    logger.info("Processing GenBank file.")
    logger.info("Parsing...")
    genbank_record = SeqIO.read(StringIO(data), "genbank")


    logger.info("Formatting data...")
    recrods = format_refseq_genbank_data(genbank_record, logger)
    logger.info(f"Succesfully extracted {len(recrods)} records from GenBank file.")

    logger.info("Printing data to stdout...")

    try:
        for r in recrods:
            print(r, flush=True)
    except BrokenPipeError:
        sys.stdout = None
        logger.error("Broken pipe error caught. Terminating program.")
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()

