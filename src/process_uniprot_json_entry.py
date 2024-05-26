#!/usr/bin/env python

"""
Given a JSON file containing one or multiple UniProt entries, as retrieved from the UniProt API, 
this script will extract the relevant information and write it to a tab-separated file.

Output format:
- Generates a tab-separated file including:
  1. Primary accession
  2. Locus tag(s) (if multiple, separated by ';')
  3. ORF names (if multiple, separated by ';')
  4. KEGG accession(s) (if multiple, separated by ';')
  5. EMBL protein ID
  6. RefSeq accession
  7. Keywords (if multiple, separated by ';')
  8. Protein name
  9. Protein existence
  10. Sequence
  11. GO terms (if multiple, separated by ';')
  12. EC number(s) (if multiple, separated by ';')
  13. Post-translational modifications (JSON array as string)
"""

import argparse
import json
import logging
import sys
from typing import Tuple, List

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
        add_help=False,
        usage="%(prog)s [options] <jsonfile>",
        description=__doc__,
        epilog="""
Examples:
  Process a single JSON file:
    $ python %(prog)s P12345.json

  Save processed data to a file:
    $ python %(prog)s P12345.json > P12345.tsv

  Chain data fetching and processing:
    $ python fetch_entries.py 9606 | python %(prog)s | python upsert_table.py

For more information, see: PLACEHOLDER
""")

    parser._positionals.title = "Arguments"
    parser.add_argument("jsonfile",
                        metavar="<jsonfile>",
                        type=str,
                        default="-",
                        nargs="?",
                        help="JSON file containing one or multiple UniProt entries, or '-' to read from stdin. Default: '-'.")

    parser._optionals.title = "Optionals"
    parser.add_argument("-h", "--help",
                        action="help",
                        default=argparse.SUPPRESS,
                        help="Display this help message and exit.")
    parser.add_argument("--log",
                        metavar="<level>",
                        type=str,
                        default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        help="Set the logging level (default: INFO).")

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


def get_locus_tag(genes: dict) -> str:

    locus_tag = []

    for evidence in genes:

        if "orderedLocusNames" in evidence:
            try:
                for oln in evidence["orderedLocusNames"]:
                    locus_tag.append(oln["value"])
            except KeyError:
                continue

    if not locus_tag:
        return "NULL"

    return ";".join(locus_tag)


def get_orf_names(genes: dict) -> str:

    orf_names = []

    for evidence in genes:

        if "orfNames" in evidence:
            try:
                for orf in evidence["orfNames"]:
                    orf_names.append(orf["value"])
            except KeyError:
                continue

    if not orf_names:
        return "NULL"

    return ";".join(orf_names)


def get_embl_protein_id(xrefs: List[dict]) -> str:

    embl_protein_id = "NULL"

    for xref in xrefs:

        try:
            if xref["database"] == "EMBL":
                for property in xref["properties"]:
                    if property["key"] == "ProteinId":
                        embl_protein_id = property["value"]
        except KeyError:
            return embl_protein_id

    return embl_protein_id

def get_refseq_accession(xrefs: List[dict]) -> str:

    refseq_accession = "NULL"

    for xref in xrefs:

        try:
            if xref["database"] == "RefSeq":
                for property in xref["properties"]:
                    if property["key"] == "accession":
                        refseq_accession = property["value"]
        except KeyError:
                return refseq_accession

        return refseq_accession


def get_kegg_accession(xrefs: List[dict]) -> str:

    kegg_accession = []

    for xref in xrefs:

        try:
            if xref["database"] == "KEGG":
                kegg_accession.append(xref["id"])
        except KeyError:
            continue

    if not kegg_accession:
        return "NULL"

    return ";".join(kegg_accession)


def get_keywords(keywords: List[dict]) -> str:


    if not keywords:
        return "NULL"

    keywords = [keyword["id"] for keyword in keywords]

    return ";".join(keywords)


def get_name(protein_description: dict) -> str:

    name = "NULL"

    try:
        name = protein_description["recommendedName"]["fullName"]["value"]
    except KeyError:
        try:
            name = protein_description["alternativeNames"][0]["fullName"]["value"]
        except KeyError:
            return name

    return name


def get_ec_number(protein_description: dict) -> str:

    ec_numbers = []

    if "recommendedName" not in protein_description:
        return "NULL"

    if "ecNumbers" not in protein_description["recommendedName"]:
        return "NULL"

    try:
        for ec in protein_description["recommendedName"]["ecNumbers"]:
            ec_numbers.append(ec["value"])
    except KeyError:
        print(protein_description)
        sys.exit(1)

    if not ec_numbers:
        return "NULL"

    return ";".join(ec_numbers)


def get_go_terms(xrefs: List[dict]) -> str:

    go_terms = []

    for xref in xrefs:

        try:
            if xref["database"] == "GO":
                go_terms.append(xref["id"])
        except KeyError:
            continue

    if not go_terms:
        return "NULL"

    return ";".join(go_terms)

def get_ptm(features: List[dict]) -> str:

    ptm = []

    for feature in features:

        try:
            if feature["type"] == "Modified residue":
                start = feature["location"]["start"]["value"]
                end = feature["location"]["end"]["value"]
                description = feature["description"]

                ptm.append({"position": f"{start}..{end}", "description": description})
        except KeyError:
            continue

    if not ptm:
        return "NULL"

    return json.dumps(ptm)


def parse_json_entry(entry: dict) -> Tuple:

    primary_accession = "NULL"
    primary_accession = entry["primaryAccession"]

    locus_tag = "NULL"
    orf_names = "NULL"
    kegg_accession = "NULL"
    embl_protein_id = "NULL"
    refseq_accession = "NULL"
    keywords = "NULL"
    name = "NULL"
    protein_existence = "NULL"
    sequence = "NULL"
    go_terms = "NULL"
    ec_number = "NULL"
    ptm = "NULL"

    if "genes" in entry:
        locus_tag = get_locus_tag(entry["genes"])
        orf_names = get_orf_names(entry["genes"])

    if "sequence" in entry:
        if "value" in entry["sequence"]:
            sequence = entry["sequence"]["value"]

    if "uniProtKBCrossReferences" in entry:
        kegg_accession = get_kegg_accession(entry["uniProtKBCrossReferences"])
        embl_protein_id = get_embl_protein_id(entry["uniProtKBCrossReferences"])
        refseq_accession = get_refseq_accession(entry["uniProtKBCrossReferences"])
        go_terms = get_go_terms(entry["uniProtKBCrossReferences"])

    if "keywords" in entry:
        keywords = get_keywords(entry["keywords"])

    if "proteinDescription" in entry:
        name = get_name(entry["proteinDescription"])
        ec_number = get_ec_number(entry["proteinDescription"])

    if "proteinExistence" in entry:
        protein_existence = entry["proteinExistence"]

    if "features" in entry:
        ptm = get_ptm(entry["features"])

    return (
        # if any is null, will be str as NULL
        primary_accession, # str
        locus_tag, # List as str joined by ;
        orf_names, # List as str joined by ;
        kegg_accession, # List as str joined by ;
        embl_protein_id, # str
        refseq_accession, # str
        keywords, # List as str joined by ;
        name, # str
        protein_existence, # str
        sequence, # str
        go_terms, # List as str joined by ;
        ec_number, # List as str joined by ;
        ptm # JSON arr as str
    )


def main():

    args, logger = setup_config()

    try:
        input = read_input(args.jsonfile)
    except FileNotFoundError:
        sys.exit(1)

    input = input.splitlines()

    try:
        json_data = [json.loads(line) for line in input]
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON data: {e}. Exiting.")
        sys.exit(1)

    for block in json_data:
        results = block["results"]

        for entry in results:

            record = parse_json_entry(entry)
            print("\t".join(record))

    sys.exit(0)


if __name__ == "__main__":
    main()

