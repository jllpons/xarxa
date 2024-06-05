#!/usr/bin/env python

"""
Creates a master ID file using the IDs from three different inputs: UniProt, RefSeq, and KEGG. 
The generated file will map the different IDs present in the database.

The three inputs are:
  1. IDs extracted from the UniProt database
  2. IDs extracted from the RefSeq annotation of the organism genome
  3. IDs extracted from the KEGG database

Input Format (UniProt):
  1. UniProt Primary Accession Number
  2. Locus Tag (separated by a semicolon if multiple, 'NULL' if not available)
  3. ORF Names (separated by a semicolon if multiple, 'NULL' if not available)
  4. KEGG Accession Number (separated by a semicolon if multiple, 'NULL' if not available)
  5. RefSeq Protein ID ('NULL' if not available)

Input Format (RefSeq):
  1. RefSeq Locus Tag
  2. Locus Tag (separated by a semicolon if multiple, 'NULL' if not available)
  3. RefSeq Protein ID ('NULL' if not available)
"""


import argparse
from dataclasses import dataclass
import logging
import sys
from typing import Optional, Tuple, List, Dict

from lib.cli import (
    CustomHelpFormatter,
    setup_logger,
    read_input
)
from lib.generic_row import parse_tsv

@dataclass
class IdMasterRecord:

    uniprot_accession: Optional[str] = None
    refseq_locus_tag: Optional[str] = None
    locus_tag: Optional[str] = None
    kegg_accession: Optional[str] = None
    refseq_protein_id: Optional[str] = None

    def __str__(self) -> str:

        return "\t".join([
            f"{self.uniprot_accession}" if self.uniprot_accession else "NULL",
            f"{self.refseq_locus_tag}" if self.refseq_locus_tag else "NULL",
            f"{self.locus_tag}" if self.locus_tag else "NULL",
            f"{self.kegg_accession}" if self.kegg_accession else "NULL",
            f"{self.refseq_protein_id}" if self.refseq_protein_id else "NULL"
        ])


class IdConflictError(Exception):
    """
    Exception raised when a conflict is detected between the IDs.
    """
    pass


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
        usage="%(prog)s [options] <uniprot> <refseq> <kegg>",
        description=__doc__,
        epilog="""
Examples:

  Parse UniProt, RefSeq, and KEGG IDs:
    $ %(prog)s uniprot.tsv refseq.tsv kegg.tsv > id_master.tsv

For more information, see: PLACEHOLDER
""")

    parser._positionals.title = "Arguments"
    parser.add_argument("uniprot",
                        metavar="<uniprot>",
                        type=str,
                        default="-",
                        nargs="?",
                        help="UniProt ID file (default: stdin).")

    parser.add_argument("refseq",
                        metavar="<refseq>",
                        type=str,
                        default="-",
                        nargs="?",
                        help="RefSeq ID file (default: stdin).")

    parser.add_argument("kegg",
                        metavar="<kegg>",
                        type=str,
                        default="-",
                        nargs="?",
                        help="KEGG ID file (default: stdin).")

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


def parse_uniprot(uniprot_ids: str, records: List[IdMasterRecord], logger: logging.Logger) -> None:
    """
    Parse the UniProt IDs and create a list of IdMasterRecord objects.

    For each UniProt record, the function will create a new IdMasterRecord object
    and append it to the records list. The IdMasterRecord object will contain the
    UniProt accession, RefSeq protein ID, Locus Tag, and KEGG accession.

    Parameters
        uniprot_ids (str): The UniProt ID file.
        records (list): A list of IdMasterRecord objects.
        logger (logging.Logger): The logger instance.

    Returns
        None
    """

    schema = {
        "uniprot_accession": str,
        "locus_tag": list,
        "orf_name": list,
        "kegg_accession": list,
        "refseq_protein_id": str
    }
    generic_rows = parse_tsv(uniprot_ids, schema)


    # Consider ORF names as locus tags
    for row in generic_rows:
        if not row.locus_tag:
            row.locus_tag = row.orf_name or []

        if row.orf_name:
            for name in row.orf_name:
                if name not in row.locus_tag:
                    row.locus_tag.append(name)

        # Split RefSeq protein ID to remove the version
        row.refseq_protein_id = row.refseq_protein_id.split(".")[0] if row.refseq_protein_id else None


    for row in generic_rows:

        uniprot_accession = row.uniprot_accession
        locus_tags = row.locus_tag or []
        kegg_accessions = row.kegg_accession or []
        refseq_protein_id = row.refseq_protein_id

        # This record only contains the UniProt accession
        if not any([locus_tags, kegg_accessions, refseq_protein_id]):
            records.append(IdMasterRecord(uniprot_accession=uniprot_accession))
            continue

        # This record contains the UniProt accession and RefSeq protein ID
        if not locus_tags and not kegg_accessions:
            records.append(IdMasterRecord(
                uniprot_accession=uniprot_accession,
                refseq_protein_id=refseq_protein_id
            ))
            continue

        # This record contains the UniProt accession, RefSeq protein ID, and KEGG accession
        if not locus_tags:
            for kegg in kegg_accessions:
                records.append(IdMasterRecord(
                    uniprot_accession=uniprot_accession,
                    kegg_accession=kegg,
                    refseq_protein_id=refseq_protein_id
                ))
            continue

        # This record contains the UniProt accession, RefSeq protein ID, and Locus Tag
        if not kegg_accessions:
            for ltag in locus_tags:
                records.append(IdMasterRecord(
                    uniprot_accession=uniprot_accession,
                    locus_tag=ltag,
                    refseq_protein_id=refseq_protein_id
                ))
            continue

        # Pair locus tags with matching KEGG accessions
        # E.g.:
        # locus_tags = ['Smlt1234', 'Smlt5678'], kegg_accessions = ['sml:Smlt1234', 'sml:Smlt5678']
        # paired_lt_kegg = [('Smlt1234', 'sml:Smlt1234'), ('Smlt5678', 'sml:Smlt5678')]
        paired_lt_kegg = pair_lt_kegg(locus_tags, kegg_accessions)

        for lt, kegg in paired_lt_kegg:

            # (lt, kegg) = (None, 'sml:Smlt1234')
            if not lt:
                logger.warning(
                    f"UniProt: '{uniprot_accession}' - No matching locus tag found for KEGG: '{kegg}', "
                    + f"Locus Tags: '{locus_tags}', KEGG Accessions: '{kegg_accessions}'"
                    + ". Record will be added with the KEGG accession only."
                    )

            # (lt, kegg) = ('Smlt1234', None)
            if not kegg:
                logger.warning(
                    f"UniProt: '{uniprot_accession}' - No matching KEGG accession found for Locus Tag: '{lt}', "
                    + f"Locus Tags: '{locus_tags}', KEGG Accessions: '{kegg_accessions}'"
                    + ". Record will be added with the Locus Tag only."
                    )

            records.append(IdMasterRecord(
                uniprot_accession=uniprot_accession,
                locus_tag=lt,
                kegg_accession=kegg,
                refseq_protein_id=refseq_protein_id
            ))


def pair_lt_kegg(locus_tags: List[str], kegg_accessions: List[str]) -> List[Tuple[Optional[str], Optional[str]]]:
    """
    Pair locus tags with matching KEGG accessions.

    Parameters
        locus_tags (list): A list of locus tags.
        kegg_accessions (list): A list of KEGG accessions.
        logger (logging.Logger): The logger instance.

    Returns
        list: A list of tuples containing the paired locus tags and KEGG accessions.
    """

    paired_lt_kegg = []

    # First pass: pair locus_tags with matching kegg_accessions
    for lt in locus_tags:
        # `next()` will assign the matched kegg accession or None if no match is found
        matched_kegg = next((kegg for kegg in kegg_accessions if lt in kegg), None)
        paired_lt_kegg.append((lt, matched_kegg))

    # Second pass: add remaining kegg_accessions that were not paired
    paired_keggs = {pair[1] for pair in paired_lt_kegg if pair[1] is not None}
    unpaired_kegg = [kegg for kegg in kegg_accessions if kegg not in paired_keggs]

    for kegg in unpaired_kegg:
        paired_lt_kegg.append((None, kegg))

    return paired_lt_kegg


def build_locus_tag_map(records: List[IdMasterRecord]) -> Dict[str, List[int]]:
    """
    Build a map consiting of each different locus tag as key and the indexes of
    the records that contain that locus tag as values.

    Parameters
        records (list): A list of IdMasterRecord objects.

    Returns
        dict: A dictionary with the locus tags as keys and the indexes of the records as values.
    """

    locus_tag_map = {}

    for i, record in enumerate(records):
        if record.locus_tag:
            if record.locus_tag not in locus_tag_map:
                locus_tag_map[record.locus_tag] = [i]
            else:
                locus_tag_map[record.locus_tag].append(i)

    return locus_tag_map


def build_refseq_protein_id_map(records: List[IdMasterRecord]) -> Dict[str, List[int]]:
    """
    Build a map consiting of each different RefSeq protein ID as key and the indexes of
    the records that contain that RefSeq protein ID as values.

    Parameters
        records (list): A list of IdMasterRecord objects.

    Returns
        dict: A dictionary with the RefSeq protein IDs as keys and the indexes of the records as values.
    """

    refseq_protein_id_map = {}

    for i, record in enumerate(records):
        if record.refseq_protein_id:
            if record.refseq_protein_id not in refseq_protein_id_map:
                refseq_protein_id_map[record.refseq_protein_id] = [i]
            else:
                refseq_protein_id_map[record.refseq_protein_id].append(i)

    return refseq_protein_id_map


def parse_refseq(refseq_ids: str, records: List[IdMasterRecord], logger: logging.Logger) -> None:

    schema = {
        "refseq_locus_tag": str,
        "locus_tag": list,
        "refseq_protein_id": str
    }
    generic_rows = parse_tsv(refseq_ids, schema)

    locus_tag_map = build_locus_tag_map(records)
    refseq_protein_id_map = build_refseq_protein_id_map(records)

    for row in generic_rows:

        refseq_locus_tag = row.refseq_locus_tag
        locus_tags = row.locus_tag or []
        refseq_protein_id = row.refseq_protein_id.split(".")[0] if row.refseq_protein_id else None


        # This record only contains the RefSeq locus tag
        if not any([locus_tags, refseq_protein_id]):
            records.append(IdMasterRecord(refseq_locus_tag=refseq_locus_tag))
            continue


        # This record contains the RefSeq locus tag and RefSeq protein ID
        if not locus_tags:

            # Check if the RefSeq protein ID is already present in the records
            if refseq_protein_id in refseq_protein_id_map:
                # For each record that contains the RefSeq protein ID, assign the RefSeq locus tag
                for i in refseq_protein_id_map[refseq_protein_id]:
                    if records[i].refseq_locus_tag and records[i].refseq_locus_tag != refseq_locus_tag:
                        logger.error(
                            f"RefSeq: '{refseq_locus_tag}' - RefSeq Protein ID: '{refseq_protein_id}' "
                            + "matched but RefSeq Locus Tags do not match."
                            + f" Existing: '{records[i].refseq_locus_tag}', New: '{refseq_locus_locus_tag}'"
                            + " Skipping..."
                        )
                        raise IdConflictError
                    records[i].refseq_locus_tag = refseq_locus_tag

            # If the RefSeq protein ID is not present, create a new record
            else:
                records.append(IdMasterRecord(
                    refseq_locus_tag=refseq_locus_tag,
                    refseq_protein_id=refseq_protein_id
                ))
                refseq_protein_id_map = build_refseq_protein_id_map(records)
            continue


        # This record contains the RefSeq locus tag and Locus Tag
        if not refseq_protein_id:

            for ltag in locus_tags:
                # Check if the locus tag is already present in the records
                if ltag in locus_tag_map:
                    # For each record that contains the locus tag, assign the RefSeq locus tag
                    for i in locus_tag_map[ltag]:
                        records[i].refseq_locus_tag = refseq_locus_tag
                # If the locus tag is not present, create a new record
                else:
                    records.append(IdMasterRecord(
                        refseq_locus_tag=refseq_locus_tag,
                        locus_tag=ltag
                        ))
                    locus_tag_map = build_locus_tag_map(records)
            continue


        # This record contains the RefSeq locus tag, Locus Tag, and RefSeq protein ID
        for ltag in locus_tags:

            # Check if the locus tag and RefSeq protein ID are already present in the records
            if ltag in locus_tag_map and refseq_protein_id in refseq_protein_id_map:
                # Best case scenario: all IDs match
                # =================================

                # Check if all the records that contain the locus tag also contain the RefSeq protein ID
                # (and vice versa)
                if set(locus_tag_map[ltag]) == set(refseq_protein_id_map[refseq_protein_id]):
                    # Both lists are the same, so we can assign the RefSeq locus tag to all the records
                    for i in locus_tag_map[ltag]:
                        records[i].refseq_locus_tag = refseq_locus_tag

                elif len(refseq_protein_id_map[refseq_protein_id]) > len(locus_tag_map[ltag]):
                    # We have more RefSeq protein IDs than locus tags
                    # This can mean:
                    # - A same RefSeq protein ID can be associated with multiple locus tags (expected)
                    # - A same locus tag can be associated with multiple RefSeq protein IDs (not allowed)
                    # - A locus tag is missing a RefSeq protein ID (should not happen but we'll handle it)

                    # Are the indexes of the rows that contain the locus tag
                    # a subset of the indexes of the rows that contain the RefSeq protein ID?
                    if set(locus_tag_map[ltag]).issubset(set(refseq_protein_id_map[refseq_protein_id])):
                        # Assign the RefSeq locus tag to all the records that contain the locus tag
                        # Why not the other way around? Because we want to ass
                        for i in locus_tag_map[ltag]:
                            records[i].refseq_locus_tag = refseq_locus_tag

                    else:
                        raise IdConflictError




                # resolve conflicts
                # - why do we have more locus tags than refseq protein ids? (or vice versa)
                # - Which record is missing the locus tag or refseq protein id?
                #    - If it's missing a locus tag, it's fine

            if ltag in locus_tag_map and refseq_protein_id not in refseq_protein_id_map:
                raise IdConflictError

            if ltag not in locus_tag_map and refseq_protein_id in refseq_protein_id_map:
                raise IdConflictError

            records.append(IdMasterRecord(
                refseq_locus_tag=refseq_locus_tag,
                locus_tag=ltag,
                refseq_protein_id=refseq_protein_id
            ))


def _parse_refseq(refseq_ids, records, logger):

    schema = {
        "refseq_locus_tag": str,
        "locus_tag": list,
        "refseq_protein_id": str
    }
    generic_rows = parse_tsv(refseq_ids, schema)

    for rs_record in generic_rows:

        refseq_locus_tag = rs_record.refseq_locus_tag
        locus_tags = rs_record.locus_tag
        refseq_protein_id = rs_record.refseq_protein_id


        # This record only contains the RefSeq locus tag
        if not any([locus_tags, refseq_protein_id]):
            records.append(IdMasterRecord(refseq_locus_tag=refseq_locus_tag))
            continue


        # This record contains the RefSeq locus tag and RefSeq protein ID
        if not locus_tags:
            # Check if the RefSeq protein ID are the same (ignoring the version)
            rs_record_refseq = rs_record.refseq_protein_id.split(".")[0]
            for master_record in records:
                master_record_refseq = master_record.refseq_protein_id.split(".")[0] if master_record.refseq_protein_id else "NULL"
                if rs_record_refseq == master_record_refseq:
                    master_record.refseq_locus_tag = refseq_locus_tag
                    # we can have multiple matches so the continue is not placed here
            continue


        # This record contains the RefSeq locus tag and Locus Tag
        if not refseq_protein_id:
            # We may have multiple locus tags for the same RefSeq locus tag
            for ltag in locus_tags:
                for master_record in records:
                    if ltag == master_record.locus_tag:
                        master_record.refseq_locus_tag = refseq_locus_tag
            continue


        for master_record in records:
            rs_record_refseq = rs_record.refseq_protein_id.split(".")[0]
            master_record_refseq = master_record.refseq_protein_id.split(".")[0] if master_record.refseq_protein_id else "NULL"

            for ltag in locus_tags:
                # Best case scenario: all IDs match
                if ltag == master_record.locus_tag and rs_record_refseq == master_record_refseq:
                    master_record.refseq_locus_tag = refseq_locus_tag
                    break

                # If the locus tags match but the RefSeq protein IDs do not
                if ltag == master_record.locus_tag and rs_record_refseq != master_record_refseq:
                    if not master_record.refseq_protein_id:
                        master_record.refseq_protein_id = rs_record.refseq_protein_id
                        master_record.refseq_locus_tag = refseq_locus_tag
                        break

                    logger.error(
                        f"RefSeq: '{refseq_locus_tag}' - Locus Tag: '{ltag}' matched but RefSeq Protein IDs do not match."
                        f" Existing: '{master_record.refseq_protein_id}', New: '{rs_record.refseq_protein_id}'"
                    )

                # If the RefSeq protein IDs match but the locus tags do not
                if ltag != master_record.locus_tag and rs_record_refseq == master_record_refseq:
                    if not master_record.locus_tag:
                        master_record.locus_tag = ltag
                        master_record.refseq_locus_tag = refseq_locus_tag
                        break

                    logger.error(
                        f"RefSeq: '{refseq_locus_tag}' - RefSeq Protein ID: '{rs_record_refseq}' matched but Locus Tags do not match."
                        f" Existing: '{master_record.locus_tag}', New: '{ltag}'"
                    )

    rs_locus_tags = {record.refseq_locus_tag for record in records}
    unpaired_rs = [rs for rs in generic_rows if rs.refseq_locus_tag not in rs_locus_tags]

    for rs_record in unpaired_rs:
        records.append(IdMasterRecord(
            refseq_locus_tag=rs_record.refseq_locus_tag,
            locus_tag=rs_record.locus_tag,
            refseq_protein_id=rs_record.refseq_protein_id
        ))


def main():

    args, logger = setup_config()

    records = []

    try:
        uniprot_ids = read_input(args.uniprot)
        refseq_ids = read_input(args.refseq)
    except FileNotFoundError:
        sys.exit(1)

    logger.info(f"Reading UniProt IDs from: {args.uniprot}")
    parse_uniprot(uniprot_ids, records, logger)
    n_records_uniprot = len(records)
    logger.info(f"After parsing UniProt IDs, {n_records_uniprot} records were created.")

    logger.info(f"Reading RefSeq IDs from: {args.refseq}")
    parse_refseq(refseq_ids, records, logger)
    n_records_refseq = len(records)
    diff_records_refseq = n_records_refseq - n_records_uniprot
    logger.info(f"After parsing RefSeq IDs, {diff_records_refseq} records were added.")



    for record in records:
        try:
            print(record)
        except BrokenPipeError:
            sys.stdin = None
            logger.error("Pipe was broken. Terminating...")
            sys.exit(1)

    sys.exit(0)

if __name__ == "__main__":
    main()




