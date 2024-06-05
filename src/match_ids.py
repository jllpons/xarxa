#!/usr/bin/env python

"""
Creates a ID map file using the IDs from three different inputs: UniProt, RefSeq, and KEGG. 
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
from typing import Optional, Tuple, List, Dict, Set

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


# UniProt Accession -> RefSeq Protein ID
# Type: Dict[str, str | None]
uniprot_to_refseq_protein_id = {}

# UniProt Accession -> Locus Tag
# Type: Dict[str, List[str] | None]
uniprot_to_locus_tag = {}

# UniProt Accession -> KEGG Accession
# Type: Dict[str, List[str] | None]
uniprot_to_kegg_accession = {}

# RefSeq Locus Tag -> RefSeq Protein ID
# Type: Dict[str, str | None]
refseq_locus_tag_to_refseq_protein_id = {}

# RefSeq Locus Tag -> Locus Tag
# Type: Dict[str, List[str] | None]
refseq_locus_tag_to_locus_tag = {}

# NOTE: Bi-directional mapping between locus tags and KEGG accessions
# Locus Tag <-> KEGG Accession
# Type: Dict[str, str | None]
locus_tag_to_kegg_accession = {}

# Type: Set[str]
locus_tag_set = set()

# Type: Set[str]
kegg_accession_set = set()


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


def parse_uniprot(uniprot_ids: str, logger: logging.Logger) -> None:
    """
    Parse the UniProt IDs and update the mapping dictionaries based on the parsed data.

    Parameters
        uniprot_ids (str): The UniProt ID file.
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

        # `foo = bar or []` will default to an empty list if bar is None
        # this way we don't get a TypeError when trying to iterate over a None value
        uniprot_accession = row.uniprot_accession
        locus_tags = row.locus_tag or []
        kegg_accessions = row.kegg_accession or []
        refseq_protein_id = row.refseq_protein_id


        if uniprot_accession in uniprot_to_refseq_protein_id:
            # We can't have duplicate UniProt accessions
            raise IdConflictError(f"Duplicate UniProt accession: {uniprot_accession}")
        uniprot_to_refseq_protein_id[uniprot_accession] = refseq_protein_id


        uniprot_to_locus_tag[uniprot_accession] = locus_tags
        uniprot_to_kegg_accession[uniprot_accession] = kegg_accessions

        # Pair locus tags with matching KEGG accessions
        # E.g.:
        # locus_tags = ['Smlt1234', 'Smlt5678'], kegg_accessions = ['sml:Smlt1234', 'sml:Smlt5678', 'sml:Smlt9101']
        # paired_lt_kegg = [('Smlt1234', 'sml:Smlt1234'), ('Smlt5678', 'sml:Smlt5678'), (None, 'sml:Smlt9101')]
        paired_lt_kegg = pair_lt_kegg(locus_tags, kegg_accessions)

        for ltag, kacc in paired_lt_kegg:
            if ltag:
                locus_tag_to_kegg_accession[ltag] = kacc
                locus_tag_set.add(ltag)
            if kacc:
                locus_tag_to_kegg_accession[kacc] = ltag
                kegg_accession_set.add(kacc)


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


def parse_refseq(refseq_ids: str, logger: logging.Logger) -> None:
    """
    Parse the RefSeq IDs and update the mapping dictionaries based on the parsed data.

    Parameters
        refseq_ids (str): The RefSeq ID file.
        logger (logging.Logger): The logger instance.

    Returns
        None
    """

    schema = {
        "refseq_locus_tag": str,
        "locus_tag": list,
        "refseq_protein_id": str
    }
    generic_rows = parse_tsv(refseq_ids, schema)

    for row in generic_rows:

        refseq_locus_tag = row.refseq_locus_tag
        locus_tags = row.locus_tag or []
        refseq_protein_id = row.refseq_protein_id.split(".")[0] if row.refseq_protein_id else None

        if refseq_locus_tag in refseq_locus_tag_to_refseq_protein_id:
            # We can't have duplicate RefSeq locus tags
            raise IdConflictError(f"Duplicate RefSeq locus tag: {refseq_locus_tag}")

        refseq_locus_tag_to_refseq_protein_id[refseq_locus_tag] = refseq_protein_id

        refseq_locus_tag_to_locus_tag[refseq_locus_tag] = locus_tags

        for lt in locus_tags:
            refseq_locus_tag_to_locus_tag[lt] = refseq_locus_tag
            locus_tag_set.add(lt)


def parse_kegg(kegg_ids: str, logger: logging.Logger) -> None:
    """
    Parse the KEGG IDs and update the mapping dictionaries based on the parsed data.

    Parameters
        kegg_ids (str): The KEGG ID file.
        logger (logging.Logger): The logger instance.

    Returns
        None
    """

    schema = {"kegg_accession": str}
    generic_rows = parse_tsv(kegg_ids, schema)

    for row in generic_rows:

        kegg_accession = row.kegg_accession
        locus_tag = row.kegg_accession.split(":")[1]

        if kegg_accession in locus_tag_to_kegg_accession:
            if locus_tag_to_kegg_accession[kegg_accession] != locus_tag:
                raise IdConflictError(
                    f"Conflicting locus tag for KEGG accession: {kegg_accession} -> {locus_tag_to_kegg_accession[kegg_accession]} vs. {locus_tag}"
                )
            locus_tag_to_kegg_accession[kegg_accession] = locus_tag
            locus_tag_set.add(locus_tag)

        if locus_tag in locus_tag_to_kegg_accession:
            if locus_tag_to_kegg_accession[locus_tag] != kegg_accession:
                raise IdConflictError(
                    f"Conflicting KEGG accession for locus tag: {locus_tag} -> {locus_tag_to_kegg_accession[locus_tag]} vs. {kegg_accession}"
                )
            locus_tag_to_kegg_accession[locus_tag] = kegg_accession
            kegg_accession_set.add(kegg_accession)


def generate_id_map(logger: logging.Logger) -> List[IdMasterRecord]:
    """
    Generate the ID mapping records by correlating data from UniProt, RefSeq, and KEGG.

    Parameters
        logger (logging.Logger): The logger instance.

    Returns
        List[IdMasterRecord]: A list of ID mapping records.

    Raises
        IdConflictError: If a conflict is detected between the IDs.
    """

    records = []

    used_uniprot_accessions = set()
    used_refseq_locus_tags = set()
    used_locus_tags = set()
    used_kegg_accessions = set()

    # Locus Tag -> RefSeq Locus Tag
    # Type: Dict[str, str]
    locus_tag_to_refseq_locus_tag = {}
    for rs_lt, lt_list in refseq_locus_tag_to_locus_tag.items():
        for lt in lt_list:
            locus_tag_to_refseq_locus_tag[lt] = rs_lt

    generate_records_from_uniprot(
            logger,
            records,
            locus_tag_to_refseq_locus_tag,
            used_uniprot_accessions,
            used_refseq_locus_tags,
            used_kegg_accessions,
            used_locus_tags
            )
    generate_records_from_refseq(
            logger,
            records,
            locus_tag_to_refseq_locus_tag,
            used_uniprot_accessions,
            used_refseq_locus_tags,
            used_kegg_accessions,
            used_locus_tags
            )
    generate_records_from_kegg(
            logger,
            records,
            locus_tag_to_refseq_locus_tag,
            used_uniprot_accessions,
            used_refseq_locus_tags,
            used_kegg_accessions,
            used_locus_tags
            )
    generate_records_from_locus_tags(
            logger,
            records,
            locus_tag_to_refseq_locus_tag,
            used_uniprot_accessions,
            used_refseq_locus_tags,
            used_kegg_accessions,
            used_locus_tags
            )

    if any([i for i in refseq_locus_tag_to_refseq_protein_id if i not in used_refseq_locus_tags]):
        logger.error("Some RefSeq locus tags were not used in the ID mapping.")
        raise IdConflictError

    if any([i for i in locus_tag_set if i not in used_locus_tags]):
        logger.error("Some locus tags were not used in the ID mapping.")
        raise IdConflictError

    if any([i for i in kegg_accession_set if i not in used_kegg_accessions]):
        logger.error("Some KEGG accessions were not used in the ID mapping.")
        raise IdConflictError


    return records


def generate_records_from_uniprot(
        logger: logging.Logger,
        records: List[IdMasterRecord],
        locus_tag_to_refseq_locus_tag: Dict[str, str],
        used_uniprot_accessions: Set[str],
        used_refseq_locus_tags: Set[str],
        used_kegg_accessions: Set[str],
        used_locus_tags: Set[str],
        ) -> None:
    """
    Generate records by correlating data from UniProt and RefSeq.

    This function generates `IdMasterRecord` entries by mapping UniProt accessions to their corresponding 
    RefSeq protein IDs, locus tags, and KEGG accessions. It handles discrepancies between UniProt and RefSeq
    protein IDs and ensures that unique associations are maintained.

    Parameters
        logger (logging.Logger): The logger instance.
        records (List[IdMasterRecord]): The list of ID mapping records.
        locus_tag_to_refseq_locus_tag (Dict[str, str]): A dictionary mapping locus tags to RefSeq locus tags.
        used_uniprot_accessions (Set[str]): A set of UniProt accessions that have been used.
        used_refseq_locus_tags (Set[str]): A set of RefSeq locus tags that have been used.
        used_kegg_accessions (Set[str]): A set of KEGG accessions that have been used.
        used_locus_tags (Set[str]): A set of locus tags that have been used.

    Returns
        None

    Raises
        IdConflictError: If a conflict is detected between the IDs.

    Details
    The function iterates over UniProt accessions and performs the following steps:
        1. Maps UniProt accessions to RefSeq protein IDs, locus tags, and KEGG accessions.
        2. If there are no locus tags or KEGG accessions for a UniProt accession, it creates a record with only the RefSeq protein ID.
        3. For each locus tag associated with a UniProt accession:
           - It attempts to map the locus tag to KEGG accession and RefSeq locus tag.
           - If there is a discrepancy between the RefSeq protein ID from UniProt and the RefSeq locus tag, it logs the discrepancy and creates a record without the UniProt accession.
           - It creates a record with the UniProt accession, locus tag, and associated information.
        4. For KEGG accessions not associated with any locus tags, it creates records similarly and ensures unique associations are maintained.
        5. The function ensures all processed accessions and tags are added to the respective sets to avoid duplication.
    """


    for uniprot_accession in uniprot_to_refseq_protein_id:

        # UniProt Accession -> RefSeq Protein ID
        refseq_protein_id = uniprot_to_refseq_protein_id[uniprot_accession]
        # UniProt Accession -> List[Locus Tag]
        locus_tags = uniprot_to_locus_tag[uniprot_accession]
        # UniProt Accession -> List[KEGG Accession]
        kegg_accessions = uniprot_to_kegg_accession[uniprot_accession]


        # No locus tags or KEGG accessions
        if len(locus_tags) == 0 and len(kegg_accessions) == 0:
            records.append(IdMasterRecord(
                uniprot_accession=uniprot_accession,
                refseq_locus_tag=None,
                locus_tag=None,
                kegg_accession=None,
                refseq_protein_id=refseq_protein_id
            ))


        # Generate one record for each locus tag
        for lt in locus_tags:

            kegg_accession = locus_tag_to_kegg_accession.get(lt, None)

            refseq_locus_tag = locus_tag_to_refseq_locus_tag.get(lt, None)

            # Is the RefSeq Protein ID obtained from UniProt is different from
            # the RefSeq Protein ID extracted from the RefSeq file?
            if refseq_locus_tag:
                # Both RefSeq Protein IDs are available so we can compare them
                if refseq_locus_tag_to_refseq_protein_id[refseq_locus_tag] and refseq_protein_id:
                    if refseq_locus_tag_to_refseq_protein_id[refseq_locus_tag] != refseq_protein_id:

                        logger.info(
                            f"Detected different RefSeq Protein IDs between UniProt and RefSeq for locus tag: {lt}"
                        )

                        # Is different,
                        # these locus tags map to different RefSeq Protein IDs
                        records.append(IdMasterRecord(
                            uniprot_accession=None, # UniProt accession is not associated with this RefSeq Protein ID
                            refseq_locus_tag=refseq_locus_tag,
                            locus_tag=lt,
                            kegg_accession=kegg_accession,
                            refseq_protein_id=refseq_locus_tag_to_refseq_protein_id[refseq_locus_tag]
                        ))
                        used_refseq_locus_tags.add(refseq_locus_tag)
                        used_locus_tags.add(lt)
                        used_kegg_accessions.add(kegg_accession)

                        # The RefSeq Locus Tag is not associated with the UniProt accession
                        # (because the RefSeq Protein ID is different)
                        # Will be set to None
                        refseq_locus_tag = None

            # If the RefSeq Protein ID is not available for the UniProt accession,
            # so we try to get it from the RefSeq Locus Tag
            if not refseq_protein_id:
                if refseq_locus_tag:
                    refseq_protein_id = refseq_locus_tag_to_refseq_protein_id.get(refseq_locus_tag, None)


            # Notice that two records are generated for a single locus tag.
            # The first record is generated with the RefSeq Locus Tag and RefSeq Protein ID obtained from the RefSeq file.
            # The second record is generated with the UniProt Accession and RefSeq Protein ID obtained from the UniProt file.
            records.append(IdMasterRecord(
                uniprot_accession=uniprot_accession,
                refseq_locus_tag=refseq_locus_tag,
                locus_tag=lt,
                kegg_accession=kegg_accession,
                refseq_protein_id=refseq_protein_id
            ))
            used_uniprot_accessions.add(uniprot_accession)
            used_refseq_locus_tags.add(refseq_locus_tag)
            used_locus_tags.add(lt)
            used_kegg_accessions.add(kegg_accession)


        # Generate records for KEGG accessions that are not associated with any locus tags
        for kacc in kegg_accessions:

            if kacc in used_kegg_accessions:
                continue

            lt = locus_tag_to_kegg_accession.get(kacc, None)
            refseq_locus_tag = locus_tag_to_refseq_locus_tag.get(lt, None)
            refseq_protein_id = refseq_locus_tag_to_refseq_protein_id.get(refseq_locus_tag, None)

            if refseq_locus_tag:
                if refseq_locus_tag_to_refseq_protein_id[refseq_locus_tag] and refseq_protein_id:
                    if refseq_locus_tag_to_refseq_protein_id[refseq_locus_tag] != refseq_protein_id:
                        logger.info(
                            f"Detected different RefSeq Protein IDs between UniProt and RefSeq for locus tag: {lt}"
                        )

                    records.append(IdMasterRecord(
                        uniprot_accession=None,
                        refseq_locus_tag=refseq_locus_tag,
                        locus_tag=lt,
                        kegg_accession=kacc,
                        refseq_protein_id=refseq_protein_id
                    ))

                    used_refseq_locus_tags.add(refseq_locus_tag)
                    used_locus_tags.add(lt)
                    used_kegg_accessions.add(kacc)

                    refseq_locus_tag = None

            if not refseq_protein_id:
                if refseq_locus_tag:
                    refseq_protein_id = refseq_locus_tag_to_refseq_protein_id.get(refseq_locus_tag, None)

            records.append(IdMasterRecord(
                uniprot_accession=uniprot_accession,
                refseq_locus_tag=refseq_locus_tag,
                locus_tag=lt,
                kegg_accession=kacc,
                refseq_protein_id=refseq_protein_id
            ))

            used_uniprot_accessions.add(uniprot_accession)
            used_refseq_locus_tags.add(refseq_locus_tag)
            used_locus_tags.add(lt)
            used_kegg_accessions.add(kacc)


def generate_records_from_refseq(
        logger: logging.Logger,
        records: List[IdMasterRecord],
        locus_tag_to_refseq_locus_tag: Dict[str, str],
        used_uniprot_accessions: Set[str],
        used_refseq_locus_tags: Set[str],
        used_kegg_accessions: Set[str],
        used_locus_tags: Set[str],
        ) -> None:
    """
    Generate records by correlating data from RefSeq.

    This function generates `IdMasterRecord` entries by mapping RefSeq locus tags to their corresponding 
    RefSeq protein IDs, locus tags, and KEGG accessions. It ensures that unique associations are maintained 
    and prevents duplication of records.

    Parameters
        logger (logging.Logger): The logger instance.
        records (List[IdMasterRecord]): The list of ID mapping records.
        locus_tag_to_refseq_locus_tag (Dict[str, str]): A dictionary mapping locus tags to RefSeq locus tags.
        used_uniprot_accessions (Set[str]): A set of UniProt accessions that have been used.
        used_refseq_locus_tags (Set[str]): A set of RefSeq locus tags that have been used.
        used_kegg_accessions (Set[str]): A set of KEGG accessions that have been used.
        used_locus_tags (Set[str]): A set of locus tags that have been used.

    Returns
        None

    Raises
        IdConflictError: If a conflict is detected between the IDs.

    Details
    The function iterates over RefSeq locus tags and performs the following steps:
        1. Checks if the RefSeq locus tag has already been processed.
        2. Maps RefSeq locus tags to RefSeq protein IDs and locus tags.
        3. If there are no locus tags associated with a RefSeq locus tag, it creates a record with only the RefSeq protein ID.
        4. For each locus tag associated with a RefSeq locus tag:
           - It attempts to map the locus tag to KEGG accession.
           - It creates a record with the RefSeq locus tag, locus tag, and associated information.
        5. The function ensures all processed tags and accessions are added to the respective sets to avoid duplication.
    """


    for refseq_locus_tag in refseq_locus_tag_to_refseq_protein_id:
        if refseq_locus_tag in used_refseq_locus_tags:
            continue

        refseq_protein_id = refseq_locus_tag_to_refseq_protein_id[refseq_locus_tag]
        locus_tags = refseq_locus_tag_to_locus_tag[refseq_locus_tag]

        if len(locus_tags) == 0:
            records.append(IdMasterRecord(
                refseq_locus_tag=refseq_locus_tag,
                locus_tag=None,
                kegg_accession=None,
                refseq_protein_id=refseq_protein_id
            ))
            used_refseq_locus_tags.add(refseq_locus_tag)

        for lt in locus_tags:
            kacc = locus_tag_to_kegg_accession.get(lt, None)

            # Here we don't provide the UniProt accession because we don't have it.
            # If the locus tag or RefSeq Protein ID would have mapped with a UniProt accession,
            # we would have provided it.
            records.append(IdMasterRecord(
                refseq_locus_tag=refseq_locus_tag,
                locus_tag=lt,
                kegg_accession=kacc,
                refseq_protein_id=refseq_protein_id
            ))
            used_refseq_locus_tags.add(refseq_locus_tag)
            used_locus_tags.add(lt)
            used_kegg_accessions.add(kacc)


def generate_records_from_kegg(
        logger: logging.Logger,
        records: List[IdMasterRecord],
        locus_tag_to_refseq_locus_tag: Dict[str, str],
        used_uniprot_accessions: Set[str],
        used_refseq_locus_tags: Set[str],
        used_kegg_accessions: Set[str],
        used_locus_tags: Set[str],
        ) -> None:
    """
    Generate records by correlating data from KEGG, and log any discrepancies found.

    This function generates `IdMasterRecord` entries by mapping KEGG accessions to their corresponding 
    RefSeq locus tags, protein IDs, and UniProt accessions. It ensures that unique associations are maintained 
    and prevents duplication of records.

    Parameters
        logger (logging.Logger): The logger instance.
        records (List[IdMasterRecord]): The list of ID mapping records.
        locus_tag_to_refseq_locus_tag (Dict[str, str]): A dictionary mapping locus tags to RefSeq locus tags.
        used_uniprot_accessions (Set[str]): A set of UniProt accessions that have been used.
        used_refseq_locus_tags (Set[str]): A set of RefSeq locus tags that have been used.
        used_kegg_accessions (Set[str]): A set of KEGG accessions that have been used.
        used_locus_tags (Set[str]): A set of locus tags that have been used.

    Returns
        None

    Raises
        IdConflictError: If a conflict is detected between the IDs.

    Details
    The function iterates over KEGG accessions and performs the following steps:
        1. Checks if the KEGG accession has already been processed.
        2. Maps KEGG accessions to locus tags, RefSeq locus tags, and RefSeq protein IDs.
        3. Attempts to map KEGG accessions to UniProt accessions using the available RefSeq protein IDs.
        4. Creates records with the available information and ensures all processed tags and accessions 
           are added to the respective sets to avoid duplication.
    """

    for kacc in kegg_accession_set:
        if kacc in used_kegg_accessions:
            continue

        lt = locus_tag_to_kegg_accession[kacc]
        refseq_locus_tag = locus_tag_to_refseq_locus_tag.get(lt, None)
        refseq_protein_id = refseq_locus_tag_to_refseq_protein_id.get(refseq_locus_tag, None)
        uniprot_accession = None

        for k, v in uniprot_to_refseq_protein_id.items():
            if v == kacc:
                uniprot_accession = k


        records.append(IdMasterRecord(
            uniprot_accession=uniprot_accession,
            refseq_locus_tag=refseq_locus_tag,
            locus_tag=lt,
            kegg_accession=kacc,
            refseq_protein_id=refseq_protein_id
        ))
        used_uniprot_accessions.add(uniprot_accession)
        used_refseq_locus_tags.add(refseq_locus_tag)
        used_locus_tags.add(lt)
        used_kegg_accessions.add(kacc)


def generate_records_from_locus_tags(
        logger: logging.Logger,
        records: List[IdMasterRecord],
        locus_tag_to_refseq_locus_tag: Dict[str, str],
        used_uniprot_accessions: Set[str],
        used_refseq_locus_tags: Set[str],
        used_kegg_accessions: Set[str],
        used_locus_tags: Set[str],
        ) -> None:
    """
    Generate records by correlating data from locus tags.

    This function generates `IdMasterRecord` entries by mapping locus tags to their corresponding
    RefSeq locus tags, protein IDs, and UniProt accessions. It ensures that unique associations are maintained
    and prevents duplication of records.

    Parameters
        logger (logging.Logger): The logger instance.
        records (List[IdMasterRecord]): The list of ID mapping records.
        locus_tag_to_refseq_locus_tag (Dict[str, str]): A dictionary mapping locus tags to RefSeq locus tags.
        used_uniprot_accessions (Set[str]): A set of UniProt accessions that have been used.
        used_refseq_locus_tags (Set[str]): A set of RefSeq locus tags that have been used.
        used_kegg_accessions (Set[str]): A set of KEGG accessions that have been used.
        used_locus_tags (Set[str]): A set of locus tags that have been used.

    Returns
        None

    Raises
        IdConflictError: If a conflict is detected between the IDs.

    Details
    The function iterates over locus tags and performs the following steps:
    1. Checks if the locus tag has already been processed.
    2. Maps locus tags to KEGG accessions, RefSeq locus tags, and RefSeq protein IDs.
    3. Attempts to map locus tags to UniProt accessions using the available RefSeq protein IDs.
    4. Creates records with the available information and ensures all processed tags and accessions 
       are added to the respective sets to avoid duplication.
    """

    for lt in locus_tag_set:
        if lt not in used_locus_tags:
            kacc = locus_tag_to_kegg_accession.get(lt, None)
            refseq_locus_tag = locus_tag_to_refseq_locus_tag.get(lt, None)
            refseq_protein_id = refseq_locus_tag_to_refseq_protein_id.get(refseq_locus_tag, None)
            uniprot_accession = None

            for k, v in uniprot_to_refseq_protein_id.items():
                if v == refseq_protein_id:
                    uniprot_accession = k

            records.append(IdMasterRecord(
                uniprot_accession=uniprot_accession,
                locus_tag=lt,
                kegg_accession=kacc,
                refseq_locus_tag=refseq_locus_tag,
                refseq_protein_id=refseq_protein_id
            ))
            used_uniprot_accessions.add(uniprot_accession)
            used_locus_tags.add(lt)
            used_kegg_accessions.add(kacc)
            used_refseq_locus_tags.add(refseq_locus_tag)


def main():

    args, logger = setup_config()


    try:
        uniprot_ids = read_input(args.uniprot)
        refseq_ids = read_input(args.refseq)
        kegg_ids = read_input(args.kegg)
    except FileNotFoundError:
        sys.exit(1)


    logger.info(f"Reading UniProt IDs from: {args.uniprot}")
    parse_uniprot(uniprot_ids, logger)
    n_records_uniprot = len(uniprot_to_refseq_protein_id)
    logger.info(f"Read {n_records_uniprot} UniProt records.")

    logger.info(f"Reading RefSeq IDs from: {args.refseq}")
    parse_refseq(refseq_ids, logger)
    n_records_refseq = len(refseq_locus_tag_to_refseq_protein_id)
    logger.info(f"Read {n_records_refseq} RefSeq records.")

    logger.info(f"Reading KEGG IDs from: {args.kegg}")
    parse_kegg(kegg_ids, logger)
    n_records_kegg = len(locus_tag_to_kegg_accession)
    logger.info(f"Read {n_records_kegg // 2} KEGG records.")

    try:
        records = generate_id_map(logger)
    except IdConflictError:
        sys.exit(1)
    n_records = len(records)
    logger.info(f"Generated {n_records} ID mapping records.")
    logger.info("Writing ID mapping records to stdout...")

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




