#!/usr/bin/env python3

"""
For each protein, check if the stored sequences (which have been derived from different sources)
match each other. If they do not match, print a warning message with detailed information.
A pairwise alignment is performed to compare the both sequences.

Beware that the pairwise alignment functionallity from Biopython is far from perfect.
If you see odd alignments, please check <https://www.ebi.ac.uk/jdispatcher/psa/emboss_water>
and <https://www.ebi.ac.uk/Tools/psa/emboss_needle/>.

We can consider two types of mismatches:
    1. Missmatch where one protein has some more amino acids that the other on the N-terminal or C-terminal.
       This type of missmatch is usually caused by the use of different annotation tools.
       Also, since Bacteria don't usually have a single start codon, this type of missmatch is common.
       Will still be printed just in case.

    2. Missmatch where a gap/multiple gaps are present in one of the sequences when compared to the other.
       This type of missmatch is the most important one, as it indicates that the sequences may not be the same.
"""

import argparse
from dataclasses import dataclass
import sys
import logging
from typing import List, Optional

from Bio import Align
import psycopg2

from lib.db_operations import (
    TABLE_NAME_ID_MASTER,
    TABLE_NAME_REFSEQ_GENOME,
    TABLE_NAME_UNIPROT_PROTEIN,
    TABLE_NAME_PROTEOMICS_QUANTIFICATION,
    connect_to_db,
    )
from lib.cli import CustomHelpFormatter, setup_logger
from lib.config import get_database_connection_string
from lib.table_id_master import (
    COLUMN_NAME_MASTER_KEY,
    COLUMN_NAME_UNIPROT_ACCESSION,
    COLUMN_NAME_REFSEQ_LOCUS_TAG,
    COLUMN_NAME_LOCUS_TAG,
    COLUMN_NAME_REFSEQ_ACCESSION,
    COLUMN_NAME_EMBL_PROTEIN_ID,
    COLUMN_NAME_KEGG_ACCESSION
)
from lib.table_refseq_genome import COLUMN_NAME_PROTEIN_SEQUENCE as COLUMN_NAME_REFSEQ_PROTEIN_SEQUENCE
from lib.table_uniprot_protein import COLUMN_NAME_PROTEIN_SEQUENCE as COLUMN_NAME_UNIPROT_PROTEIN_SEQUENCE
from lib.table_proteomics_peptide_ptm import COLUMN_NAME_SEQUENCE as COLUMN_NAME_PROTEOMICS_SEQUENCE


@dataclass
class Ids:
    uniprot_accession: Optional[str] = None
    refseq_locus_tag: Optional[List[str]] = None
    locus_tag: Optional[List[str]] = None
    refseq_accession: Optional[List[str]] = None
    embl_protein_id: Optional[str] = None
    kegg_accession: Optional[List[str]] = None


@dataclass
class Sequences:
    refseq_genome_sequence: Optional[str] = None
    uniprot_sequence: Optional[str] = None
    proteomics_sequence: Optional[str] = None


    def can_be_compared(self) -> bool:
        """
        Returns True if at least two sequences are present.
        """
        return sum([1 for s in self.__dict__.values() if s]) > 1


    def without_met(self, sequence: str) -> str:
            """
            Returns the sequence without the first amino acid (if it is a methionine).

            Args:
                sequence: The amino acid sequence.

            Returns:
                The sequence without the first amino acid if it is a methionine, otherwise the original sequence.
            """
            return sequence[1:] if sequence and sequence[0] == "M" else sequence


@dataclass
class Record:
    master_key: str
    ids: Ids
    sequences: Sequences


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
        usage="%(prog)s [options]",
        description=__doc__,
    )

    parser._optionals.title = "Options"
    parser.add_argument("-h", "--help",
                        action="help",
                        default=argparse.SUPPRESS,
                        help="Display this help message and exit.")
    parser.add_argument("--db",
                        metavar="STR",
                        type=str,
                        help="Database connection string. Reads from 'config/database.json' if unspecified.")
    parser.add_argument("--log",
                        metavar="<level>",
                        type=str,
                        default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        help="Set the logging level (default: INFO).")

    return parser


def retrieve_records(conn: psycopg2.extensions.connection) -> List[Record]:
    """
    Retrieve all records from the ID_MASTER table. Each record contains the master key and
    the IDs of the protein. Records are parsed into a list of Record objects.

    Args:
        conn: The database connection.

    Returns:
        A list of Record objects.
    """

    query = f"""
SELECT
    {COLUMN_NAME_MASTER_KEY},
    {COLUMN_NAME_UNIPROT_ACCESSION},
    {COLUMN_NAME_REFSEQ_LOCUS_TAG},
    {COLUMN_NAME_LOCUS_TAG},
    {COLUMN_NAME_REFSEQ_ACCESSION},
    {COLUMN_NAME_EMBL_PROTEIN_ID},
    {COLUMN_NAME_KEGG_ACCESSION}
FROM {TABLE_NAME_ID_MASTER}
    """

    with conn.cursor() as cur:
        cur.execute(query)
        ids = cur.fetchall()

    out = []

    for (
        master_key,
        uniprot_accession,
        refseq_locus_tag,
        locus_tag,
        refseq_accession,
        embl_protein_id,
        kegg_accession
    ) in ids:

        out.append(
            Record(
                master_key,
                Ids(
                    uniprot_accession,
                    refseq_locus_tag,
                    locus_tag,
                    refseq_accession,
                    embl_protein_id,
                    kegg_accession
                ),
                Sequences()
            )
        )

    return out


def assign_sequences(conn: psycopg2.extensions.connection, records: List[Record]) -> List[Record]:
    """
    Try to retrieve the sequences for each record from the database and assign them to the
    corresponding Record object.
    If the sequences can be compared (i.e. at least two sequences are present), the Record object
    is added to the list of records to compare, which is returned.

    Args:
        conn: The database connection.
        records: A list of Record objects.

    Returns:
        A list of Record objects that have at least two sequences that can be compared.
    """

    records_to_compare = []
    for r in records:

        r.sequences.refseq_genome_sequence = retrieve_refseq_genome_sequence(conn, r.master_key)
        r.sequences.uniprot_sequence = retrieve_uniprot_genome_sequence(conn, r.master_key)
        r.sequences.proteomics_sequence = retrieve_proteomics_sequence(conn, r.master_key)

        if r.sequences.can_be_compared():
            records_to_compare.append(r)

    return records_to_compare


def retrieve_refseq_genome_sequence(conn: psycopg2.extensions.connection, key: str) -> str | None:

    query = f"""
SELECT
    {COLUMN_NAME_REFSEQ_PROTEIN_SEQUENCE}
FROM {TABLE_NAME_REFSEQ_GENOME}
WHERE {COLUMN_NAME_MASTER_KEY} = '{key}'
    """

    with conn.cursor() as cur:
        cur.execute(query)
        sequence = cur.fetchone()

    return sequence[0] if sequence else None


def retrieve_uniprot_genome_sequence(conn: psycopg2.extensions.connection, key: str) -> str | None:

        query = f"""
SELECT
    {COLUMN_NAME_UNIPROT_PROTEIN_SEQUENCE}
FROM {TABLE_NAME_UNIPROT_PROTEIN}
WHERE {COLUMN_NAME_MASTER_KEY} = '{key}'
    """

        with conn.cursor() as cur:
            cur.execute(query)
            sequence = cur.fetchone()

        return sequence[0] if sequence else None


def retrieve_proteomics_sequence(conn: psycopg2.extensions.connection, key: str) -> str | None:

    query = f"""
SELECT
    {COLUMN_NAME_PROTEOMICS_SEQUENCE}
FROM {TABLE_NAME_PROTEOMICS_QUANTIFICATION}
WHERE {COLUMN_NAME_MASTER_KEY} = '{key}'
    """

    with conn.cursor() as cur:
        cur.execute(query)
        sequence = cur.fetchone()

    return sequence[0] if sequence else None


def compare_sequences(logger, record, source1, source2, sequence1, sequence2):

    log_msg = """
Found mismatched sequences between **{source1}** and **{source2}**.

Identifiers:
    - UniProt Accession: {uniprot_accession}
    - RefSeq Locus Tag: {refseq_locus_tag}
    - Locus Tag: {locus_tag}
    - RefSeq Accession: {refseq_accession}
    - EMBL Protein ID: {embl_protein_id}
    - KEGG Accession: {kegg_accession}

Sequences:
{source1}:
{sequence1}
{source2}:
{sequence2}

Protein alignment:
    - Query: {source1}
    - Target: {source2}
    - Score: {alignments[0].score}

{alignments[0]}
"""

    if record.sequences.without_met(sequence1) != record.sequences.without_met(sequence2):

        aligner = Align.PairwiseAligner()
        alignments = aligner.align(
            record.sequences.without_met(sequence2),
            record.sequences.without_met(sequence1)
        )

        logger.warning(log_msg.format(
            source1=source1,
            source2=source2,
            uniprot_accession=record.ids.uniprot_accession,
            refseq_locus_tag=record.ids.refseq_locus_tag,
            locus_tag=record.ids.locus_tag,
            refseq_accession=record.ids.refseq_accession,
            embl_protein_id=record.ids.embl_protein_id,
            kegg_accession=record.ids.kegg_accession,
            sequence1=sequence1,
            sequence2=sequence2,
            alignments=alignments
        ))


def main():

    parser = setup_argparse()
    args = parser.parse_args()

    logger = setup_logger(args.log)

    if len(sys.argv) == 1 and not sys.stdin.isatty():
        parser.print_help()
        sys.exit(1)

    logger.info(f"Arguments: {vars(args)}")
    logger.info("Checking protein sequences...")

    if not args.db:
        try:
            args.db = get_database_connection_string()
        except (FileNotFoundError, KeyError):
            sys.exit(1)

    # Connect to the database
    try:
        conn = connect_to_db(args.db)
    except psycopg2.Error:
        sys.exit(1)

    records = retrieve_records(conn)
    logger.info(f"Retrieved {len(records)} records from the database.")

    records_to_compare = assign_sequences(conn, records)
    logger.info(f"Found {len(records_to_compare)} records with at least two sequences to compare.")
    logger.info("Comparing sequences...")

    for r in records_to_compare:

        match (
            isinstance(r.sequences.refseq_genome_sequence, str),
            isinstance(r.sequences.uniprot_sequence, str),
            isinstance(r.sequences.proteomics_sequence, str)
        ):

            case (True, True, False):

                if r.sequences.without_met(r.sequences.refseq_genome_sequence) \
                != r.sequences.without_met(r.sequences.uniprot_sequence):

                    compare_sequences(
                        logger,
                        r,
                        "RefSeq Genome Protein Sequence",
                        "UniProtKB Genome Protein Sequence",
                        r.sequences.refseq_genome_sequence,
                        r.sequences.uniprot_sequence
                    )


            case (True, False, True):

                if r.sequences.without_met(r.sequences.refseq_genome_sequence) \
                != r.sequences.without_met(r.sequences.proteomics_sequence):

                    compare_sequences(
                        logger,
                        r,
                        "RefSeq Genome Protein Sequence",
                        "Proteomics Protein Sequence",
                        r.sequences.refseq_genome_sequence,
                        r.sequences.proteomics_sequence
                    )


            case (False, True, True):
                if r.sequences.without_met(r.sequences.uniprot_sequence) \
                != r.sequences.without_met(r.sequences.proteomics_sequence):

                    compare_sequences(
                        logger,
                        r,
                        "UniProtKB Genome Protein Sequence",
                        "Proteomics Protein Sequence",
                        r.sequences.uniprot_sequence,
                        r.sequences.proteomics_sequence
                    )


            case (True, True, True):

                if r.sequences.without_met(r.sequences.refseq_genome_sequence) \
                != r.sequences.without_met(r.sequences.uniprot_sequence):

                    compare_sequences(
                        logger,
                        r,
                        "RefSeq Genome Protein Sequence",
                        "UniProtKB Genome Protein Sequence",
                        r.sequences.refseq_genome_sequence,
                        r.sequences.uniprot_sequence
                    )

                if r.sequences.without_met(r.sequences.refseq_genome_sequence) \
                != r.sequences.without_met(r.sequences.proteomics_sequence):


                    compare_sequences(
                        logger,
                        r,
                        "RefSeq Genome Protein Sequence",
                        "Proteomics Protein Sequence",
                        r.sequences.refseq_genome_sequence,
                        r.sequences.proteomics_sequence
                    )

                if r.sequences.without_met(r.sequences.uniprot_sequence) \
                != r.sequences.without_met(r.sequences.proteomics_sequence):

                    compare_sequences(
                        logger,
                        r,
                        "UniProtKB Genome Protein Sequence",
                        "Proteomics Protein Sequence",
                        r.sequences.uniprot_sequence,
                        r.sequences.proteomics_sequence
                    )

            case _:
                continue

    conn.close()


if __name__ == "__main__":
    main()
