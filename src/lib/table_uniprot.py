#!/usr/bin/env python3

"""
This module contains functions to interact with the 'uniprot' table in the
database.

The `uniprot` table contains information related from all of the UniProtKB entries
for a given organism.
"""

from dataclasses import dataclass
import logging
from typing import List, Optional

import psycopg2

from lib.schema import (
    TABLE_NAME_UNIPROT,
    TABLE_STRUCTURE_UNIPROT,
    COLUMN_NAME_UNIPROT_ACCESSION,
    COLUMN_NAME_LOCUS_TAG,
    COLUMN_NAME_ORF_NAME,
    COLUMN_NAME_KEGG_ACCESSION,
    COLUMN_NAME_EMBL_PROTEIN_ID,
    COLUMN_NAME_REFSEQ_ACCESSION,
    COLUMN_NAME_KEYWORDS,
    COLUMN_NAME_PROTEIN_NAME,
    COLUMN_NAME_PROTEIN_EXISTENCE,
    COLUMN_NAME_SEQUENCE,
    COLUMN_NAME_GO_TERM,
    COLUMN_NAME_EC_NUMBER,
    COLUMN_NAME_POST_TRANSLATIONAL_MODIFICATION,
)
from lib.db_operations import execute_query, create_table_if_not_exists
from lib.generic_row import parse_tsv


TSV_FORMAT_SCHEMA_UNIPROT_PROTEIN = {
    "uniprot_accession": str,
    "locus_tag": list,
    "orf_name": list,
    "kegg_accession": list,
    "embl_protein_id": str,
    "refseq_accession": str,
    "keywords": list,
    "protein_name": str,
    "protein_existence": str,
    "sequence": str,
    "go_term": list,
    "ec_number": list,
    "post_translational_modification": dict,
}


@dataclass
class UniprotRecord:
    uniprot_accession: str

    locus_tag: Optional[List[str]] = None
    orf_name: Optional[List[str]] = None
    kegg_accession: Optional[List[str]] = None
    embl_protein_id: Optional[str] = None
    refseq_accession: Optional[str] = None
    keywords: Optional[List[str]] = None
    protein_name: Optional[str] = None
    protein_existence: Optional[str] = None
    sequence: Optional[str] = None
    go_term: Optional[List[str]] = None
    ec_number: Optional[List[str]] = None
    post_translational_modification: Optional[dict] = None


logger = logging.getLogger(__name__)


def format_data(tab_data: str) -> List[UniprotRecord]:
    """
    Given a TSV file, this function parses the data and returns a list of
    UniprotRecord objects.

    Args:
        tab_data: A string containing the TSV data

    Returns:
        List[UniprotRecord]
    """

    generic_rows = parse_tsv(tab_data, TSV_FORMAT_SCHEMA_UNIPROT_PROTEIN)

    rows = [r.to_specific_structure(UniprotRecord) for r in generic_rows]

    return rows


def validate_records(records: List[UniprotRecord]) -> None:
    """
    Given a list of UniprotRecord objects, this function validates the records
    to ensure that there are no duplicate gene IDs.

    If a duplicate is found, a ValueError is raised.
    """
    # NOTE: More validation can be added here as needed.

    uniport_accessions = set()

    for record in records:

        if record.uniprot_accession in uniport_accessions:
            logger.error(f"Duplicate UniProt accession found: {record.uniprot_accession}")
            raise ValueError

        uniport_accessions.add(record.uniprot_accession)


def upsert_record(record: UniprotRecord, conn: psycopg2.extensions.connection) -> None:
    """
    Given a UniprotRecord object, this function upserts the record into the
    corresponding table in the database.

    Args:
        record: A UniprotRecord object
        conn: A psycopg2 connection object

    Returns:
        None

    Raises:
        psycopg2.Error: If an error occurs during the upsert operation
    """

    query = f"""
INSERT INTO {TABLE_NAME_UNIPROT} (
    {COLUMN_NAME_UNIPROT_ACCESSION},
    {COLUMN_NAME_LOCUS_TAG},
    {COLUMN_NAME_ORF_NAME},
    {COLUMN_NAME_KEGG_ACCESSION},
    {COLUMN_NAME_EMBL_PROTEIN_ID},
    {COLUMN_NAME_REFSEQ_ACCESSION},
    {COLUMN_NAME_KEYWORDS},
    {COLUMN_NAME_PROTEIN_NAME},
    {COLUMN_NAME_PROTEIN_EXISTENCE},
    {COLUMN_NAME_SEQUENCE},
    {COLUMN_NAME_GO_TERM},
    {COLUMN_NAME_EC_NUMBER},
    {COLUMN_NAME_POST_TRANSLATIONAL_MODIFICATION}
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
)
ON CONFLICT ({COLUMN_NAME_UNIPROT_ACCESSION})

DO UPDATE SET
    {COLUMN_NAME_LOCUS_TAG} = EXCLUDED.{COLUMN_NAME_LOCUS_TAG},
    {COLUMN_NAME_ORF_NAME} = EXCLUDED.{COLUMN_NAME_ORF_NAME},
    {COLUMN_NAME_KEGG_ACCESSION} = EXCLUDED.{COLUMN_NAME_KEGG_ACCESSION},
    {COLUMN_NAME_EMBL_PROTEIN_ID} = EXCLUDED.{COLUMN_NAME_EMBL_PROTEIN_ID},
    {COLUMN_NAME_REFSEQ_ACCESSION} = EXCLUDED.{COLUMN_NAME_REFSEQ_ACCESSION},
    {COLUMN_NAME_KEYWORDS} = EXCLUDED.{COLUMN_NAME_KEYWORDS},
    {COLUMN_NAME_PROTEIN_NAME} = EXCLUDED.{COLUMN_NAME_PROTEIN_NAME},
    {COLUMN_NAME_PROTEIN_EXISTENCE} = EXCLUDED.{COLUMN_NAME_PROTEIN_EXISTENCE},
    {COLUMN_NAME_SEQUENCE} = EXCLUDED.{COLUMN_NAME_SEQUENCE},
    {COLUMN_NAME_GO_TERM} = EXCLUDED.{COLUMN_NAME_GO_TERM},
    {COLUMN_NAME_EC_NUMBER} = EXCLUDED.{COLUMN_NAME_EC_NUMBER},
    {COLUMN_NAME_POST_TRANSLATIONAL_MODIFICATION} = EXCLUDED.{COLUMN_NAME_POST_TRANSLATIONAL_MODIFICATION}
"""

    params = (
        record.uniprot_accession,
        record.locus_tag,
        record.orf_name,
        record.kegg_accession,
        record.embl_protein_id,
        record.refseq_accession,
        record.keywords,
        record.protein_name,
        record.protein_existence,
        record.sequence,
        record.go_term,
        record.ec_number,
    )

    try:
        execute_query(query, conn, params)
    except psycopg2.Error as e:
        logger.error(f"Error upserting record: {record.uniprot_accession}")
        raise e


def run_upsert_uniprot(
        in_data: str,
        conn: psycopg2.extensions.connection,
        ) -> None:
    """
    Given a TSV file containing UniProt Protein data, this function parses
    the data, validates the records, and upserts them into the database.

    Args:
        in_data: A string containing the TSV data
        conn: A psycopg2 connection object

    Returns:
        None

    Raises:
        ValueError: If a duplicate UniProt accession is found
        psycopg2.Error: If an error occurs during the upsert operation
    """

    logger.info(f"Upserting UniProt Protein data into the '{TABLE_NAME_UNIPROT}' table...")

    logger.debug("Parsing input data...")
    records = format_data(in_data)
    logger.info(f"Succesfully parsed {len(records)} records")

    logger.info("Validating records...")
    validate_records(records)
    logger.info("Successfully validated records")

    create_table_if_not_exists(
        TABLE_NAME_UNIPROT,
        TABLE_STRUCTURE_UNIPROT,
        conn,
    )

    raise NotImplementedError

    conn.commit()


