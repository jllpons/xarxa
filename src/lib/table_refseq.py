#!/usr/bin/env python

"""
This module contains functions to interact with the 'refseq' table in the database.

The 'refseq' table contains the data retrieved from parsing the RefSeq
annotated genome of a given organism.
"""

from dataclasses import dataclass
import logging
from typing import List, Optional

import psycopg2

from lib.db_operations import (
    execute_query,
    create_table_if_not_exists
)
from lib.generic_row import parse_tsv, GenericRow
from lib.schema import (
    TABLE_NAME_REFSEQ,
    TABLE_STRUCTURE_REFSEQ,
    COLUMN_NAME_REFSEQ_LOCUS_TAG,
    COLUMN_NAME_LOCUS_TAG,
    COLUMN_NAME_REFSEQ_ACCESSION,
    COLUMN_NAME_STRAND_LOCATION,
    COLUMN_NAME_START_POSITION,
    COLUMN_NAME_END_POSITION,
    COLUMN_NAME_TRANSLATED_PROTEIN_SEQUENCE,
)


TSV_FORMAT_SCHEMA_REFSEQ = {
    "refseq_locus_tag": str,
    "locus_tag": list,
    "refseq_accession": list,
    "strand_location": str,
    "start_position": str,
    "end_position": str,
    "protein_sequence": str
}

@dataclass
class RefseqRow:

    refseq_locus_tag: str
    strand_location: str
    start_position: str
    end_position: str

    locus_tag: Optional[List[str]] = None
    refseq_accession: Optional[List[str]] = None
    protein_sequence: Optional[str] = None


logger = logging.getLogger(__name__)


def format_data(tab_data: str) -> List[RefseqRow]:
    """
    Given a TSV file, this function parses the data and returns a list of
    RefseqRow objects.

    Args:
        tab_data (str): A string containing the TSV data.

    Returns:
        List[RefseqRow]: A list of RefseqRow objects.
    """

    generic_rows = parse_tsv(tab_data, TSV_FORMAT_SCHEMA_REFSEQ)

    return [r.to_specific_structure(RefseqRow) for r in generic_rows]


def validate_records(records: List[RefseqRow]) -> None:
    """
    Given a list of RefseqRow objects, this function validates the data
    to ensure that there are no duplicate UniProt accessions.

    If a duplicate is found, a ValueError is raised.
    """
    # NOTE: More validation can be added here as needed.

    refseq_locus_tag_set = set()

    for record in records:
        if record.refseq_locus_tag in refseq_locus_tag_set:
            raise ValueError(f"Duplicate RefSeq locus tag found: {record.refseq_locus_tag}")

        refseq_locus_tag_set.add(record.refseq_locus_tag)


def upsert_record(record, conn):
    """
    Given a RefseqRow object, this function upserts the record into the
    corresponding table in the database.

    Args:
        record (RefseqRow): The record to upsert.
        conn: The psycopg2 connection object.

    Returns:
        None

    Raises:
        psycopg2.Error: If there is an error upserting the record.
    """

    query = f"""
INSERT INTO {TABLE_NAME_REFSEQ} (
    {COLUMN_NAME_REFSEQ_LOCUS_TAG},
    {COLUMN_NAME_LOCUS_TAG},
    {COLUMN_NAME_REFSEQ_ACCESSION},
    {COLUMN_NAME_STRAND_LOCATION},
    {COLUMN_NAME_START_POSITION},
    {COLUMN_NAME_END_POSITION},
    {COLUMN_NAME_TRANSLATED_PROTEIN_SEQUENCE}
) VALUES (
    %s, %s, %s, %s, %s, %s, %s
)

ON CONFLICT ({COLUMN_NAME_REFSEQ_LOCUS_TAG})

DO UPDATE SET
    {COLUMN_NAME_LOCUS_TAG} = EXCLUDED.{COLUMN_NAME_LOCUS_TAG},
    {COLUMN_NAME_REFSEQ_ACCESSION} = EXCLUDED.{COLUMN_NAME_REFSEQ_ACCESSION},
    {COLUMN_NAME_STRAND_LOCATION} = EXCLUDED.{COLUMN_NAME_STRAND_LOCATION},
    {COLUMN_NAME_START_POSITION} = EXCLUDED.{COLUMN_NAME_START_POSITION},
    {COLUMN_NAME_END_POSITION} = EXCLUDED.{COLUMN_NAME_END_POSITION},
    {COLUMN_NAME_TRANSLATED_PROTEIN_SEQUENCE} = EXCLUDED.{COLUMN_NAME_TRANSLATED_PROTEIN_SEQUENCE}
"""

    params = (
        record.refseq_locus_tag,
        record.locus_tag,
        record.refseq_accession,
        record.strand_location,
        record.start_position,
        record.end_position,
        record.protein_sequence
    )

    try:
        execute_query(query, conn, params)
    except psycopg2.Error as e:
        logger.error(f"Error upserting record: {record.refseq_locus_tag}")
        raise e



def run_upsert_refseq(
        in_data: str,
        conn: psycopg2.extensions.connection
) -> None:

    """
    Given a string containing TSV data and a psycopg2 connection object, this
    function parses the data, validates it, and upserts the records into the
    'refseq_genome' table in the database.

    Args:
        in_data (str): A string containing the TSV data.
        conn: The psycopg2 connection object.

    Returns:
        None

    Raises:
        ValueError: If there are any validation errors.
        psycopg2.Error: If there are any issues upserting the records.
    """

    logger.info(f"Upserting RefSeq genome data into {TABLE_NAME_REFSEQ} table...")

    logger.info("Parsing input data...")
    records = format_data(in_data)
    logger.info(f"Succesfully parsed {len(records)} records")

    logger.info("Validating records...")
    validate_records(records)
    logger.info("Successfully validated records")

    create_table_if_not_exists(
            TABLE_NAME_REFSEQ,
            TABLE_STRUCTURE_REFSEQ,
            conn
            )

    raise NotImplementedError("Need to implement upsert_record function")

    conn.commit()

