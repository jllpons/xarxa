#!/usr/bin/env python3

"""
This module contains functions to interact with the 'id_mapper' table in the database.

The 'id_mapper' table is used to map different types of IDs used in the database.
"""

from dataclasses import dataclass
import logging
from typing import List, Optional

import psycopg2

from lib.schema import (
    TABLE_NAME_ID_MAPPER,
    TABLE_STRUCTURE_ID_MAPPER,
    COLUMN_NAME_UNIPROT_ACCESSION,
    COLUMN_NAME_REFSEQ_LOCUS_TAG,
    COLUMN_NAME_LOCUS_TAG,
    COLUMN_NAME_KEGG_ACCESSION,
    COLUMN_NAME_REFSEQ_PROTEIN_ID,
    TABLE_INDEX_ID_MAPPER,
)
from lib.db_operations import execute_query, create_table_if_not_exists
from lib.generic_row import parse_tsv


TSV_FORMAT_SCHEMA_ID_MAPPER = {
    "uniprot_accession": str,
    "refseq_locus_tag": str,
    "locus_tag": str,
    "kegg_accession": str,
    "refseq_protein_id": str,
}


@dataclass
class IdMapperRecord:
    uniprot_accession: Optional[str] = None
    refseq_locus_tag: Optional[str] = None
    locus_tag: Optional[str] = None
    kegg_accession: Optional[str] = None
    refseq_protein_id: Optional[str] = None


logger = logging.getLogger(__name__)


def format_data(tab_data: str) -> List[IdMapperRecord]:
    """
    Given a TSV file, this function parses the data and returns a list of
    IdMapperRecord objects.

    Args:
        tab_data: A string containing the TSV data

    Returns:
        List[IdMapperRecord]: A list of IdMapperRecord objects
    """

    generic_rows = parse_tsv(tab_data, TSV_FORMAT_SCHEMA_ID_MAPPER)

    rows = [r.to_specific_structure(IdMapperRecord) for r in generic_rows]

    return rows


def upsert_record(record: IdMapperRecord, conn: psycopg2.extensions.connection) -> None:
    """
    Given a IdMapperRecord object, this function upserts the record into the
    corresponding table in the database.

    Args:
        record: A IdMapperRecord object
        conn: A psycopg2 connection object

    Returns:
        None

    Raises:
        psycopg2.Error: If an error occurs during the upsert operation
    """

    query = f"""
INSERT INTO {TABLE_NAME_ID_MAPPER} (
    {COLUMN_NAME_UNIPROT_ACCESSION},
    {COLUMN_NAME_REFSEQ_LOCUS_TAG},
    {COLUMN_NAME_LOCUS_TAG},
    {COLUMN_NAME_KEGG_ACCESSION},
    {COLUMN_NAME_REFSEQ_PROTEIN_ID}
) VALUES (%s, %s, %s, %s, %s)
"""

    params = (
        record.uniprot_accession,
        record.refseq_locus_tag,
        record.locus_tag,
        record.kegg_accession,
        record.refseq_protein_id,
    )

    try:
        execute_query(query, conn, params)
    except psycopg2.Error as e:
        logger.error(f"Error upserting record: {record}")
        raise e


def run_upsert_id_mapper(
        in_data: str,
        conn: psycopg2.extensions.connection,
        ) -> None:
    """
    Given a TSV file containing IdMapper data, this function parses
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

    logger.info("Upserting IdMapper data...")

    logger.debug("Parsing input data...")
    records = format_data(in_data)
    logger.info(f"Succesfully parsed {len(records)} records")

#     logger.info("Validating records...")
#     validate_records(records)
#     logger.info("Successfully validated records")

    create_table_if_not_exists(
        TABLE_NAME_ID_MAPPER,
        TABLE_STRUCTURE_ID_MAPPER,
        conn,
    )

    logger.info("Creating indexes...")
    execute_query(TABLE_INDEX_ID_MAPPER, conn)
    logger.info("Successfully created indexes")


    for record in records:

        try:
            upsert_record(record, conn)
        except psycopg2.Error as e:
            logger.error(f"Error upserting record: {record}")
            logger.error(e)
            conn.rollback()
            raise e

    conn.commit()


