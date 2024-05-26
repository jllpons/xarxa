#!/usr/bin/env python3

"""
This module contains functions to interact with the `kegg` table in the database.

The `kegg` table contains information about KEGG accessions, pathways, and KOs
of a given organism.
"""

from dataclasses import dataclass
import logging
from typing import List, Optional

import psycopg2

from lib.db_operations import (
    execute_query, 
    create_table_if_not_exists
)
from lib.generic_row import parse_tsv
from lib.schema import (
    TABLE_NAME_KEGG,
    TABLE_STRUCTURE_KEGG,
    COLUMN_NAME_KEGG_ACCESSION,
    COLUMN_NAME_KEGG_PATHWAY,
    COLUMN_NAME_KEGG_ORTHOLOGY
)


TSV_FORMAT_SCHEMA_KEGG = {
    "kegg_accession": str,
    "kegg_pathway": list,
    "kegg_orthology": list
}

@dataclass
class KeggRecord:

    kegg_accession: str

    kegg_pathway: Optional[List[str]] = None
    kegg_orthology: Optional[List[str]] = None


logger = logging.getLogger(__name__)


def format_data(tab_data: str) -> List[KeggRecord]:
    """
    Given a TSV file, this function parses the data and returns a list of
    KeggRecord objects.

    Args:
        tab_data: A string containing the TSV data

    Returns:
        List[KeggRecord]
    """

    generic_rows = parse_tsv(tab_data, TSV_FORMAT_SCHEMA_KEGG)

    rows = [r.to_specific_structure(KeggRecord) for r in generic_rows]

    return rows


def validate_records(records: List[KeggRecord]) -> None:
    """
    Given a list of UniprotIdsRow objects, this function validates the records
    to ensure that there are no duplicate UniProt accessions.

    If a duplicate is found, a ValueError is raised.
    """
    # NOTE: More validation can be added here as needed.

    kegg_accessions = set()

    for record in records:
        if record.kegg_accession in kegg_accessions:
            logger.error(f"Duplicate KEGG Accession: {record.kegg_accession}")
            raise ValueError

        kegg_accessions.add(record.kegg_accession)


def upsert_record(record: KeggRecord,
                  conn: psycopg2.extensions.connection) -> None:
    """
    Given a KeggRecord object, this function upserts the record into the
    corresponding table in the database.

    Args:
        record: A KeggRecord object
        conn: A psycopg2 connection object

    Returns:
        None

    Raises:
        psycopg2.Error: If an error occurs during the upsert operation
    """

    query = f"""
INSERT INTO {TABLE_NAME_KEGG} (
    {COLUMN_NAME_KEGG_ACCESSION},
    {COLUMN_NAME_KEGG_PATHWAY},
    {COLUMN_NAME_KEGG_ORTHOLOGY}
) VALUES (
    %s, %s, %s
)
ON CONFLICT ({COLUMN_NAME_KEGG_ACCESSION})

DO UPDATE SET
    {COLUMN_NAME_KEGG_PATHWAY} = EXCLUDED.{COLUMN_NAME_KEGG_PATHWAY},
    {COLUMN_NAME_KEGG_ORTHOLOGY} = EXCLUDED.{COLUMN_NAME_KEGG_ORTHOLOGY}
"""

    params = (
        record.kegg_accession,
        record.kegg_pathway,
        record.kegg_orthology
    )

    try:
        execute_query(query, conn, params)
    except psycopg2.Error as e:
        logger.error(f"Error upserting record: {record.kegg_accession}")
        raise e


def run_upsert_kegg(
        in_data: str,
        conn: psycopg2.extensions.connection
        ) -> None:

    """
    Given a string containing TSV data, this function parses the data, validates it,
    and upserts it into the `kegg` table in the database.

    Args:
        in_data: A string containing TSV data
        conn: A psycopg2 connection object

    Returns:
        None

    Raises:
        ValueError: If a duplicate UniProt accession is found
        psycopg2.Error: If an error occurs during the upsert operation
    """

    logger.info(f"Upserting KEGG Organism data into {TABLE_NAME_KEGG} table...")

    logger.debug("Parsing input data...")


    records = format_data(in_data)
    logger.info(f"Succesfully parsed {len(records)} records")

    logger.info("Validating records...")
    validate_records(records)
    logger.info("Successfully validated records")

    create_table_if_not_exists(
        TABLE_NAME_KEGG,
        TABLE_STRUCTURE_KEGG,
        conn
    )

    raise NotImplementedError("Implement the upsert_record function")

    conn.commit()




