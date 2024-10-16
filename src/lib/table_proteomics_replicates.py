#!/usr/bin/env python

"""
This module contains functions to interact with the 'proteomics_replicates' table in the database.

The 'proteomics_replicates' table contains the intesity values for each protein in each replicate.
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
    TABLE_NAME_PROTEOMICS_REPLICATES,
    TABLE_STRUCTURE_PROTEOMICS_REPLICATES,
    COLUMN_NAME_EXPERIMENTAL_ID,
    COLUMN_NAME_EXPERIMENTAL_CONDITION_NAME,
    COLUMN_NAME_REPLICATE,
    COLUMN_NAME_PEPTIDE_SEQUENCE,
    COLUMN_NAME_PEPTIDE_POSITIONS,
    COLUMN_NAME_PEPTIDE_PTMS,
    COLUMN_NAME_INTENSITY,
)


TSV_FORMAT_SCHEMA_PROTEOMICS_REPLICATES = {
    "experimental_id": str,
    "peptide_sequence": str,
    "peptide_positions": str,
    "peptide_ptms": str,
    "intensity": float
}

@dataclass
class ProteomicsReplicatesRecord:

    experimental_id: str
    peptide_sequence: str
    peptide_positions: str
    peptide_ptms: str
    intensity: float

    experimental_condition_name: Optional[str] = None
    replicate: Optional[int] = None

logger = logging.getLogger(__name__)


def format_data(tab_data: str) -> List[ProteomicsReplicatesRecord]:
    """
    Given a TSV file, this function parses the data and returns a list of
    ProteomicsReplicatesRecord objects.

    Args:
        tab_data (str): A string containing the TSV data.

    Returns:
        List[ProteomicsReplicatesRecord]: A list of ProteomicsReplicatesRecord objects.
    """

    generic_rows = parse_tsv(tab_data, TSV_FORMAT_SCHEMA_PROTEOMICS_REPLICATES)

    return [r.to_specific_structure(ProteomicsReplicatesRecord) for r in generic_rows]


def validate_records(records: List[ProteomicsReplicatesRecord]) -> None:
    """
    Given a list of ProteomicsReplicatesRecord objects, this function validates the data
    to ensure there are no duplicates or other validation rules.

    Args:
        records (List[ProteomicsReplicatesRecord]): The list of records to validate.

    Raises:
        ValueError: If there are validation errors.
    """
    pass


def upsert_record(
        record: ProteomicsReplicatesRecord,
        conn: psycopg2.extensions.connection) -> None:
    """
    Given a ProteomicsReplicatesRecord object, this function upserts the record into the
    corresponding table in the database.

    Args:
        record (ProteomicsReplicatesRecord): The record to upsert.
        conn: The psycopg2 connection object.

    Returns:
        None

    Raises:
        psycopg2.Error: If there is an error upserting the record.
    """

    query = f"""
INSERT INTO {TABLE_NAME_PROTEOMICS_REPLICATES} (
    {COLUMN_NAME_EXPERIMENTAL_ID},
    {COLUMN_NAME_EXPERIMENTAL_CONDITION_NAME},
    {COLUMN_NAME_REPLICATE},
    {COLUMN_NAME_PEPTIDE_SEQUENCE},
    {COLUMN_NAME_PEPTIDE_POSITIONS},
    {COLUMN_NAME_PEPTIDE_PTMS},
    {COLUMN_NAME_INTENSITY}
) VALUES (
    %s, %s, %s, %s, %s, %s, %s
)
"""

    params = (
        record.experimental_id,
        record.experimental_condition_name,
        record.replicate,
        record.peptide_sequence,
        record.peptide_positions,
        record.peptide_ptms,
        record.intensity
    )

    try:
        execute_query(query, conn, params)
    except psycopg2.Error as e:
        logger.error(f"Error upserting record: {record.experimental_id}")
        raise e


def run_upsert_proteomics_replicates(
        in_data: str,
        experimental_condition_name: str,
        replicate: int,
        conn: psycopg2.extensions.connection
) -> None:
    """
    Given a string containing TSV data and a psycopg2 connection object, this
    function parses the data, validates it, and upserts the records into the
    'proteomics' table in the database.

    Args:
        in_data (str): A string containing the TSV data.
        experimental_condition_name (str): The experimental condition name.
        replicate (int): The replicate number.
        conn: The psycopg2 connection object.

    Returns:
        None

    Raises:
        ValueError: If there are any validation errors.
        psycopg2.Error: If there are any issues upserting the records.
    """

    logger.info(f"Upserting data into {TABLE_NAME_PROTEOMICS_REPLICATES} table...")

    logger.info("Parsing input data...")
    records = format_data(in_data)
    logger.info(f"Succesfully parsed {len(records)} records")

    logger.info("Validating records...")
    validate_records(records)
    logger.info("Successfully validated records")

    create_table_if_not_exists(
            TABLE_NAME_PROTEOMICS_REPLICATES,
            TABLE_STRUCTURE_PROTEOMICS_REPLICATES,
            conn
    )

    logger.info("Upserting records...")
    for record in records:

        record.experimental_condition_name = experimental_condition_name
        record.replicate = replicate

        try:
            upsert_record(record, conn)
        except psycopg2.Error as e:
            logger.error(f"Error upserting record: {record}")
            logger.error(e)
            conn.rollback()
            raise e

    conn.commit()

