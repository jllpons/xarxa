#!/usr/bin/env python

"""
This module contains functions to interact with the 'transcriptomics_counts' table in the database.

The 'transcriptomics_counts' table contains data specific to the transcriptomics counts
of every experimental condition.
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
    TABLE_NAME_TRANSCRIPTOMICS_COUNTS,
    TABLE_STRUCTURE_TRANSCRIPTOMICS_COUNTS,
    COLUMN_NAME_EXPERIMENTAL_ID,
    COLUMN_NAME_EXPERIMENTAL_CONDITION_NAME,
    COLUMN_NAME_REPLICATE,
    COLUMN_NAME_READ_COUNT,
    COLUMN_NAME_NORMALIZED_READ_COUNT

)


TSV_FORMAT_SCHEMA_TRANSCRIPTOMICS_COUNTS = {
    "experimental_id": str,
    "read_count": float,
    "normalized_count": float
}

@dataclass
class TranscriptomicsCountsRecord:

    experimental_id: str
    read_count: float
    normalized_count: float

    experimental_condition_name: Optional[str] = None
    replicate: Optional[int] = None

logger = logging.getLogger(__name__)


def format_data(tab_data: str) -> List[TranscriptomicsCountsRecord]:
    """
    Given a TSV file, this function parses the data and returns a list of
    TranscriptomicsCountsRecord objects.

    Args:
        tab_data (str): A string containing the TSV data.

    Returns:
        List[TranscriptomicsCountsRecord]: A list of TranscriptomicsCountsRecord objects.
    """

    generic_rows = parse_tsv(tab_data, TSV_FORMAT_SCHEMA_TRANSCRIPTOMICS_COUNTS)

    return [r.to_specific_structure(TranscriptomicsCountsRecord) for r in generic_rows]


def validate_records(records: List[TranscriptomicsCountsRecord]) -> None:
    """
    Given a list of TranscriptomicsCountsRecord objects, this function validates the data
    to ensure there are no duplicates or other validation rules.

    Args:
        records (List[TranscriptomicsCountsRecord]): The list of records to validate.

    Raises:
        ValueError: If there are validation errors.
    """
    # Example validation: No duplicate column1 values
    unique_values = set()

    for record in records:
        if record.experimental_id in unique_values:
            raise ValueError(f"Duplicate column1 value found: {record.experimental_id}")

        unique_values.add(record.experimental_id)


def upsert_record(
        record: TranscriptomicsCountsRecord,
        conn: psycopg2.extensions.connection) -> None:
    """
    Given a TranscriptomicsCountsRecord object, this function upserts the record into the
    corresponding table in the database.

    Args:
        record (TranscriptomicsCountsRecord): The record to upsert.
        conn: The psycopg2 connection object.

    Returns:
        None

    Raises:
        psycopg2.Error: If there is an error upserting the record.
    """

    query = f"""
INSERT INTO {TABLE_NAME_TRANSCRIPTOMICS_COUNTS} (
    {COLUMN_NAME_EXPERIMENTAL_ID},
    {COLUMN_NAME_EXPERIMENTAL_CONDITION_NAME},
    {COLUMN_NAME_REPLICATE},
    {COLUMN_NAME_READ_COUNT},
    {COLUMN_NAME_NORMALIZED_READ_COUNT}
) VALUES (
    %s, %s, %s, %s, %s
)

ON CONFLICT ({COLUMN_NAME_EXPERIMENTAL_ID}, {COLUMN_NAME_EXPERIMENTAL_CONDITION_NAME}, {COLUMN_NAME_REPLICATE})
DO UPDATE SET
    {COLUMN_NAME_READ_COUNT} = EXCLUDED.{COLUMN_NAME_READ_COUNT},
    {COLUMN_NAME_NORMALIZED_READ_COUNT} = EXCLUDED.{COLUMN_NAME_NORMALIZED_READ_COUNT}
"""

    params = (
        record.experimental_id,
        record.experimental_condition_name,
        record.replicate,
        record.read_count,
        record.normalized_count
    )

    try:
        execute_query(query, conn, params)
    except psycopg2.Error as e:
        logger.error(f"Error upserting record: {record.experimental_id}")
        raise e


def run_upsert_transcriptomics_counts(
        in_data: str,
        experimental_condition_name: str,
        replicate: int,
        conn: psycopg2.extensions.connection
) -> None:
    """
    Given a string containing TSV data and a psycopg2 connection object, this
    function parses the data, validates it, and upserts the records into the
    'transcriptomics_counts' table in the database.

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

    logger.info(f"Upserting data into {TABLE_NAME_TRANSCRIPTOMICS_COUNTS} table...")

    logger.info("Parsing input data...")
    records = format_data(in_data)
    logger.info(f"Succesfully parsed {len(records)} records")

    logger.info("Validating records...")
    validate_records(records)
    logger.info("Successfully validated records")

    create_table_if_not_exists(
            TABLE_NAME_TRANSCRIPTOMICS_COUNTS,
            TABLE_STRUCTURE_TRANSCRIPTOMICS_COUNTS,
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

