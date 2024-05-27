#!/usr/bin/env python

"""
This module contains functions to interact with the 'experimental_condition' table in the database.

The 'experimental_condition' table contains data specific to the '{table_description}'.
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
    TABLE_NAME_EXPERIMENTAL_CONDITION,
    TABLE_STRUCTURE_EXPERIMENTAL_CONDITION,
    COLUMN_NAME_EXPERIMENTAL_CONDITION_NAME,
    COLUMN_NAME_EXPERIMENTAL_CONDITION_DESCRIPTION,
    COLUMN_NAME_EXPERIMENTAL_CONDITION_TYPE,
)


TSV_FORMAT_SCHEMA_EXPERIMENTAL_CONDITION = {
    COLUMN_NAME_EXPERIMENTAL_CONDITION_NAME: str,
    COLUMN_NAME_EXPERIMENTAL_CONDITION_DESCRIPTION: str,
    COLUMN_NAME_EXPERIMENTAL_CONDITION_TYPE: str,
}

@dataclass
class ExperimentalConditionRecord:
    name: str
    experimental_condition_type: str
    description: Optional[str] = None


logger = logging.getLogger(__name__)


def format_data(tab_data: str) -> List[ExperimentalConditionRecord]:
    """
    Given a TSV file, this function parses the data and returns a list of
    ExperimentalConditionRecord objects.

    Args:
        tab_data (str): A string containing the TSV data.

    Returns:
        List[ExperimentalConditionRecord]: A list of ExperimentalConditionRecord objects.
    """

    generic_rows = parse_tsv(tab_data, TSV_FORMAT_SCHEMA_EXPERIMENTAL_CONDITION)

    return [r.to_specific_structure(ExperimentalConditionRecord) for r in generic_rows]


def validate_records(records: List[ExperimentalConditionRecord]) -> None:
    """
    Given a list of ExperimentalConditionRecord objects, this function validates the data
    to ensure there are no duplicates or other validation rules.

    Args:
        records (List[ExperimentalConditionRecord]): The list of records to validate.

    Raises:
        ValueError: If there are validation errors.
    """
    # Example validation: No duplicate column1 values
    unique_values = set()

    for record in records:
        if record.name in unique_values:
            raise ValueError(f"Duplicate column1 value found: {record.name}")

        unique_values.add(record.name)


def upsert_record(record: ExperimentalConditionRecord, conn: psycopg2.extensions.connection) -> None:
    """
    Given a ExperimentalConditionRecord object, this function upserts the record into the
    corresponding table in the database.

    Args:
        record (ExperimentalConditionRecord): The record to upsert.
        conn: The psycopg2 connection object.

    Returns:
        None

    Raises:
        psycopg2.Error: If there is an error upserting the record.
    """

    query = f"""
INSERT INTO {TABLE_NAME_EXPERIMENTAL_CONDITION} (
    {COLUMN_NAME_EXPERIMENTAL_CONDITION_NAME},
    {COLUMN_NAME_EXPERIMENTAL_CONDITION_DESCRIPTION},
    {COLUMN_NAME_EXPERIMENTAL_CONDITION_TYPE},
) VALUES (
    %s, %s, %s
)

ON CONFLICT ({COLUMN_NAME_EXPERIMENTAL_CONDITION_NAME})
DO UPDATE SET
    {COLUMN_NAME_EXPERIMENTAL_CONDITION_NAME} = EXCLUDED.{COLUMN_NAME_EXPERIMENTAL_CONDITION_NAME},
    {COLUMN_NAME_EXPERIMENTAL_CONDITION_DESCRIPTION} = EXCLUDED.{COLUMN_NAME_EXPERIMENTAL_CONDITION_DESCRIPTION},
"""

    params = (
        record.name,
        record.description,
        record.experimental_condition_type,
    )

    try:
        execute_query(query, conn, params)
    except psycopg2.Error as e:
        logger.error(f"Error upserting record: {record.name}")
        raise e


def run_upsert_experimental_condition(
        in_data: str,
        conn: psycopg2.extensions.connection
) -> None:
    """
    Given a string containing TSV data and a psycopg2 connection object, this
    function parses the data, validates it, and upserts the records into the
    'experimental_condition' table in the database.

    Args:
        in_data (str): A string containing the TSV data.
        conn: The psycopg2 connection object.

    Returns:
        None

    Raises:
        ValueError: If there are any validation errors.
        psycopg2.Error: If there are any issues upserting the records.
    """

    logger.info(f"Upserting data into {TABLE_NAME_EXPERIMENTAL_CONDITION} table...")

    logger.info("Parsing input data...")
    records = format_data(in_data)
    logger.info(f"Succesfully parsed {len(records)} records")

    logger.info("Validating records...")
    validate_records(records)
    logger.info("Successfully validated records")

    create_table_if_not_exists(
            TABLE_NAME_EXPERIMENTAL_CONDITION,
            TABLE_STRUCTURE_EXPERIMENTAL_CONDITION,
            conn
    )

    raise NotImplementedError("upsert_record function needs to be implemented")


    conn.commit()
