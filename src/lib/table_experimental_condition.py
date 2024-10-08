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
    execute_fetchall_query,
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
    "name": str,
    "description": str,
    "experimental_condition_type": str,
}

@dataclass
class ExperimentalConditionRecord:
    name: str
    description: str
    experimental_condition_type: str


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
    {COLUMN_NAME_EXPERIMENTAL_CONDITION_TYPE}
) VALUES (
    %s, %s, %s
)

ON CONFLICT ({COLUMN_NAME_EXPERIMENTAL_CONDITION_NAME})
DO UPDATE SET
    {COLUMN_NAME_EXPERIMENTAL_CONDITION_NAME} = EXCLUDED.{COLUMN_NAME_EXPERIMENTAL_CONDITION_NAME},
    {COLUMN_NAME_EXPERIMENTAL_CONDITION_DESCRIPTION} = EXCLUDED.{COLUMN_NAME_EXPERIMENTAL_CONDITION_DESCRIPTION}
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

    logger.info("Upserting records...")
    for record in records:

        try:
            upsert_record(record, conn)
        except psycopg2.Error as e:
            logger.error(f"Error upserting record: {record}")
            logger.error(e)
            conn.rollback()
            raise e


    conn.commit()


def condition_is_valid(
        conn: psycopg2.extensions.connection,
        experiment_type: str,
        condition: str,
) -> bool:
    """
    Given a psycopg2 connection object, an experiment type, and a condition,
    this function checks if the condition is valid for the given experiment type.

    Args:
        conn: The psycopg2 connection object.
        experiment_type (str): The experiment type.
        condition (str): The condition to check.

    Returns:
        bool: True if the condition is valid, False otherwise.
    """

    query = f"""
SELECT EXISTS (
    SELECT 1
    FROM {TABLE_NAME_EXPERIMENTAL_CONDITION}
    WHERE {COLUMN_NAME_EXPERIMENTAL_CONDITION_NAME} = %s
    AND {COLUMN_NAME_EXPERIMENTAL_CONDITION_TYPE} = %s
)
"""

    params = (
        condition,
        experiment_type,
    )

    try:
        result = execute_fetchall_query(query, conn, params)
    except psycopg2.Error as e:
        logger.error(f"Error checking if condition is valid: {condition}")
        raise e

    return result[0][0]

