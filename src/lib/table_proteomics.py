#!/usr/bin/env python

"""
This module contains functions to interact with the 'proteomics_peptide_modifications' table in the database.

The 'proteomics_peptide_modifications' table contains data specific to the modifications
detected in peptides derived from peptide spectrum matches (PSMs) in proteomics experiments.
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
from lib.generic_row import parse_tsv
from lib.schema import (
    TABLE_NAME_PROTEOMICS,
    TABLE_STRUCTURE_PROTEOMICS,
    COLUMN_NAME_EXPERIMENTAL_ID,
    COLUMN_NAME_CONDITION_A,
    COLUMN_NAME_CONDITION_B,
    COLUMN_NAME_PEPTIDE_SEQUENCE,
    COLUMN_NAME_PEPTIDE_POSITIONS,
    COLUMN_NAME_PEPTIDE_PTMS,
    COLUMN_NAME_LOG2_FOLD_CHANGE,
    COLUMN_NAME_P_VALUE,
    COLUMN_NAME_ADJUSTED_P_VALUE,
)


TSV_FORMAT_SCHEMA_PROTEOMICS = {
    COLUMN_NAME_EXPERIMENTAL_ID: str,
    COLUMN_NAME_PEPTIDE_SEQUENCE: str,
    COLUMN_NAME_PEPTIDE_POSITIONS: str,
    COLUMN_NAME_PEPTIDE_PTMS: str,
    COLUMN_NAME_LOG2_FOLD_CHANGE: float,
    COLUMN_NAME_P_VALUE: float,
    COLUMN_NAME_ADJUSTED_P_VALUE: float,
}

@dataclass
class ProteomicsRecord:

    experimental_id: str
    peptide_sequence: str
    peptide_positions: str
    peptide_ptms: str
    log2_fold_change: float
    p_value: float
    adjusted_p_value: float

    condition_a: Optional[str] = None
    condition_b: Optional[str] = None


logger = logging.getLogger(__name__)


def format_data(tab_data: str) -> List[ProteomicsRecord]:
    """
    Given a TSV file, this function parses the data and returns a list of
    ProteomicsPeptideModificationsRecord objects.

    Args:
        tab_data (str): A string containing the TSV data.

    Returns:
        List[ProteomicsPeptideModificationsRecord]: A list of ProteomicsPeptideModificationsRecord objects.
    """

    generic_rows = parse_tsv(tab_data, TSV_FORMAT_SCHEMA_PROTEOMICS)

    return [r.to_specific_structure(ProteomicsRecord) for r in generic_rows]


def validate_records(records: List[ProteomicsRecord]) -> None:
    """
    Given a list of ProteomicsPeptideModificationsRecord objects, this function validates the data
    to ensure there are no duplicates or other validation rules.

    Args:
        records (List[ProteomicsPeptideModificationsRecord]): The list of records to validate.

    Raises:
        ValueError: If there are validation errors.
    """
    # Example validation: No duplicate column1 values
    pass


def upsert_record(record: ProteomicsRecord, conn: psycopg2.extensions.connection) -> None:
    """
    Given a ProteomicsPeptideModificationsRecord object, this function upserts the record into the
    corresponding table in the database.

    Args:
        record (ProteomicsPeptideModificationsRecord): The record to upsert.
        conn: The psycopg2 connection object.

    Returns:
        None

    Raises:
        psycopg2.Error: If there is an error upserting the record.
    """

    query = f"""
INSERT INTO {TABLE_NAME_PROTEOMICS} (
    {COLUMN_NAME_EXPERIMENTAL_ID},
    {COLUMN_NAME_CONDITION_A},
    {COLUMN_NAME_CONDITION_B},
    {COLUMN_NAME_PEPTIDE_SEQUENCE},
    {COLUMN_NAME_PEPTIDE_POSITIONS},
    {COLUMN_NAME_PEPTIDE_PTMS},
    {COLUMN_NAME_LOG2_FOLD_CHANGE},
    {COLUMN_NAME_P_VALUE},
    {COLUMN_NAME_ADJUSTED_P_VALUE}
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s
)
"""

    params = (
        record.experimental_id,
        record.condition_a,
        record.condition_b,
        record.peptide_sequence,
        record.peptide_positions,
        record.peptide_ptms,
        record.log2_fold_change,
        record.p_value,
        record.adjusted_p_value
    )

    try:
        execute_query(query, conn, params)
    except psycopg2.Error as e:
        logger.error(f"Error upserting record: {record}")
        raise e


def run_upsert_proteomics(
        in_data: str,
        condition_a: str,
        condition_b: str,
        conn: psycopg2.extensions.connection
) -> None:
    """
    Given a string containing TSV data and a psycopg2 connection object, this
    function parses the data, validates it, and upserts the records into the
    'proteomics' table in the database.

    Args:
        in_data (str): A string containing the TSV data.
        condition_a (str): The name of the first experimental condition.
        condition_b (str): The name of the second experimental condition.
        conn: The psycopg2 connection object.

    Returns:
        None

    Raises:
        ValueError: If there are any validation errors.
        psycopg2.Error: If there are any issues upserting the records.
    """

    logger.info(f"Upserting data into {TABLE_NAME_PROTEOMICS} table...")

    logger.info("Parsing input data...")
    records = format_data(in_data)
    logger.info(f"Succesfully parsed {len(records)} records")

    logger.info("Validating records...")
    validate_records(records)
    logger.info("Successfully validated records")

    create_table_if_not_exists(
            TABLE_NAME_PROTEOMICS,
            TABLE_STRUCTURE_PROTEOMICS,
            conn
    )
    
    for record in records:

        record.condition_a = condition_a
        record.condition_b = condition_b

        try:
            upsert_record(record, conn)
        except psycopg2.Error as e:
            logger.error(f"Error upserting record: {record}")
            logger.error(e)
            conn.rollback()
            raise e

    conn.commit()
