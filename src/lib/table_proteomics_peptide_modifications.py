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
    execute_query,
    create_table_if_not_exists
)
from lib.generic_row import parse_tsv
from lib.schema import (
    TABLE_NAME_PROTEOMICS_PEPTIDE_MODIFICATIONS,
    TABLE_STRUCTURE_PROTEOMICS_PEPTIDE_MODIFICATIONS,
    COLUMN_NAME_EXPERIMENTAL_ID,
    COLUMN_NAME_EXPERIMENTAL_CONDITION_NAME,
    COLUMN_NAME_MODIFICATION_TYPE,
    COLUMN_NAME_MODIFICATION_POSITION,
    COLUMN_NAME_PEPTIDE_SEQUENCE,
    COLUMN_NAME_START_POSITION_COMPLETE_PROTEIN,
    COLUMN_NAME_END_POSITION_COMPLETE_PROTEIN,
    COLUMN_NAME_PSM_AMBIGUITY,
    COLUMN_NAME_PEP,
    COLUMN_NAME_Q_VALUE,
    COLUMN_NAME_SEARCH_ENGINE_CONFIDENCE,
)


TSV_FORMAT_SCHEMA_PROTEOMICS_PEPTIDE_MODIFICATIONS = {
    COLUMN_NAME_EXPERIMENTAL_ID: str,
    COLUMN_NAME_EXPERIMENTAL_CONDITION_NAME: str,
    COLUMN_NAME_MODIFICATION_TYPE: str,
    COLUMN_NAME_MODIFICATION_POSITION: str,
    COLUMN_NAME_PEPTIDE_SEQUENCE: str,
    COLUMN_NAME_START_POSITION_COMPLETE_PROTEIN: int,
    COLUMN_NAME_END_POSITION_COMPLETE_PROTEIN: int,
    COLUMN_NAME_PSM_AMBIGUITY: str,
    COLUMN_NAME_PEP: float,
    COLUMN_NAME_Q_VALUE: float,
    COLUMN_NAME_SEARCH_ENGINE_CONFIDENCE: str,
}

@dataclass
class ProteomicsPeptideModificationsRecord:

    experimental_id: str
    modification_type: str
    modification_position: str
    peptide_sequence: str
    start_position_complete_protein: int
    end_position_complete_protein: int
    psm_ambiguity: str
    pep: float
    q_value: float
    search_engine_confidence: str

    experimental_condition_name: Optional[str] = None


logger = logging.getLogger(__name__)


def format_data(tab_data: str) -> List[ProteomicsPeptideModificationsRecord]:
    """
    Given a TSV file, this function parses the data and returns a list of
    ProteomicsPeptideModificationsRecord objects.

    Args:
        tab_data (str): A string containing the TSV data.

    Returns:
        List[ProteomicsPeptideModificationsRecord]: A list of ProteomicsPeptideModificationsRecord objects.
    """

    generic_rows = parse_tsv(tab_data, TSV_FORMAT_SCHEMA_PROTEOMICS_PEPTIDE_MODIFICATIONS)

    return [r.to_specific_structure(ProteomicsPeptideModificationsRecord) for r in generic_rows]


def validate_records(records: List[ProteomicsPeptideModificationsRecord]) -> None:
    """
    Given a list of ProteomicsPeptideModificationsRecord objects, this function validates the data
    to ensure there are no duplicates or other validation rules.

    Args:
        records (List[ProteomicsPeptideModificationsRecord]): The list of records to validate.

    Raises:
        ValueError: If there are validation errors.
    """
    # Example validation: No duplicate column1 values
    unique_values = set()

    for record in records:
        if record.experimental_id in unique_values:
            raise ValueError(f"Duplicate experimental_id value found: {record.experimental_id}")

        unique_values.add(record.experimental_id)


def upsert_record(record: ProteomicsPeptideModificationsRecord, conn: psycopg2.extensions.connection) -> None:
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
INSERT INTO {TABLE_NAME_PROTEOMICS_PEPTIDE_MODIFICATIONS} (
    {COLUMN_NAME_EXPERIMENTAL_ID},
    {COLUMN_NAME_EXPERIMENTAL_CONDITION_NAME},
    {COLUMN_NAME_MODIFICATION_TYPE},
    {COLUMN_NAME_MODIFICATION_POSITION},
    {COLUMN_NAME_PEPTIDE_SEQUENCE},
    {COLUMN_NAME_START_POSITION_COMPLETE_PROTEIN},
    {COLUMN_NAME_END_POSITION_COMPLETE_PROTEIN},
    {COLUMN_NAME_PSM_AMBIGUITY},
    {COLUMN_NAME_PEP},
    {COLUMN_NAME_Q_VALUE},
    {COLUMN_NAME_SEARCH_ENGINE_CONFIDENCE}
) VALUES (
    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
)

ON CONFLICT (
    {COLUMN_NAME_EXPERIMENTAL_ID},
    {COLUMN_NAME_EXPERIMENTAL_CONDITION_NAME},
    {COLUMN_NAME_MODIFICATION_TYPE},
    {COLUMN_NAME_MODIFICATION_POSITION},
)
DO UPDATE SET
    {COLUMN_NAME_PEPTIDE_SEQUENCE} = EXCLUDED.{COLUMN_NAME_PEPTIDE_SEQUENCE},
    {COLUMN_NAME_START_POSITION_COMPLETE_PROTEIN} = EXCLUDED.{COLUMN_NAME_START_POSITION_COMPLETE_PROTEIN},
    {COLUMN_NAME_END_POSITION_COMPLETE_PROTEIN} = EXCLUDED.{COLUMN_NAME_END_POSITION_COMPLETE_PROTEIN},
    {COLUMN_NAME_PSM_AMBIGUITY} = EXCLUDED.{COLUMN_NAME_PSM_AMBIGUITY},
    {COLUMN_NAME_PEP} = EXCLUDED.{COLUMN_NAME_PEP},
    {COLUMN_NAME_Q_VALUE} = EXCLUDED.{COLUMN_NAME_Q_VALUE},
    {COLUMN_NAME_SEARCH_ENGINE_CONFIDENCE} = EXCLUDED.{COLUMN_NAME_SEARCH_ENGINE_CONFIDENCE}
"""

    params = (
        record.experimental_id,
        record.experimental_condition_name,
        record.modification_type,
        record.modification_position,
        record.peptide_sequence,
        record.start_position_complete_protein,
        record.end_position_complete_protein,
        record.psm_ambiguity,
        record.pep,
        record.q_value,
        record.search_engine_confidence,
    )

    try:
        execute_query(query, conn, params)
    except psycopg2.Error as e:
        logger.error(f"Error upserting record: {record}")
        raise e


def run_upsert_proteomics_peptide_modifications(
        in_data: str,
        experimental_condition_name: str,
        conn: psycopg2.extensions.connection
) -> None:
    """
    Given a string containing TSV data and a psycopg2 connection object, this
    function parses the data, validates it, and upserts the records into the
    'proteomics_peptide_modifications' table in the database.

    Args:
        in_data (str): A string containing the TSV data.
        experimental_condition_name (str): The name of the experimental condition.
        conn: The psycopg2 connection object.

    Returns:
        None

    Raises:
        ValueError: If there are any validation errors.
        psycopg2.Error: If there are any issues upserting the records.
    """

    logger.info(f"Upserting data into {TABLE_NAME_PROTEOMICS_PEPTIDE_MODIFICATIONS} table...")

    logger.info("Parsing input data...")
    records = format_data(in_data)
    logger.info(f"Succesfully parsed {len(records)} records")

    logger.info("Validating records...")
    validate_records(records)
    logger.info("Successfully validated records")

    create_table_if_not_exists(
            TABLE_NAME_PROTEOMICS_PEPTIDE_MODIFICATIONS,
            TABLE_STRUCTURE_PROTEOMICS_PEPTIDE_MODIFICATIONS,
            conn
    )

    for record in records:

        record.experimental_condition_name = experimental_condition_name

        upsert_record(record, conn)

    conn.commit()
