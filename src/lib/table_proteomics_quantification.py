#!/usr/bin/env python

"""
This module contains functions to interact with the 'proteomics_quantification' table in the database.

The 'proteomics_quantification' table contains data specific to the quantification
values derived from proteomics experiments.
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
    TABLE_NAME_PROTEOMICS_QUANTIFICATION,
    TABLE_STRUCTURE_PROTEOMICS_QUANTIFICATION,
    COLUMN_NAME_EXPERIMENTAL_ID,
    COLUMN_NAME_EXPERIMENTAL_CONDITION_NAME,
    COLUMN_NAME_REPLICATE,
    COLUMN_NAME_PROTEIN_SEQUENCE,
    COLUMN_NAME_SUM_PEP,
    COLUMN_NAME_COMBINED_Q_VALUE,
    COLUMN_NAME_ABUNDANCE,
    COLUMN_NAME_ABUNDANCE_NORMALIZED,
    COLUMN_NAME_ABUNDANCE_COUNT
)


TSV_FORMAT_SCHEMA_PROTEOMICS_QUANTIFICATION = {
    COLUMN_NAME_EXPERIMENTAL_ID: str,
    COLUMN_NAME_EXPERIMENTAL_CONDITION_NAME: str,
    COLUMN_NAME_REPLICATE: int,
    COLUMN_NAME_PROTEIN_SEQUENCE: str,
    COLUMN_NAME_SUM_PEP: float,
    COLUMN_NAME_COMBINED_Q_VALUE: float,
    COLUMN_NAME_ABUNDANCE: float,
    COLUMN_NAME_ABUNDANCE_NORMALIZED: float,
    COLUMN_NAME_ABUNDANCE_COUNT: float

}

@dataclass
class ProteomicsQuantificationRecord:

    experimental_id: str
    sequence: str
    sum_pep: float
    combined_q_value: float
    abundance: float
    abundance_normalized: float
    abundance_count: float

    experimental_condition_name: Optional[str] = None
    replicate: Optional[int] = None


logger = logging.getLogger(__name__)


def format_data(tab_data: str) -> List[ProteomicsQuantificationRecord]:
    """
    Given a TSV file, this function parses the data and returns a list of
    ProteomicsQuantificationRecord objects.

    Args:
        tab_data (str): A string containing the TSV data.

    Returns:
        List[ProteomicsQuantificationRecord]: A list of ProteomicsQuantificationRecord objects.
    """

    generic_rows = parse_tsv(tab_data, TSV_FORMAT_SCHEMA_PROTEOMICS_QUANTIFICATION)

    return [r.to_specific_structure(ProteomicsQuantificationRecord) for r in generic_rows]


def validate_records(records: List[ProteomicsQuantificationRecord]) -> None:
    """
    Given a list of ProteomicsQuantificationRecord objects, this function validates the data
    to ensure there are no duplicates or other validation rules.

    Args:
        records (List[ProteomicsQuantificationRecord]): The list of records to validate.

    Raises:
        ValueError: If there are validation errors.
    """
    # Example validation: No duplicate column1 values
    unique_values = set()

    for record in records:
        if record.experimental_id in unique_values:
            raise ValueError(f"Duplicate experimental_id value found: {record.experimental_id}")

        unique_values.add(record.experimental_id)


def upsert_record(record: ProteomicsQuantificationRecord, conn: psycopg2.extensions.connection) -> None:
    """
    Given a ProteomicsQuantificationRecord object, this function upserts the record into the
    corresponding table in the database.

    Args:
        record (ProteomicsQuantificationRecord): The record to upsert.
        conn: The psycopg2 connection object.

    Returns:
        None

    Raises:
        psycopg2.Error: If there is an error upserting the record.
    """

    query = f"""
INSERT INTO {TABLE_NAME_PROTEOMICS_QUANTIFICATION} (
    {COLUMN_NAME_EXPERIMENTAL_ID},
    {COLUMN_NAME_EXPERIMENTAL_CONDITION_NAME},
    {COLUMN_NAME_REPLICATE},
    {COLUMN_NAME_PROTEIN_SEQUENCE},
    {COLUMN_NAME_SUM_PEP},
    {COLUMN_NAME_COMBINED_Q_VALUE},
    {COLUMN_NAME_ABUNDANCE},
    {COLUMN_NAME_ABUNDANCE_NORMALIZED},
    {COLUMN_NAME_ABUNDANCE_COUNT}
) VALUES (
    %s,%s,%s,%s,%s,%s,%s,%s,%s
)

ON CONFLICT (
    {COLUMN_NAME_EXPERIMENTAL_ID},
    {COLUMN_NAME_EXPERIMENTAL_CONDITION_NAME},
    {COLUMN_NAME_REPLICATE},
)
DO UPDATE SET
    {COLUMN_NAME_PROTEIN_SEQUENCE} = EXCLUDED.{COLUMN_NAME_PROTEIN_SEQUENCE},
    {COLUMN_NAME_SUM_PEP} = EXCLUDED.{COLUMN_NAME_SUM_PEP},
    {COLUMN_NAME_COMBINED_Q_VALUE} = EXCLUDED.{COLUMN_NAME_COMBINED_Q_VALUE},
    {COLUMN_NAME_ABUNDANCE} = EXCLUDED.{COLUMN_NAME_ABUNDANCE},
    {COLUMN_NAME_ABUNDANCE_NORMALIZED} = EXCLUDED.{COLUMN_NAME_ABUNDANCE_NORMALIZED},
    {COLUMN_NAME_ABUNDANCE_COUNT} = EXCLUDED.{COLUMN_NAME_ABUNDANCE_COUNT}
"""

    params = (
        record.experimental_id,
        record.experimental_condition_name,
        record.replicate,
        record.sequence,
        record.sum_pep,
        record.combined_q_value,
        record.abundance,
        record.abundance_normalized,
        record.abundance_count
    )

    try:
        execute_query(query, conn, params)
    except psycopg2.Error as e:
        logger.error(f"Error upserting record: {record}")
        raise e


def run_upsert_proteomics_quantification(
        in_data: str,
        experimental_condition_name: str,
        replicate: int,
        conn: psycopg2.extensions.connection
) -> None:
    """
    Given a string containing TSV data and a psycopg2 connection object, this
    function parses the data, validates it, and upserts the records into the
    'proteomics_quantification' table in the database.

    Args:
        in_data (str): A string containing the TSV data.
        experimental_condition_name (str): The name of the experimental condition.
        replicate (int): The replicate number.
        conn: The psycopg2 connection object.

    Returns:
        None

    Raises:
        ValueError: If there are any validation errors.
        psycopg2.Error: If there are any issues upserting the records.
    """

    logger.info(f"Upserting data into {TABLE_NAME_PROTEOMICS_QUANTIFICATION} table...")

    logger.info("Parsing input data...")
    records = format_data(in_data)
    logger.info(f"Succesfully parsed {len(records)} records")

    logger.info("Validating records...")
    validate_records(records)
    logger.info("Successfully validated records")

    create_table_if_not_exists(
            TABLE_NAME_PROTEOMICS_QUANTIFICATION,
            TABLE_STRUCTURE_PROTEOMICS_QUANTIFICATION,
            conn
    )

    for record in records:

        record.experimental_condition_name = experimental_condition_name
        record.replicate = replicate


        upsert_record(record, conn)

    conn.commit()
