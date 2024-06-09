#!/usr/bin/env python3

"""
This module contains functions to interact with a 'kegg_relations' table in the
database.

The `kegg_relations` table contains information about relations in KEGG found
in all of the KEGG pathways for a given organism.
"""

from dataclasses import dataclass
import logging
from typing import List, Optional

import psycopg2

from lib.db_operations import (
    execute_query,
    create_table_if_not_exists,
    execute_fetchall_query
)
from lib.generic_row import parse_tsv
from lib.schema import (
        TABLE_NAME_KEGG_RELATIONS,
        TABLE_STRUCTURE_KEGG_RELATIONS,
        TABLE_INDEX_KEGG_RELATIONS,
        COLUMN_NAME_KEGG_RELATION_SOURCE,
        COLUMN_NAME_KEGG_RELATION_TARGET,
        COLUMN_NAME_KEGG_PATHWAY,
        COLUMN_NAME_KEGG_RELATION_TYPE,
        COLUMN_NAME_KEGG_RELATION_SUBTYPE,
        COLUMN_NAME_KEGG_RELATION_SUBTYPE_NAME,
        TABLE_NAME_ID_MAPPER,
        COLUMN_NAME_KEGG_ACCESSION,
        )


TSV_FORMAT_SCHEMA_KEGG_RELATIONS = {
    "kegg_accession_source": str,
    "kegg_accession_target": str,
    "pathway": str,
    "relation_type": str,
    "relation_subtype": list,
    "relation_subtype_values": list,
}


@dataclass
class KeggRelationsRecord:

    kegg_accession_source: str
    kegg_accession_target: str
    pathway: str
    relation_type: str

    relation_subtype: Optional[List[str]] = None
    relation_subtype_values: Optional[List[str]] = None


logger = logging.getLogger(__name__)


def format_data(tab_data: str) -> List[KeggRelationsRecord]:
    """
    Given a TSV file, this function parses the data and returns a list of
    KeggRelationsRecord objects.

    Args:
        tab_data: A string containing the TSV data

    Returns:
        List[KeggRelationsRecord]: A list of KeggRelationsRecord objects
    """

    generic_rows = parse_tsv(tab_data, TSV_FORMAT_SCHEMA_KEGG_RELATIONS)

    rows = [r.to_specific_structure(KeggRelationsRecord) for r in generic_rows]

    return rows


# Not used ATM
def validate_records(records: List[KeggRelationsRecord]) -> None:
    """
    """
    # NOTE: More validation can be added here as needed.

    raise NotImplementedError


def insert_record(record: KeggRelationsRecord,
                  conn: psycopg2.extensions.connection) -> None:
    """
    Given a KeggRelationsRecord object, this function inserts the record into
    corresponding table in the database.

    In this kind of table, it is difficult to determine whether a record must be
    updated since all fields may change and an update may be equivalent to a
    new relation found. Therefore, we will use the ON CONFLICT DO NOTHING
    clause to avoid duplicates in the table.

    Args:
        record: A KeggRelationsRecord object
        conn: A psycopg2 connection object

    Returns:
        None

    Raises:
        psycopg2.Error: If an error occurs during the upsert operation
    """

    query = f"""
INSERT INTO {TABLE_NAME_KEGG_RELATIONS} (
    {COLUMN_NAME_KEGG_RELATION_SOURCE},
    {COLUMN_NAME_KEGG_RELATION_TARGET},
    {COLUMN_NAME_KEGG_PATHWAY},
    {COLUMN_NAME_KEGG_RELATION_TYPE},
    {COLUMN_NAME_KEGG_RELATION_SUBTYPE_NAME},
    {COLUMN_NAME_KEGG_RELATION_SUBTYPE}
) VALUES (
    %s, %s, %s, %s, %s, %s
) ON CONFLICT DO NOTHING
"""

    if record.relation_subtype is None:
        return

    if record.relation_subtype_values is None:
        return

    for subtype, subtype_value in zip(record.relation_subtype, record.relation_subtype_values):


        for st_value in subtype_value.split(" "):

            params = (
                record.kegg_accession_source,
                record.kegg_accession_target,
                record.pathway,
                record.relation_type,
                subtype,
                st_value,
            )

            try:
                execute_query(query, conn, params)
            except psycopg2.Error as e:
                logger.error(f"Error inserting record: {record}")
                raise e


def run_upsert_kegg_relations(
        in_data: str,
        conn: psycopg2.extensions.connection,
        ) -> None:
    """
    Given a TSV file containing KEGG relations data, this function parses
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

    logger.info(f"Upserting KEGG relations data into the '{TABLE_NAME_KEGG_RELATIONS}' table")

    logger.debug("Parsing input data...")
    records = format_data(in_data)
    logger.info(f"Succesfully parsed {len(records)} records")

    #logger.info("Validating records...")
    #validate_records(records)
    #logger.info("Successfully validated records")

    create_table_if_not_exists(
        TABLE_NAME_KEGG_RELATIONS,
        TABLE_STRUCTURE_KEGG_RELATIONS,
        conn,
    )
    execute_query(TABLE_INDEX_KEGG_RELATIONS, conn)

    logger.info("Upserting records...")
    for record in records:

        try:
            insert_record(record, conn)
        except psycopg2.Error as e:
            logger.error(f"Error upserting record: {record}")
            logger.error(e)
            conn.rollback()
            raise e

    conn.commit()


def is_protein(conn: psycopg2.extensions.connection, kegg_accession: str) -> bool:

    query = f"""
SELECT COUNT(*) FROM {TABLE_NAME_ID_MAPPER}
WHERE {COLUMN_NAME_KEGG_ACCESSION} = %s
    """

    params = (kegg_accession,)

    try:
        results = execute_fetchall_query(query, conn, params)
    except psycopg2.Error as e:
        logger.error(f"Error getting KEGG relations for {kegg_accession}")
        raise e

    return results[0][0] > 0


def get_kegg_targets(
        conn: psycopg2.extensions.connection,
        kegg_accession: str,
        ) -> List[str]:

    target_kegg_accessions = []

    query = f"""
SELECT
    {COLUMN_NAME_KEGG_RELATION_TARGET}
FROM {TABLE_NAME_KEGG_RELATIONS}
WHERE {COLUMN_NAME_KEGG_RELATION_SOURCE} = %s
    """

    params = (kegg_accession,)

    try:
        results = execute_fetchall_query(query, conn, params)
    except psycopg2.Error as e:
        logger.error(f"Error getting KEGG relations for {kegg_accession}")
        raise e

    for result in results:
        if is_protein(conn, result):
            target_kegg_accessions.append(result)

    return target_kegg_accessions
