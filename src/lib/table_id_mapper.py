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
from lib.db_operations import (
        execute_query,
        create_table_if_not_exists,
        execute_fetchall_query,
)
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
ON CONFLICT (
    {COLUMN_NAME_UNIPROT_ACCESSION},
    {COLUMN_NAME_REFSEQ_LOCUS_TAG},
    {COLUMN_NAME_LOCUS_TAG},
    {COLUMN_NAME_KEGG_ACCESSION},
    {COLUMN_NAME_REFSEQ_PROTEIN_ID}
) DO NOTHING
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


def map_id(
        conn: psycopg2.extensions.connection,
        id: str,
        ) -> List[IdMapperRecord]:
    """
    """

    query = f"""
SELECT
    {COLUMN_NAME_UNIPROT_ACCESSION},
    {COLUMN_NAME_REFSEQ_LOCUS_TAG},
    {COLUMN_NAME_LOCUS_TAG},
    {COLUMN_NAME_KEGG_ACCESSION},
    {COLUMN_NAME_REFSEQ_PROTEIN_ID}
FROM {TABLE_NAME_ID_MAPPER}
WHERE {COLUMN_NAME_UNIPROT_ACCESSION} = %s OR
      {COLUMN_NAME_REFSEQ_LOCUS_TAG} = %s OR
      {COLUMN_NAME_LOCUS_TAG} = %s OR
      {COLUMN_NAME_KEGG_ACCESSION} = %s OR
      {COLUMN_NAME_REFSEQ_PROTEIN_ID} = %s
"""

    params = (id, id, id, id, id)

    try:
        results = execute_fetchall_query(query, conn, params)
    except psycopg2.Error as e:
        logger.error(f"Error fetching record for ID: {id}")
        raise e

    results = [IdMapperRecord(*r) for r in results]

    if len(results) == 0:
        return []

    return results

def _map_id(
        conn: psycopg2.extensions.connection,
        id: str,
        ) -> List[str]:
    """
    """

    is_uniprot_query = f"""
SELECT {COLUMN_NAME_UNIPROT_ACCESSION}
FROM {TABLE_NAME_ID_MAPPER}
WHERE {COLUMN_NAME_UNIPROT_ACCESSION} = %s
"""

    is_refseq_locus_tag_query = f"""
SELECT {COLUMN_NAME_REFSEQ_LOCUS_TAG}
FROM {TABLE_NAME_ID_MAPPER}
WHERE {COLUMN_NAME_REFSEQ_LOCUS_TAG} = %s
"""

    is_locus_tag_query = f"""
SELECT {COLUMN_NAME_LOCUS_TAG}
FROM {TABLE_NAME_ID_MAPPER}
WHERE {COLUMN_NAME_LOCUS_TAG} = %s
"""

    is_kegg_accession_query = f"""
SELECT {COLUMN_NAME_KEGG_ACCESSION}
FROM {TABLE_NAME_ID_MAPPER}
WHERE {COLUMN_NAME_KEGG_ACCESSION} = %s
"""

    is_refseq_protein_id_query = f"""
SELECT {COLUMN_NAME_REFSEQ_PROTEIN_ID}
FROM {TABLE_NAME_ID_MAPPER}
WHERE {COLUMN_NAME_REFSEQ_PROTEIN_ID} = %s
"""

    params = (id,)

    try:
        is_uniprot = execute_fetchall_query(is_uniprot_query, conn, params)
        is_refseq_locus_tag = execute_fetchall_query(is_refseq_locus_tag_query, conn, params)
        is_locus_tag = execute_fetchall_query(is_locus_tag_query, conn, params)
        is_kegg_accession = execute_fetchall_query(is_kegg_accession_query, conn, params)
        is_refseq_protein_id = execute_fetchall_query(is_refseq_protein_id_query, conn, params)
    except psycopg2.Error as e:
        logger.error(f"Error fetching record for ID: {id}")
        raise e

    results = []

    query = None

    if is_uniprot:
        query = f"""
SELECT *
FROM {TABLE_NAME_ID_MAPPER}
WHERE {COLUMN_NAME_UNIPROT_ACCESSION} = %s
"""

    elif is_refseq_locus_tag:
        query = f"""
SELECT *
FROM {TABLE_NAME_ID_MAPPER}
WHERE {COLUMN_NAME_REFSEQ_LOCUS_TAG} = %s
"""

    elif is_locus_tag:
        query = f"""
SELECT *
FROM {TABLE_NAME_ID_MAPPER}
WHERE {COLUMN_NAME_LOCUS_TAG} = %s
"""

    elif is_kegg_accession:
        query = f"""
SELECT *
FROM {TABLE_NAME_ID_MAPPER}
WHERE {COLUMN_NAME_KEGG_ACCESSION} = %s
"""

    elif is_refseq_protein_id:
        query = f"""
SELECT *
FROM {TABLE_NAME_ID_MAPPER}
WHERE {COLUMN_NAME_REFSEQ_PROTEIN_ID} = %s
"""

    else:
        results = []

    if not query:
        return results

    try:
        results = execute_fetchall_query(query, conn, params)
    except psycopg2.Error as e:
        logger.error(f"Error fetching record for ID: {id}")
        raise e

    if len(results) == 0:
        return []

    uniprot_accession = set([r[0] for r in results if r[0] is not None])
    if uniprot_accession != {None}:
        uniprot_accession = ";".join(uniprot_accession)
    else:
        uniprot_accession = "NULL"

    refseq_locus_tag = set([r[1] for r in results if r[1] is not None])
    if refseq_locus_tag != {None}:
        refseq_locus_tag = ";".join(refseq_locus_tag)
    else:
        refseq_locus_tag = "NULL"

    locus_tag = set([r[2] for r in results if r[2] is not None])
    if locus_tag != {None}:
        locus_tag = ";".join(locus_tag)
    else:
        locus_tag = "NULL"

    kegg_accession = set([r[3] for r in results if r[3] is not None])
    if kegg_accession != {None}:
        kegg_accession = ";".join(kegg_accession)
    else:
        kegg_accession = "NULL"

    refseq_protein_id = set([r[4] for r in results if r[4] is not None])
    if refseq_protein_id != {None}:
        refseq_protein_id = ";".join(refseq_protein_id)
    else:
        refseq_protein_id = "NULL"

    results = [
        uniprot_accession,
        refseq_locus_tag,
        locus_tag,
        kegg_accession,
        refseq_protein_id,
    ]

    return results




