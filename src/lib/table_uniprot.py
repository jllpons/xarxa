#!/usr/bin/env python3

"""
This module contains functions to interact with the 'uniprot' table in the
database.

The `uniprot` table contains information related from all of the UniProtKB entries
for a given organism.
"""

from dataclasses import dataclass
import json
import logging
from typing import List, Optional

import psycopg2

from lib.schema import (
    TABLE_NAME_UNIPROT,
    TABLE_STRUCTURE_UNIPROT,
    COLUMN_NAME_UNIPROT_ACCESSION,
    COLUMN_NAME_LOCUS_TAG,
    COLUMN_NAME_ORF_NAME,
    COLUMN_NAME_KEGG_ACCESSION,
    COLUMN_NAME_REFSEQ_PROTEIN_ID,
    COLUMN_NAME_EMBL_PROTEIN_ID,
    COLUMN_NAME_KEYWORD,
    COLUMN_NAME_PROTEIN_NAME,
    COLUMN_NAME_PROTEIN_EXISTENCE,
    COLUMN_NAME_SEQUENCE,
    COLUMN_NAME_GO_TERM,
    COLUMN_NAME_EC_NUMBER,
    TABLE_NAME_UNIPROT_KEYWORD,
    TABLE_STRUCTURE_UNIPROT_KEYWORD,
    TABLE_INDEX_UNIPROT_KEYWORD,
    TABLE_NAME_UNIPROT_GO_TERM,
    TABLE_STRUCTURE_UNIPROT_GO_TERM,
    TABLE_INDEX_UNIPROT_GO_TERM,
    TABLE_NAME_UNIPROT_EC_NUMBER,
    TABLE_STRUCTURE_UNIPROT_EC_NUMBER,
    TABLE_INDEX_UNIPROT_EC_NUMBER,
    TABLE_NAME_UNIPROT_PTM,
    COLUMN_NAME_UNIPROT_PTM_START,
    COLUMN_NAME_UNIPROT_PTM_END,
    COLUMN_NAME_UNIPROT_PTM_DESCRIPTION,
    TABLE_STRUCTURE_UNIPROT_PTM,
    TABLE_INDEX_UNIPROT_PTM,
)
from lib.db_operations import execute_query, create_table_if_not_exists
from lib.generic_row import parse_tsv


TSV_FORMAT_SCHEMA_UNIPROT_PROTEIN = {
    "uniprot_accession": str,
    "locus_tag": list,
    "orf_name": list,
    "kegg_accession": list,
    "refseq_protein_id": str,
    "embl_protein_id": str,
    "keywords": list,
    "protein_name": str,
    "protein_existence": str,
    "sequence": str,
    "go_term": list,
    "ec_number": list,
    "post_translational_modification": dict,
}


@dataclass
class UniprotRecord:
    uniprot_accession: str

    locus_tag: Optional[List[str]] = None
    orf_name: Optional[List[str]] = None
    kegg_accession: Optional[List[str]] = None
    refseq_protein_id: Optional[List[str]] = None
    embl_protein_id: Optional[str] = None
    keywords: Optional[List[str]] = None
    protein_name: Optional[str] = None
    protein_existence: Optional[str] = None
    sequence: Optional[str] = None
    go_term: Optional[List[str]] = None
    ec_number: Optional[List[str]] = None
    post_translational_modification: Optional[dict] = None


logger = logging.getLogger(__name__)


def format_data(tab_data: str) -> List[UniprotRecord]:
    """
    Given a TSV file, this function parses the data and returns a list of
    UniprotRecord objects.

    Args:
        tab_data: A string containing the TSV data

    Returns:
        List[UniprotRecord]
    """

    generic_rows = parse_tsv(tab_data, TSV_FORMAT_SCHEMA_UNIPROT_PROTEIN)

    rows = [r.to_specific_structure(UniprotRecord) for r in generic_rows]

    return rows


def validate_records(records: List[UniprotRecord]) -> None:
    """
    Given a list of UniprotRecord objects, this function validates the records
    to ensure that there are no duplicate gene IDs.

    If a duplicate is found, a ValueError is raised.
    """
    # NOTE: More validation can be added here as needed.

    uniport_accessions = set()

    for record in records:

        if record.uniprot_accession in uniport_accessions:
            logger.error(f"Duplicate UniProt accession found: {record.uniprot_accession}")
            raise ValueError

        uniport_accessions.add(record.uniprot_accession)


def upsert_uniprot_table(record: UniprotRecord, conn: psycopg2.extensions.connection) -> None:
    """
    Given a UniprotRecord object, this function upserts the record into the
    corresponding table in the database.

    Args:
        record: A UniprotRecord object
        conn: A psycopg2 connection object

    Returns:
        None

    Raises:
        psycopg2.Error: If an error occurs during the upsert operation
    """

    query = f"""
INSERT INTO {TABLE_NAME_UNIPROT} (
    {COLUMN_NAME_UNIPROT_ACCESSION},
    {COLUMN_NAME_LOCUS_TAG},
    {COLUMN_NAME_ORF_NAME},
    {COLUMN_NAME_KEGG_ACCESSION},
    {COLUMN_NAME_REFSEQ_PROTEIN_ID},
    {COLUMN_NAME_EMBL_PROTEIN_ID},
    {COLUMN_NAME_PROTEIN_NAME},
    {COLUMN_NAME_PROTEIN_EXISTENCE},
    {COLUMN_NAME_SEQUENCE}
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s
)
ON CONFLICT ({COLUMN_NAME_UNIPROT_ACCESSION})
DO UPDATE SET
    {COLUMN_NAME_LOCUS_TAG} = EXCLUDED.{COLUMN_NAME_LOCUS_TAG},
    {COLUMN_NAME_ORF_NAME} = EXCLUDED.{COLUMN_NAME_ORF_NAME},
    {COLUMN_NAME_KEGG_ACCESSION} = EXCLUDED.{COLUMN_NAME_KEGG_ACCESSION},
    {COLUMN_NAME_REFSEQ_PROTEIN_ID} = EXCLUDED.{COLUMN_NAME_REFSEQ_PROTEIN_ID},
    {COLUMN_NAME_EMBL_PROTEIN_ID} = EXCLUDED.{COLUMN_NAME_EMBL_PROTEIN_ID},
    {COLUMN_NAME_PROTEIN_NAME} = EXCLUDED.{COLUMN_NAME_PROTEIN_NAME},
    {COLUMN_NAME_PROTEIN_EXISTENCE} = EXCLUDED.{COLUMN_NAME_PROTEIN_EXISTENCE},
    {COLUMN_NAME_SEQUENCE} = EXCLUDED.{COLUMN_NAME_SEQUENCE}
"""

    params = (
        record.uniprot_accession,
        record.locus_tag,
        record.orf_name,
        record.kegg_accession,
        record.refseq_protein_id,
        record.embl_protein_id,
        record.protein_name,
        record.protein_existence,
        record.sequence,
    )

    try:
        execute_query(query, conn, params)
    except psycopg2.Error as e:
        logger.error(f"Error upserting record: {record.uniprot_accession}")
        raise e


def upsert_uniprot_keyword_table(record: UniprotRecord, conn: psycopg2.extensions.connection) -> None:
    """
    Given a UniprotRecord object, this function upserts the record into the
    corresponding table in the database.

    Args:
        record: A UniprotRecord object
        conn: A psycopg2 connection object

    Returns:
        None

    Raises:
        psycopg2.Error: If an error occurs during the upsert operation
    """

    query = f"""
INSERT INTO {TABLE_NAME_UNIPROT_KEYWORD} (
    {COLUMN_NAME_UNIPROT_ACCESSION},
    {COLUMN_NAME_KEYWORD}
) VALUES (
    %s, %s
)
ON CONFLICT ({COLUMN_NAME_UNIPROT_ACCESSION}, {COLUMN_NAME_KEYWORD})
DO NOTHING
"""

    if record.keywords is None:
        return

    for keyword in record.keywords:
        params = (
            record.uniprot_accession,
            keyword,
        )

        try:
            execute_query(query, conn, params)
        except psycopg2.Error as e:
            logger.error(f"Error upserting record: {record.uniprot_accession}")
            raise e


def upsert_uniprot_go_term_table(record: UniprotRecord, conn: psycopg2.extensions.connection) -> None:
    """
    Given a UniprotRecord object, this function upserts the record into the
    corresponding table in the database.

    Args:
        record: A UniprotRecord object
        conn: A psycopg2 connection object

    Returns:
        None

    Raises:
        psycopg2.Error: If an error occurs during the upsert operation
    """

    query = f"""
INSERT INTO {TABLE_NAME_UNIPROT_GO_TERM} (
    {COLUMN_NAME_UNIPROT_ACCESSION},
    {COLUMN_NAME_GO_TERM}
) VALUES (
    %s, %s
)
ON CONFLICT ({COLUMN_NAME_UNIPROT_ACCESSION}, {COLUMN_NAME_GO_TERM})
DO NOTHING
"""

    if record.go_term is None:
        return

    for go_term in record.go_term:
        params = (
            record.uniprot_accession,
            go_term,
        )

        try:
            execute_query(query, conn, params)
        except psycopg2.Error as e:
            logger.error(f"Error upserting record: {record.uniprot_accession}")
            raise e


def upsert_uniprot_ec_number_table(record: UniprotRecord, conn: psycopg2.extensions.connection) -> None:
    """
    Given a UniprotRecord object, this function upserts the record into the
    corresponding table in the database.

    Args:
        record: A UniprotRecord object
        conn: A psycopg2 connection object

    Returns:
        None

    Raises:
        psycopg2.Error: If an error occurs during the upsert operation
    """

    query = f"""
INSERT INTO {TABLE_NAME_UNIPROT_EC_NUMBER} (
    {COLUMN_NAME_UNIPROT_ACCESSION},
    {COLUMN_NAME_EC_NUMBER}
) VALUES (
    %s, %s
)
ON CONFLICT ({COLUMN_NAME_UNIPROT_ACCESSION}, {COLUMN_NAME_EC_NUMBER})
DO NOTHING
"""

    if record.ec_number is None:
        return

    for ec_number in record.ec_number:
        params = (
            record.uniprot_accession,
            ec_number,
        )

        try:
            execute_query(query, conn, params)
        except psycopg2.Error as e:
            logger.error(f"Error upserting record: {record.uniprot_accession}")
            raise e


def upsert_uniprot_ptm_table(record: UniprotRecord, conn: psycopg2.extensions.connection) -> None:
    """
    Given a UniprotRecord object, this function upserts the record into the
    corresponding table in the database.

    Args:
        record: A UniprotRecord object
        conn: A psycopg2 connection object

    Returns:
        None

    Raises:
        psycopg2.Error: If an error occurs during the upsert operation
    """

    query = f"""
INSERT INTO {TABLE_NAME_UNIPROT_PTM} (
    {COLUMN_NAME_UNIPROT_ACCESSION},
    {COLUMN_NAME_UNIPROT_PTM_START},
    {COLUMN_NAME_UNIPROT_PTM_END},
    {COLUMN_NAME_UNIPROT_PTM_DESCRIPTION}
) VALUES (
    %s, %s, %s, %s
)
ON CONFLICT ({COLUMN_NAME_UNIPROT_ACCESSION}, {COLUMN_NAME_UNIPROT_PTM_START}, {COLUMN_NAME_UNIPROT_PTM_END})
DO UPDATE SET
    {COLUMN_NAME_UNIPROT_PTM_DESCRIPTION} = EXCLUDED.{COLUMN_NAME_UNIPROT_PTM_DESCRIPTION}
"""

    if record.post_translational_modification is None:
        return

    json_obj = json.loads(record.post_translational_modification)
    for ptm_dict in json_obj:
        positions = ptm_dict["position"].split("..")
        start = positions[0]
        end = positions[1]
        description = ptm_dict["description"]
        params = (
            record.uniprot_accession,
            start,
            end,
            description,
        )

        try:
            execute_query(query, conn, params)
        except psycopg2.Error as e:
            logger.error(f"Error upserting record: {record.uniprot_accession}")
            raise e


def upsert_record(record: UniprotRecord, conn: psycopg2.extensions.connection) -> None:
    """
    Given a UniprotRecord object, this function upserts the record into the
    corresponding table in the database.

    Args:
        record: A UniprotRecord object
        conn: A psycopg2 connection object

    Returns:
        None

    Raises:
        psycopg2.Error: If an error occurs during the upsert operation
    """

    upsert_uniprot_table(record, conn)

    upsert_uniprot_keyword_table(record, conn)

    upsert_uniprot_go_term_table(record, conn)

    upsert_uniprot_ec_number_table(record, conn)

    upsert_uniprot_ptm_table(record, conn)


def run_upsert_uniprot(
        in_data: str,
        conn: psycopg2.extensions.connection,
        ) -> None:
    """
    Given a TSV file containing UniProt Protein data, this function parses
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

    logger.info(f"Upserting UniProt Protein data into the '{TABLE_NAME_UNIPROT}' table...")

    logger.debug("Parsing input data...")
    records = format_data(in_data)
    logger.info(f"Succesfully parsed {len(records)} records")

    logger.info("Validating records...")
    validate_records(records)
    logger.info("Successfully validated records")

    logger.info("Creating tables and indexes if they do not exist...")
    create_table_if_not_exists(
        TABLE_NAME_UNIPROT,
        TABLE_STRUCTURE_UNIPROT,
        conn,
    )

    create_table_if_not_exists(
        TABLE_NAME_UNIPROT_KEYWORD,
        TABLE_STRUCTURE_UNIPROT_KEYWORD,
        conn,
    )
    execute_query(TABLE_INDEX_UNIPROT_KEYWORD, conn)

    create_table_if_not_exists(
        TABLE_NAME_UNIPROT_GO_TERM,
        TABLE_STRUCTURE_UNIPROT_GO_TERM,
        conn,
    )
    execute_query(TABLE_INDEX_UNIPROT_GO_TERM, conn)

    create_table_if_not_exists(
        TABLE_NAME_UNIPROT_EC_NUMBER,
        TABLE_STRUCTURE_UNIPROT_EC_NUMBER,
        conn,
    )
    execute_query(TABLE_INDEX_UNIPROT_EC_NUMBER, conn)

    create_table_if_not_exists(
        TABLE_NAME_UNIPROT_PTM,
        TABLE_STRUCTURE_UNIPROT_PTM,
        conn,
    )
    execute_query(TABLE_INDEX_UNIPROT_PTM, conn)



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


