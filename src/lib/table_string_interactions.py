#!/usr/bin/env python3

"""
This module contains functions to interact with a 'string_interactions' table in
the database.

The `string_interactions` table contains information about protein-protein
interactions from the STRING database.
"""

from dataclasses import dataclass
import logging
from typing import List, Optional, Tuple

import psycopg2

from lib.db_operations import (
    execute_query,
    create_table_if_not_exists,
)

from lib.generic_row import parse_tsv, GenericRow
from lib.schema import (
    TABLE_NAME_STRING_INTERACTIONS,
    TABLE_STRUCTURE_STRING_INTERACTIONS,
    TABLE_INDEX_STRING_INTERACTIONS,
    COLUMN_NAME_PROTEIN_A,
    COLUMN_NAME_PROTEIN_B,
    COLUMN_NAME_NEIGHBORHOOD,
    COLUMN_NAME_NEIGHBORHOOD_TRANSFERRED,
    COLUMN_NAME_FUSION,
    COLUMN_NAME_PHYLOGENETIC_COOCCURRENCE,
    COLUMN_NAME_HOMOLOGY,
    COLUMN_NAME_COEXPRESSION,
    COLUMN_NAME_COEXPRESSION_TRANSFERRED,
    COLUMN_NAME_EXPERIMENTAL,
    COLUMN_NAME_EXPERIMENTAL_TRANSFERRED,
    COLUMN_NAME_DATABASE,
    COLUMN_NAME_DATABASE_TRANSFERRED,
    COLUMN_NAME_TEXTMINING,
    COLUMN_NAME_TEXTMINING_TRANSFERRED,
    COLUMN_NAME_COMBINED_SCORE,
        )



TSV_FORMAT_SCHEMA_STRING_INTERACTIONS = {
    COLUMN_NAME_PROTEIN_A: str,
    COLUMN_NAME_PROTEIN_B: str,
    COLUMN_NAME_NEIGHBORHOOD: int,
    COLUMN_NAME_NEIGHBORHOOD_TRANSFERRED: int,
    COLUMN_NAME_FUSION: int,
    COLUMN_NAME_PHYLOGENETIC_COOCCURRENCE: int,
    COLUMN_NAME_HOMOLOGY: int,
    COLUMN_NAME_COEXPRESSION: int,
    COLUMN_NAME_COEXPRESSION_TRANSFERRED: int,
    COLUMN_NAME_EXPERIMENTAL: int,
    COLUMN_NAME_EXPERIMENTAL_TRANSFERRED: int,
    COLUMN_NAME_DATABASE: int,
    COLUMN_NAME_DATABASE_TRANSFERRED: int,
    COLUMN_NAME_TEXTMINING: int,
    COLUMN_NAME_TEXTMINING_TRANSFERRED: int,
    COLUMN_NAME_COMBINED_SCORE: int,
}


@dataclass
class StringInteractionsRecord:

    protein_a: str
    protein_b: str
    neighborhood: int
    neighborhood_transferred: int
    fusion: int
    phylogenetic_cooccurrence: int
    homology: int
    coexpression: int
    coexpression_transferred: int
    experimental: int
    experimental_transferred: int
    database: int
    database_transferred: int
    textmining: int
    textmining_transferred: int
    combined_score: int


logger = logging.getLogger(__name__)


def format_data(tab_data: str) -> List[StringInteractionsRecord]:
    """
    Given a TSV file, this function parses the data and returns a list of
    StringInteractionsRecord objects.

    Args:
        tab_data: A string containing the TSV data

    Returns:
        List[StringInteractionsRecord]: A list of StringInteractionsRecord objects
    """

    generic_rows = parse_tsv(tab_data, TSV_FORMAT_SCHEMA_STRING_INTERACTIONS)

    rows = [r.to_specific_structure(StringInteractionsRecord) for r in generic_rows]

    return rows


# Not used ATM
def validate_records(records: List[StringInteractionsRecord]) -> None:
    """
    """
    # NOTE: More validation can be added here as needed.

    raise NotImplementedError



def upsert_record(record: StringInteractionsRecord,
                  conn: psycopg2.extensions.connection) -> None:
    """
    Given a StringInteractionsRecord object, this function upserts the record into the
    corresponding table in the database.

    Args:
        record: A StringInteractionsRecord object
        conn: A psycopg2 connection object

    Returns:
        None

    Raises:
        psycopg2.Error: If an error occurs during the upsert operation
    """

    query = f"""
INSERT INTO {TABLE_NAME_STRING_INTERACTIONS} (
    {COLUMN_NAME_PROTEIN_A},
    {COLUMN_NAME_PROTEIN_B},
    {COLUMN_NAME_NEIGHBORHOOD},
    {COLUMN_NAME_NEIGHBORHOOD_TRANSFERRED},
    {COLUMN_NAME_FUSION},
    {COLUMN_NAME_PHYLOGENETIC_COOCCURRENCE},
    {COLUMN_NAME_HOMOLOGY},
    {COLUMN_NAME_COEXPRESSION},
    {COLUMN_NAME_COEXPRESSION_TRANSFERRED},
    {COLUMN_NAME_EXPERIMENTAL},
    {COLUMN_NAME_EXPERIMENTAL_TRANSFERRED},
    {COLUMN_NAME_DATABASE},
    {COLUMN_NAME_DATABASE_TRANSFERRED},
    {COLUMN_NAME_TEXTMINING},
    {COLUMN_NAME_TEXTMINING_TRANSFERRED},
    {COLUMN_NAME_COMBINED_SCORE}
) VALUES (
    %s,
    %s,
    %s,
    %s,
    %s,
    %s,
    %s,
    %s,
    %s,
    %s,
    %s,
    %s,
    %s,
    %s,
    %s,
    %s
)
ON CONFLICT ({COLUMN_NAME_PROTEIN_A}, {COLUMN_NAME_PROTEIN_B})
DO UPDATE SET
    {COLUMN_NAME_NEIGHBORHOOD} = EXCLUDED.{COLUMN_NAME_NEIGHBORHOOD},
    {COLUMN_NAME_NEIGHBORHOOD_TRANSFERRED} = EXCLUDED.{COLUMN_NAME_NEIGHBORHOOD_TRANSFERRED},
    {COLUMN_NAME_FUSION} = EXCLUDED.{COLUMN_NAME_FUSION},
    {COLUMN_NAME_PHYLOGENETIC_COOCCURRENCE} = EXCLUDED.{COLUMN_NAME_PHYLOGENETIC_COOCCURRENCE},
    {COLUMN_NAME_HOMOLOGY} = EXCLUDED.{COLUMN_NAME_HOMOLOGY},
    {COLUMN_NAME_COEXPRESSION} = EXCLUDED.{COLUMN_NAME_COEXPRESSION},
    {COLUMN_NAME_COEXPRESSION_TRANSFERRED} = EXCLUDED.{COLUMN_NAME_COEXPRESSION_TRANSFERRED},
    {COLUMN_NAME_EXPERIMENTAL} = EXCLUDED.{COLUMN_NAME_EXPERIMENTAL},
    {COLUMN_NAME_EXPERIMENTAL_TRANSFERRED} = EXCLUDED.{COLUMN_NAME_EXPERIMENTAL_TRANSFERRED},
    {COLUMN_NAME_DATABASE} = EXCLUDED.{COLUMN_NAME_DATABASE},
    {COLUMN_NAME_DATABASE_TRANSFERRED} = EXCLUDED.{COLUMN_NAME_DATABASE_TRANSFERRED},
    {COLUMN_NAME_TEXTMINING} = EXCLUDED.{COLUMN_NAME_TEXTMINING},
    {COLUMN_NAME_TEXTMINING_TRANSFERRED} = EXCLUDED.{COLUMN_NAME_TEXTMINING_TRANSFERRED},
    {COLUMN_NAME_COMBINED_SCORE} = EXCLUDED.{COLUMN_NAME_COMBINED_SCORE}
"""

    params = (
        record.protein_a,
        record.protein_b,
        record.neighborhood,
        record.neighborhood_transferred,
        record.fusion,
        record.phylogenetic_cooccurrence,
        record.homology,
        record.coexpression,
        record.coexpression_transferred,
        record.experimental,
        record.experimental_transferred,
        record.database,
        record.database_transferred,
        record.textmining,
        record.textmining_transferred,
        record.combined_score,
    )

    try:
        execute_query(query, conn, params)
    except psycopg2.Error as e:
        logger.error(f"Error upserting record: query={record.protein_a}, target={record.protein_b}")
        raise e


def run_upsert_string_interactions(
        in_data: str,
        conn: psycopg2.extensions.connection,
        ) -> None:
    """
    Given a TSV file containing String interactions data, this function parses
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

    logger.info(f"Upserting String interactions data into the '{TABLE_NAME_STRING_INTERACTIONS}' table...")

    logger.debug("Parsing input data...")
    records = format_data(in_data)
    logger.info(f"Succesfully parsed {len(records)} records")

    # No validation implemented yet
    #logger.info("Validating records...")
    #validate_records(records)
    #logger.info("Successfully validated records")

    create_table_if_not_exists(
        TABLE_NAME_STRING_INTERACTIONS,
        TABLE_STRUCTURE_STRING_INTERACTIONS,
        conn,
    )
    execute_query(TABLE_INDEX_STRING_INTERACTIONS, conn)

    logger.info("Upserting records...")
    # STRING interactions are **undirected**, meaning that if the combined score
    # does not change between the A-B and B-A relations, it can be discarted.
    relation_set = set()
    for record in records:

        # Skip duplicates
        smaller = min(record.protein_a, record.protein_b)
        larger = max(record.protein_a, record.protein_b)

        if (smaller, larger, record.combined_score) in relation_set:
            continue


        try:
            upsert_record(record, conn)
        except psycopg2.Error as e:
            logger.error(f"Error upserting record: {record}")
            logger.error(e)
            conn.rollback()
            raise e

        relation_set.add((smaller, larger, record.combined_score))

    conn.commit()


def get_string_targets(
        conn: psycopg2.extensions.connection,
        refseq_locus_tag: str,
        threshold: int,
        ) -> List[str]:

    target_refseq_locus_tags = []

    query_a = f"""
SELECT {COLUMN_NAME_PROTEIN_B}
FROM {TABLE_NAME_STRING_INTERACTIONS}
WHERE {COLUMN_NAME_PROTEIN_A} = %s AND {COLUMN_NAME_COMBINED_SCORE} > %s
"""

    query_b = f"""
SELECT {COLUMN_NAME_PROTEIN_A}
FROM {TABLE_NAME_STRING_INTERACTIONS}
WHERE {COLUMN_NAME_PROTEIN_B} = %s AND {COLUMN_NAME_COMBINED_SCORE} > %s
"""

    params = (refseq_locus_tag, threshold,)

    logger.debug(f"Fetching STRING targets: refseq_locus_tag={refseq_locus_tag}")
    logger.debug(f"Threshold: {threshold}")
    logger.debug(f"Query A: {query_a}")
    logger.debug(f"Query B: {query_b}")
    logger.debug(f"Params: {params}")

    try:
        with conn.cursor() as cur:
            cur.execute(query_a, params)
            target_refseq_locus_tags.extend([row[0] for row in cur.fetchall()])

            cur.execute(query_b, params)
            target_refseq_locus_tags.extend([row[0] for row in cur.fetchall()])

    except psycopg2.Error as e:
        logger.error(f"Error fetching STRING targets: refseq_locus_tag={refseq_locus_tag}")
        raise e

    return target_refseq_locus_tags



