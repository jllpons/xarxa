#!/usr/bin/env python3

"""
This module contains functions for performing basic SQL operations,
used by multiple modules.

Also contains constants for the names of the tables and enums.
"""

import logging
from typing import List, Optional

import psycopg2

from lib.generic_row import GenericRow


logger = logging.getLogger(__name__)


def execute_query(
        query: str,
        conn: psycopg2.extensions.connection,
        params: Optional[tuple] = None
        ) -> None:

    """
    Executes a simple query on the database.

    Args:
        query (str): The query to execute.
        conn (psycopg2.extensions.connection): The connection to use.
        params (Optional[List]): The parameters to use in the query.

    Returns:
        None: The query was executed successfully.

    Raises:
        psycopg2.Error: If an error occurs while executing the query.
    """

    try:
        with conn.cursor() as cursor:
            logger.debug(f"Executing query '{query}' with parameters '{params}'")
            cursor.execute(query, params or ())
            logger.debug("Query executed successfully")
            logger.debug(f"Query: '{cursor.query.decode()}'")
            logger.debug(f"Row count: '{cursor.rowcount}'")
    except psycopg2.Error as error:
        conn.rollback()
        logger.error(f"Error executing query: {error}")
        raise error


def execute_fetchall_query(
        query: str,
        conn: psycopg2.extensions.connection,
        params: Optional[tuple] = None
        ) -> list:
    """
    Executes a fetchall query on the database.

    Args:
        query (str): The query to execute.
        conn (psycopg2.extensions.connection): The connection to use.
        params (Optional[List]): The parameters to use in the query.

    Returns:
        list: The results of the query.

    Raises:
        psycopg2.Error: If an error occurs while executing the query.
    """

    try:

        with conn.cursor() as cursor:

            logger.debug(f"Executing query '{query}' with parameters '{params}'")
            cursor.execute(query, params or ())
            logger.debug("Query executed successfully")
            logger.debug(f"Query: '{cursor.query.decode()}'")
            logger.debug(f"Row count: '{cursor.rowcount}'")

            return cursor.fetchall()

    except psycopg2.Error as error:
        conn.rollback()
        logger.error(f"Error executing query: {error}")
        raise error


def create_table_if_not_exists(table_name: str,
                               table_content: str,
                               conn: psycopg2.extensions.connection) -> None:
    """
    This function will create a table in the database if it does not exist.

    Parameters:
        table_name (str): The name of the table to be created.
        table_content (str): The content of the table to be created.
        conn (psycopg2.extensions.connection): The connection to the database.

    Returns:
        None: The table will be created in the database.

    Raises:
        psycopg2.Error: If an error occurs while creating the table.
    """

    logger.debug(f"Creating `{table_name}` table in the database")

    query = f"CREATE TABLE IF NOT EXISTS {table_name} ({table_content})"

    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
    except psycopg2.Error as e:
        conn.rollback()
        logger.error(f"Error creating table: {e}")
        raise e

    conn.commit()

    logger.debug(f"Propertly created `{table_name}` table in the database")


def connect_to_db(db: str,
                  quiet: bool = False) -> psycopg2.extensions.connection:
    """
    Connect to the database.

    Parameters:
        db (str): The connection string to the database.
        quiet (bool): If True, the function will not print any messages to the console.

    Returns:
        conn (psycopg2.extensions.connection): The connection to the database.

    Raises:
        psycopg2.Error: If the connection to the database fails.
    """

    logger.debug(f"Atempting to connect to the database: {db}")
    try:
        conn = psycopg2.connect(db)
    except psycopg2.Error as e:
        logger.error(f"Error connecting to the database: {e}")
        raise e

    if not quiet:
        logger.info("Succesfully connected to the database.")

    return conn


def get_all_tables(conn: psycopg2.extensions.connection) -> List[str]:
    """
    This function will return a list of all tables in the database.

    Parameters:
        conn (psycopg2.extensions.connection): The connection to the database.

    Returns:
        List[str]: A list of all tables in the database.
    """

    logger.debug("Getting all tables in the database")

    query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"

    try:
        tables = execute_fetchall_query(query, conn)
    except psycopg2.Error as e:
        logger.error(f"Error getting all tables: {e}")
        raise e

    logger.debug(f"Tables in the database: {tables}")

    return list(map(lambda x: x[0], tables))


def get_table_columns(table_name: str,
                      conn: psycopg2.extensions.connection) -> List[str]:
    """
    This function will return a list of all columns in a table.

    Parameters:
        table_name (str): The name of the table to get the columns from.
        conn (psycopg2.extensions.connection): The connection to the database.

    Returns:
        List[str]: A list of all columns in the table.
    """

    logger.debug(f"Getting all columns in the `{table_name}` table")

    query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'"

    try:
        columns = execute_fetchall_query(query, conn)
    except psycopg2.Error as e:
        logger.error(f"Error getting all columns: {e}")
        raise e

    logger.debug(f"Columns in the `{table_name}` table: {columns}")

    return list(map(lambda x: x[0], columns))


def get_records(table_name: str,
                column_names: List[str],
                conn: psycopg2.extensions.connection,
                ) -> List[GenericRow]:
    """
    This function will return all records in a table with the specified columns.

    Parameters:
        table_name (str): The name of the table to get the records from.
        column_names (List[str]): The columns to get from the table.
        conn (psycopg2.extensions.connection): The connection to the database.

    Returns:
        List[GenericRow]: A list of all records in the table.
    """

    logger.debug(f"Getting all records in the `{table_name}` table")

    query = f"SELECT {', '.join(column_names)} FROM {table_name}"

    try:
        records = execute_fetchall_query(query, conn)
    except psycopg2.Error as e:
        logger.error(f"Error getting all records: {e}")
        raise e

    logger.debug(f"Records in the `{table_name}` table: {records}")

    generic_rows = []
    for record in records:
        map = {}
        for i, column in enumerate(column_names):
            map[column] = record[i]

        generic_rows.append(GenericRow(**map))

    return generic_rows


def table_already_exists(table_name: str, conn: psycopg2.extensions.connection) -> bool:
    """
    This function will check if a table already exists in the database.

    Parameters:
        table_name (str): The name of the table to be checked.
        conn (psycopg2.extensions.connection): The connection to the database.

    Returns:
        bool: True if the table already exists, False otherwise.
    """

    logger.debug(f"Checking if `{table_name}` table already exists in the database")

    with conn.cursor() as cursor:
        cursor.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}')")
        return cursor.fetchone()[0]

