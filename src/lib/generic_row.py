#!/usr/bin/env python3

"""
This script provides functionality to parse tab-separated values (TSV) content
into dynamic data structures. It supports transforming TSV content based on a
predefined schema that dictates the expected column names and their corresponding
data types. This enables flexible and type-safe parsing of TSV files into Python objects.

When loading TSV content, into 'GenericRow' objects, ww can control in a centralized way
how the different data types are parsed and represented within the different stcripts.
"""


import csv
from io import StringIO
import json
import logging
from typing import List


logger = logging.getLogger(__name__)


class GenericRow:
    """
    A generic container for holding row data from a TSV file. It supports dynamic
    attribute assignment based on column names and allows for the transformation
    of this generic row data into more specific data structures.

    It also provides a string representation of the row's data, where values are
    separated by tabs, lists are joined with semicolons, and None values are
    represented as "NULL".
    """

    def __init__(self, **kwargs):
        """
        Initializes a GenericRow instance with dynamic attributes based on
        provided keyword arguments.

        Args:
            **kwargs: Arbitrary keyword arguments representing column names
                      and their values.
        """

        for key, value in kwargs.items():
            setattr(self, key, value)


    def to_specific_structure(self, structure_type):
        """
        Transforms the generic row data into a specific structure type,
        facilitating the conversion of loosely typed data into a more structured
        and defined form.

        Args:
            structure_type (type): The class reference to be used for creating 
                                   the specific data structure instance.

        Returns:
            An instance of the specified `structure_type` class, initialized
            with the row's data.
        """

        fields = {}

        for key, value in self.__dict__.items():
            if key in structure_type.__dataclass_fields__:
                fields[key] = value

        return structure_type(**fields)


    def __str__(self):
        """
        Generates a string representation of the row, where values are separated by tabs. Lists are joined with
        semicolons, and None values are represented as "NULL".

        Returns:
            str: A tab-separated string representation of the row's data.
        """

        formatted_values = []

        for value in self.__dict__.values():

            if isinstance(value, list):
                # Join the list with semicolon
                if value is None:
                    formatted_values.append("NULL")
                elif len(value) == 0:
                    formatted_values.append("NULL")
                else:
                    formatted_values.append(";".join(str(v) if v is not None else "NULL" for v in value))

            elif value is None:
                # Replace None with NULL
                formatted_values.append("NULL")

            else:
                # Leave the value as is and convert it to string
                formatted_values.append(str(value))

        return "\t".join(formatted_values)


def parse_tsv(tsv_content: str,
              schema: dict,
              list_sep: str = ";"
              ) -> List[GenericRow]:
    """
    Parses TSV content into a list of `GenericRow` objects based on a provided schema. The schema defines
    the expected columns, their names, and data types, allowing for type-safe parsing of the TSV content.

    Args:
        tsv_content (str): The TSV content as a string.
        schema (dict): A dictionary defining the expected column names and their corresponding data types.
        list_sep (str): The separator used for splitting string representations of lists.

    Returns:
        List[GenericRow]: A list of `GenericRow` objects, each representing a row from the TSV content.
    """

    # Convert the TSV content into a file-like object so it can be read by the CSV reader
    # Not a big fan but CSV reader should provide more edge case handling than I would do in a custom parser
    tsv_file = StringIO(tsv_content)

    rows = []

    # Read the TSV content with the CSV reader
    reader = csv.DictReader(tsv_file, delimiter="\t", fieldnames=list(schema.keys()))

    for row in reader:

        if len(row) != len(schema):
            logger.warning(
                f"When parsing the TSV content, the row '{row}' does not match the schema '{schema}'. "
                + f"Lenght of row: {len(row)}, lenght of schema: {len(schema)}"
            )
            continue

        parsed_row = {}

        for column, value in row.items():

            target_type = schema.get(column, str)

            if not value:
                parsed_row[column] = None
                continue
            elif value is None:
                parsed_row[column] = None
                continue
            elif value == '':
                parsed_row[column] = None
                continue
            elif value == "NULL":
                parsed_row[column] = None
                continue

            if target_type == str:
                try:
                    parsed_row[column] = str(value) if value != "NULL" else None
                except ValueError:
                    logger.warning(f"Failed to parse string from column '{column}' with value '{value}'")
                    parsed_row[column] = None

            elif target_type == int:
                try:
                    parsed_row[column] = int(value) if value != "NULL" else None
                except ValueError:
                    logger.warning(f"Failed to parse integer from column '{column}' with value '{value}'")
                    parsed_row[column] = None

            elif target_type == float:
                logger.debug(f"Trying to parse float from column '{column}' with value '{value}'")
                try:
                    parsed_row[column] = float(value) if value != "NULL" else None
                except ValueError:
                    try:
                        parsed_row[column] = float(value.replace(",", ".")) if value != "NULL" else None
                    except ValueError:
                        logger.warning(f"Failed to parse float from column '{column}' with value '{value}'")
                        parsed_row[column] = None

            elif target_type == bool:
                try:
                    parsed_row[column] = bool(value) if value != "NULL" else None
                except ValueError:
                    logger.warning(f"Failed to parse boolean from column '{column}' with value '{value}'")
                    parsed_row[column] = None

            elif target_type == list:
                content = []
                try:
                    for v in value.split(list_sep):
                        if not v or v == "NULL":
                            continue
                        content.append(v)
                except ValueError:
                    logger.warning(f"Failed to parse list from column '{column}' with value '{value}'")
                    content = None
                except AttributeError:
                    content = None

                if content:
                    parsed_row[column] = content

            elif target_type == dict:
                if value == "NULL":
                    parsed_row[column] = None
                    continue
                try:
                    value = value.replace("\'", "\"")
                    parsed_row[column] = json.dumps(json.loads(value))
                except json.JSONDecodeError:
                    logger.warning(f"Failed to parse dict from column '{column}' with value '{value}'")
                    parsed_row[column] = None

            else:
                logger.warning(f"Unknown type '{target_type}' for column '{column}'")

        rows.append(GenericRow(**parsed_row))

    return rows

