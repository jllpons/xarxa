#!/usr/bin/env python3

"""
Part of the netwdb project.

This module contains the functions that modify how the user interacts with the
scripts via the command line. This includes functions that parse command line
arguments and to setup a custom logger.
"""

import argparse
import datetime
import logging
import json
import os
import sys


logger = logging.getLogger(__name__)


class CustomHelpFormatter(argparse.RawTextHelpFormatter):

    def add_usage(self, usage, actions, groups, prefix=None):
        if prefix is None:
            prefix = 'Usage: '
        return super(CustomHelpFormatter, self).add_usage(
                            usage, actions, groups, prefix)

    def _format_action_invocation(self, action):
        if not action.option_strings or action.nargs == 0:
            return super()._format_action_invocation(action)
        default = self._get_default_metavar_for_optional(action)
        args_string = self._format_args(action, default)
        return ', '.join(action.option_strings) + ' ' + args_string


def setup_logger(level: str) -> logging.Logger:
    """
    Setup a custom logger.

    Args:
        None

    Returns:
        logging.Logger object
    """

    # Example: `[script.py::do_something] INFO: This is a log msg.
    format = "[%(filename)s::%(funcName)s] %(levelname)s: %(message)s"
    level = logging.getLevelName(level)

    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setLevel(level)
    stderr_handler.setFormatter(logging.Formatter(format))

    filename = os.path.basename(sys.argv[0])
    date = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

    file_handler = logging.FileHandler(f"logs/{date}_{filename}.log")
    file_handler.setLevel(level)
    file_handler.setFormatter(logging.Formatter(format))

    # Configure a custom logger
    # `filename` is the name of the file where the logging message was generated
    # `funcName` is the name of the function where the logging message was generated
    # `levelname` is the level of the logging message (e.g. INFO, WARNING, ERROR, etc.)
    # `message` is the actual logging message
    # All messages are printed to stderr (`stream=sys.stderr`)
    logging.basicConfig(level=logging.getLevelName(level),
                        format="[%(filename)s::%(funcName)s] %(levelname)s: %(message)s",
                        handlers=[
                            stderr_handler,
                            file_handler
                            ]
                        )

    logger = logging.getLogger(__name__)

    # These two libraries are very verbose and populate the logs with
    # unnecessary information. We can safely suppress their logs.
    logging.getLogger("requests").propagate = False
    logging.getLogger("urllib3").propagate = False

    return logger


def read_input(file_path: str) -> str:
    """
    Reads the input file or stdin if the file_path is "-".

    Parameters
        file_path (str): The path to the input file.

    Returns
        data (str): The data read from the input file.

    Raises:
        FileNotFoundError: If the input file is not found.
    """

    if file_path == "-":
        data = sys.stdin.read()
    else:
        if not os.path.isfile(file_path):
            logger.error(f"File not found: {file_path}")
            raise FileNotFoundError

        with open(file_path, "r") as f:
            data = f.read()

    return data


def get_database_connection_string(default_path: str = "config/configuration.json") -> str:
    """
    Reads the database connection string from the configuration file.

    Parameters
        default_path (str): The default path to the configuration file.

    Returns
        str: The database connection string.

    Raises
        FileNotFoundError: If the configuration file is not found.
        KeyError: If the database connection string is not found in the configuration file.
    """

    try:
        with open(default_path, "r") as f:
            config = json.load(f)
    except FileNotFoundError as e:
        logger.error("Configuration file not found.")
        raise e

    try:
        conn_str = config["database_data"]["connection"]
        return conn_str
    except KeyError as e:
        logger.error("Database connection string not found in configuration file.")
        raise e


def load_config(default_path: str = "config/configuration.json") -> dict:
    """
    Reads the configuration file.

    Parameters
        default_path (str): The default path to the configuration file.

    Returns
        dict: The configuration file as a dictionary.

    Raises
        FileNotFoundError: If the configuration file is not found.
    """

    try:
        with open(default_path, "r") as f:
            config = json.load(f)
    except FileNotFoundError as e:
        logger.error("Configuration file not found.")
        raise e

    return config


