"""Modifies and executed dune query for today's data"""
from os import getenv
import sys
import logging
import argparse

from duneapi.api import DuneAPI
from duneapi.types import DuneQuery, Network
from duneapi.util import open_query

from dune_api_scripts.update.utils import Environment, refresh, update_args
from dune_api_scripts.utils import app_data_entries


def update_raw_app_data(dune: DuneAPI, env: Environment) -> None:
    """Updates the RAW App Data View"""
    values = app_data_entries()
    query = DuneQuery(
        name="Raw App Data Mapping",
        description="",
        raw_sql=open_query("./dune_api_scripts/queries/raw_app_data.sql").replace(
            "{{VALUES}}", values
        ),
        network=Network.MAINNET,
        parameters=[env.as_query_param()],
        query_id=int(getenv("QUERY_ID_RAW_APP_DATA", "1032460")),
    )
    refresh(dune, query)


def update_parsed_app_data(dune: DuneAPI, env: Environment) -> None:
    """Updates the Parsed App Data View"""
    query = DuneQuery(
        name="Parsed App Data",
        description="",
        raw_sql=open_query("./dune_api_scripts/queries/parsed_app_data.sql"),
        network=Network.MAINNET,
        parameters=[env.as_query_param()],
        query_id=int(getenv("QUERY_ID_PARSED_APP_DATA", "1032466")),
    )
    refresh(dune, query)


def main(environment: Environment) -> None:
    """Update raw and parsed app data"""
    dune_connection = DuneAPI.new_from_environment()
    try:
        update_raw_app_data(dune_connection, environment)
        update_parsed_app_data(dune_connection, environment)
    except (RuntimeError, AssertionError):
        logging.exception("Failed update run due to an error!")
        raise


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--environment", type=Environment, choices=list(Environment), required=True
    )
    args = parser.parse_args()
    sys.exit(main(environment=args.environment))
