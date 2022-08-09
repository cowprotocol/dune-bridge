"""Modifies and executed dune query for today's data"""
import argparse
from os import getenv

from duneapi.api import DuneAPI
from duneapi.types import DuneQuery, Network
from duneapi.util import open_query

from dune_api_scripts.update.utils import Environment, refresh
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


if __name__ == "__main__":
    dune_connection = DuneAPI.new_from_environment()
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-e",
        "--environment",
        type=Environment,
        choices=list(Environment),
        default=Environment.TEST,
    )
    args = parser.parse_args()
    try:
        update_raw_app_data(dune_connection, args.environment)
        update_parsed_app_data(dune_connection, args.environment)
    except (RuntimeError, AssertionError) as err:
        print("Failed update run due to", err)
