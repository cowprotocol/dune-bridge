"""Modifies and executed dune query for today's data"""
import argparse
from enum import Enum
from os import getenv

from duneapi.api import DuneAPI
from duneapi.types import DuneQuery, Network, QueryParameter
from duneapi.util import open_query

from .utils import app_data_entries


def refresh(dune: DuneAPI, query: DuneQuery):
    dune.initiate_query(query)
    job_id = dune.execute_query(query)
    dune.get_results(job_id)
    print(
        f"{query.name} successfully updated: https://dune.xyz/queries/{query.query_id}"
    )


def update_raw_app_data(dune: DuneAPI):
    values = app_data_entries()
    query_id = int(getenv("QUERY_ID_RAW_APP_DATA"))
    query = DuneQuery(
        name="Raw App Data Mapping",
        description="",
        raw_sql=open_query("./dune_api_scripts/queries/raw_app_data.sql").replace(
            "{{VALUES}}", values
        ),
        network=Network.MAINNET,
        parameters=[],
        query_id=query_id,
    )
    refresh(dune, query)


class Environment(Enum):
    staging = "barn"
    production = "prod"

    def __str__(self) -> str:
        return self.value


def update_parsed_app_data(dune: DuneAPI, env: Environment):
    query_id = int(getenv("QUERY_ID_PARSED_APP_DATA"))
    query = DuneQuery(
        name="Parsed App Data",
        description="",
        raw_sql=open_query("./dune_api_scripts/queries/parsed_app_data.sql"),
        network=Network.MAINNET,
        parameters=[
            QueryParameter.enum_type("Environment", env.value, ["barn", "prod"])
        ],
        query_id=query_id,
    )
    refresh(dune, query)


if __name__ == "__main__":
    dune_connection = DuneAPI.new_from_environment()
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--environment", type=Environment, choices=list(Environment), required=True
    )
    args = parser.parse_args()
    try:
        update_raw_app_data(dune_connection)
        update_parsed_app_data(dune_connection, args.environment)
    except RuntimeError as err:
        print("Failed to update due to", err)
