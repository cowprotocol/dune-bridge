"""Modifies and executed dune query for today's data"""
import argparse
from enum import Enum
from os import getenv

from duneapi.api import DuneAPI
from duneapi.types import DuneQuery, Network, QueryParameter
from duneapi.util import open_query
import click

from .utils import app_data_entries


def refresh(dune: DuneAPI, query: DuneQuery):
    """Updates and executes `query`"""
    dune.initiate_query(query)
    job_id = dune.execute_query(query)
    dune.get_results(job_id)
    print(
        f"{query.name} successfully updated: https://dune.xyz/queries/{query.query_id}"
    )


class Environment(Enum):
    """Enum for Deployment Environments"""

    STAGING = "barn"
    PRODUCTION = "prod"

    def __str__(self) -> str:
        return self.value

    def as_query_param(self) -> QueryParameter:
        """Converts Environment to Dune Query Parameter"""
        return QueryParameter.text_type("Environment", self.value,)


def update_raw_app_data(dune: DuneAPI, env: Environment):
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


def update_parsed_app_data(dune: DuneAPI, env: Environment):
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


@click.command()
@click.option("--environment", "-e", help="The environment to")
def main(environment):
    dune_connection = DuneAPI.new_from_environment()
    try:
        update_raw_app_data(dune_connection, environment)
        update_parsed_app_data(dune_connection, environment)
    except (RuntimeError, AssertionError) as err:
        print("Failed update run due to", err)


if __name__ == "__main__":
    main()
