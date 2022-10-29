"""
A few project level Enums
"""
import argparse
import os
from enum import Enum

from duneapi.api import DuneAPI
from duneapi.types import QueryParameter, DuneQuery, Network
from duneapi.util import open_query

from dune_api_scripts.local_env import QUERY_ROOT


class Environment(Enum):
    """Enum for Deployment Environments"""

    STAGING = "barn"
    PRODUCTION = "prod"
    TEST = "test"

    def __str__(self) -> str:
        return str(self.value)

    def as_query_param(self) -> QueryParameter:
        """Converts Environment to Dune Query Parameter"""
        return QueryParameter.enum_type(
            "Environment", str(self), [str(e) for e in Environment]
        )


def refresh(dune: DuneAPI, query: DuneQuery) -> None:
    """Updates and executes `query`"""
    dune.initiate_query(query)
    job_id = dune.execute_query(query)
    # TODO - This blocking call waits for execution to finish (could be removed)
    dune.get_results(job_id)
    print(
        f"{query.name} successfully updated: https://dune.xyz/queries/{query.query_id}"
    )


def push_view(
    dune: DuneAPI,
    query_file: str,
    values: list[str],
    env: Environment,
    query_id: int
) -> None:
    # TODO - use this in update_appdata_view.py and update/user_retention.py
    file_path = os.path.join(QUERY_ROOT, query_file)
    """Updates user generated view with retention values"""
    raw_sql = open_query(file_path).replace("{{Values}}", ",\n      ".join(values))
    query = DuneQuery(
        raw_sql=raw_sql,
        parameters=[env.as_query_param()],
        network=Network.MAINNET,
        query_id=query_id,
    )
    refresh(dune, query)


def update_args() -> argparse.Namespace:
    """Arguments used to pass table environment name"""
    # TODO - it would be a lot easier to pass Environment and an ENV var.
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-e",
        "--environment",
        type=Environment,
        choices=list(Environment),
        default=Environment.TEST,
    )
    return parser.parse_args()
