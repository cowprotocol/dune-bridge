"""
A few project level Enums
"""
import argparse
from enum import Enum

from duneapi.api import DuneAPI
from duneapi.types import QueryParameter, DuneQuery


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
    dune.get_results(query, job_id)
    print(
        f"{query.name} successfully updated: https://dune.xyz/queries/{query.query_id}"
    )


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
