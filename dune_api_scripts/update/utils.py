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
    query_id: int,
    values: list[str],
    query_params: list[QueryParameter],
    separator: str = ",\n"
) -> None:
    # TODO - use this in update_appdata_view.py and update/user_retention.py
    file_path = os.path.join(QUERY_ROOT, query_file)
    """Updates user generated view with retention values"""
    raw_sql = open_query(file_path).replace("{{Values}}", separator.join(values))
    print(f"Pushing approximately {len(raw_sql.encode('utf-8')) / 10 ** 6:.2f} Mb to Dune.")
    query = DuneQuery(
        raw_sql=raw_sql,
        name=query_file,
        parameters=query_params,
        network=Network.MAINNET,
        query_id=query_id,
    )
    refresh(dune, query)


def paginated_table_name(table_name: str, env: Environment, page: int) -> str:
    return f"{table_name}_{env}_page_{page}"


def multi_push_view(
    dune: DuneAPI,
    query_file: str,
    aggregate_query_file: str,
    base_table_name: str,
    query_id: int,
    values: list[str],
    partition_size: int,
    env: Environment,
) -> None:
    partitioned_values = [
        values[i:i + partition_size]
        for i in range(0, len(values), partition_size)
    ]
    aggregate_tables = []
    print(f"Partition")
    for page, chunk in enumerate(partitioned_values):
        table_name = paginated_table_name(base_table_name, env, page)
        push_view(
            dune,
            query_file,
            query_id,
            values=chunk,
            query_params=[QueryParameter.text_type("TableName", table_name)]
        )
        aggregate_tables.append(f"select * from dune_user_generated.{table_name}")
    # TODO - assert sorted values,
    #  - check if updates are even needed (by making a hash select statement)
    #  - Don't update pages that are unchanged

    # This combines all the pages into a single table.
    push_view(
        dune,
        query_file=aggregate_query_file,
        query_id=query_id,
        values=aggregate_tables,
        query_params=[env.as_query_param()],
        separator="\nunion\n"
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
