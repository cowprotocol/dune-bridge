"""Basic client for connecting to postgres database with login credentials"""
import os
from enum import Enum
from pathlib import Path

import psycopg2
import pandas as pd
from dotenv import load_dotenv
from pandas import DataFrame
from sqlalchemy import create_engine, engine

from dune_api_scripts.local_env import QUERY_ROOT


class OrderbookEnv(Enum):
    """
    Enum for distinguishing between CoW Protocol's staging and production environment
    """

    BARN = "BARN"
    PROD = "PROD"

    def __str__(self) -> str:
        return str(self.value)


class PgEngine:

    @staticmethod
    def _make(db_env: OrderbookEnv) -> engine:
        """Returns a connection to postgres database"""
        load_dotenv()
        host = os.environ[f"{db_env}_ORDERBOOK_HOST"]
        port = os.environ[f"{db_env}_ORDERBOOK_PORT"]
        database = os.environ[f"{db_env}_ORDERBOOK_DB"]
        user = os.environ[f"{db_env}_ORDERBOOK_USER"]
        password = os.environ[f"{db_env}_ORDERBOOK_PASSWORD"]
        db_string = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
        return create_engine(db_string)

    @staticmethod
    def fetch_and_merge(filename: str, path: Path = QUERY_ROOT) -> DataFrame:
        with open(os.path.join(path, filename), "r", encoding="utf-8") as query_file:
            query = query_file.read()
        # Need to fetch results from both order-books (prod and barn)
        prod_df: DataFrame = pd.read_sql(sql=query, con=PgEngine._make(OrderbookEnv.PROD))
        barn_df: DataFrame = pd.read_sql(sql=query, con=PgEngine._make(OrderbookEnv.BARN))
        # TODO - Need to validate no overlap without being specific to dataset returned!
        # Solvers do not appear in both environments!
        # assert set(prod_df.solver).isdisjoint(set(barn_df.solver)), "receiver overlap!"
        return pd.concat([prod_df, barn_df])
