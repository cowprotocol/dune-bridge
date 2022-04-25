"""Modifies and executed dune query for today's data"""
from os import getenv

from duneapi.api import DuneAPI
from duneapi.types import DuneQuery, Network
from .utils import app_data_entries

if __name__ == "__main__":
    # initialize the environment
    dune = DuneAPI.new_from_environment()
    VALUES = app_data_entries()

    # build query
    QUERY = f"""
    CREATE OR REPLACE VIEW
    dune_user_generated.gp_appdata (app_data, referrer)
    AS VALUES {VALUES};"""

    # update query in dune
    query_id = int(getenv("QUERY_ID_ALL_APP_DATA", "257782"))

    dune_query = DuneQuery("", "", QUERY, Network.MAINNET, [], query_id)
    # fetch data
    dune.execute_query(dune_query)
    # Check out the results here: https://dune.xyz/queries/257782
