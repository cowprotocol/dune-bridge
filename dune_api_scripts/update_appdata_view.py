"""Modifies and executed dune query for today's data"""
from os import getenv

from duneapi.api import DuneAPI
from duneapi.types import DuneQuery, Network
from duneapi.util import open_query

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

    app_data_query = DuneQuery(
        name="App Data Mapping",
        description="",
        raw_sql=QUERY,
        network=Network.MAINNET,
        parameters=[],
        query_id=int(getenv("QUERY_ID_ALL_APP_DATA", "257782")),
    )
    # App hash with referral data as json
    dune.initiate_query(app_data_query)
    dune.execute_query(app_data_query)
    # Check out the raw results here: https://dune.xyz/queries/257782

    # Parsed referral data
    parsed_app_data_query = DuneQuery(
        name="Parsed App Data Mapping",
        description="",
        raw_sql=open_query("./dune_api_scripts/queries/parsed_app_data.sql"),
        network=Network.MAINNET,
        parameters=[],
        query_id=int(getenv("QUERY_ID_PARSED_APP_DATA", "279621")),
    )
    # TODO - update execute_query to only need query ID and parameters.
    dune.execute_query(parsed_app_data_query)
    # Check out the parsed results here: https://dune.xyz/queries/279621
