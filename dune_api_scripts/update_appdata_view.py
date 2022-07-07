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

    # build query from VALUES
    QUERY = open_query("./dune_api_scripts/queries/parsed_app_data.sql").replace(
        "{{VALUES}}", VALUES
    )
    query_id = int(getenv("QUERY_ID_ALL_APP_DATA", "863359"))
    app_data_query = DuneQuery(
        name="App Data Mapping",
        description="",
        raw_sql=QUERY,
        network=Network.MAINNET,
        parameters=[],
        query_id=query_id,
    )
    # App hash with referral data as json
    try:
        dune.initiate_query(app_data_query)
        dune.execute_query(app_data_query)
        print(f"Successfully updated app data view at https://dune.xyz/queries/{query_id}")
    except Exception as e:
        print(e)
    # Check out the raw results here: https://dune.xyz/queries/863359
