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
        print(
            f"app data successfully updated at https://dune.xyz/queries/{query_id}"
        )
    except SystemExit as err:
        # This is an issue with error handling on duneapi side:
        # https://github.com/bh2smith/duneapi/issues/48
        print("Failed likely due to dune login credentials", err)
    except Exception as err:  # pylint:disable=broad-except
        # TODO - this is only temporary till we can fix the above exception handling...
        #  Essentially, we allow failure so not to disturb the processes.
        print("Unhandled exception", err)
    # Check out the raw results here: https://dune.xyz/queries/863359
