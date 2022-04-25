"""
Queries and stores today's trading history in a file called
`user_data_from{today's date}.json`.
Note that this file name is dictated by method `utils.store_as_json_file`.
"""
import datetime
import os
import time
from duneapi.api import DuneAPI
from duneapi.types import DuneQuery, Network
from .utils import store_as_json_file
from .queries import build_query_for_affiliate_data

JOB_FREQUENCY_IN_MINUTES = 5


def build_query_for_todays_trading_volume():
    """
    Constructs appropriate query for fetching today's trading data.
    """
    today = datetime.date.today() - datetime.timedelta(minutes=JOB_FREQUENCY_IN_MINUTES)
    tomorrow = today + datetime.timedelta(days=1)
    start_date = f'\'{today.strftime("%Y-%m-%d")}\''
    end_date = f'\'{tomorrow.strftime("%Y-%m-%d")}\''

    return build_query_for_affiliate_data(start_date, end_date)


if __name__ == "__main__":
    # initialize the environment
    dune = DuneAPI.new_from_environment()

    query_id = int(os.getenv("QUERY_ID_TODAYS_TRADING_DATA", "249240"))
    QUERY = build_query_for_todays_trading_volume()
    time_of_request = int(time.time())
    dune_query = DuneQuery("", "", QUERY, Network.MAINNET, [], query_id)
    # fetch data
    data = dune.fetch(dune_query)
    data_set = {"user_data": data, "time_of_download": time_of_request}
    store_as_json_file(data_set)
