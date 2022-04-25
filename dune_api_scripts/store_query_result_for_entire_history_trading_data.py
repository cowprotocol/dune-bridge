"""
Queries and stores the entire trading history in the file
`user_data_entire_history.json`.
Note that this file name is actually hard coded in
`utils.open_downloaded_history_file`.
"""
import json
import os
import time
import datetime
from duneapi.api import DuneAPI
from duneapi.types import DuneQuery, Network
from .queries import build_query_for_affiliate_data
from .utils import open_downloaded_history_file


def build_query_for_all_trading_data():
    """
    Constructs query for all time trading data
    """
    start_date = "'2021-03-01'"  # Launch date (approx)
    today = datetime.date.today()
    # End date will be the midnight between yesterday and today, as hours are cut off
    end_date = f'\'{today.strftime("%Y-%m-%d")}\''
    return build_query_for_affiliate_data(start_date, end_date)


if __name__ == "__main__":
    # Entire history does not need to be downloaded again,
    # if file was already downloaded in the past and exists.
    file_entire_history = open_downloaded_history_file()

    # initialize the environment
    dune = DuneAPI.new_from_environment()

    # build query
    QUERY = build_query_for_all_trading_data()
    query_id = int(os.getenv("QUERY_ID_ENTIRE_HISTORY_TRADING_DATA", "157348"))
    time_of_request = int(time.time())
    dune_query = DuneQuery("", "", QUERY, Network.MAINNET, [], query_id)
    # fetch data
    data = dune.fetch(dune_query)
    data_set = {"user_data": data, "time_of_download": time_of_request}

    with open(file_entire_history, "w+", encoding="utf-8") as f:
        json.dump(data_set, f, ensure_ascii=False, indent=4)
