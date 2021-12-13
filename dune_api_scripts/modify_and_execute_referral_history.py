"""
Modifies and executes dune query for entire history of trading data.
"""
import os

from .queries import build_query_for_affiliate_data
from .utils import dune_from_environment, open_downloaded_history_file


if __name__ == "__main__":
    # Entire history does not need to be downloaded again. do not run query,
    # if the download has been done in the past and file exists
    file_entire_history = open_downloaded_history_file()

    # initialize the environment
    dune = dune_from_environment()

    # build query
    QUERY = build_query_for_affiliate_data()

    # update query in dune
    query_id = int(os.getenv('QUERY_ID_ENTIRE_HISTORY_TRADING_DATA', "157348"))
    dune.initiate_new_query(query_id, query=QUERY)

    # run query in dune
    dune.execute_query(query_id)
