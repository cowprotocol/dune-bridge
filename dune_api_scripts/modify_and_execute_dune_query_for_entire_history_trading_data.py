"""
Modifies and executes dune query for entire history of trading data.
"""
import datetime
import os

from queries import build_query_for_affiliate_data
from utils import dune_from_environment
from utils import open_downloaded_history_file


def build_query_for_all_trading_data():
    """
    Constructs query for all time trading data
    """
    start_date = "'2021-03-01'"  # Launch date (approx)
    today = datetime.date.today()
    # End date will be the midnight between yesterday and today, as hours are cut off
    end_date = f'\'{today}\''
    return build_query_for_affiliate_data(start_date, end_date)


# Entire history does not need to be downloaded again. do not run query,
# if the download has been done in the past and file exists
file_entire_history = open_downloaded_history_file()

# initialize the environment
dune = dune_from_environment()

# build query
QUERY = build_query_for_all_trading_data()

# update query in dune
query_id = int(os.getenv('QUERY_ID_ENTIRE_HISTORY_TRADING_DATA', "157348"))
dune.initiate_new_query(query_id, query=QUERY)

# run query in dune
dune.execute_query(query_id)
