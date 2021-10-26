import os
from duneanalytics import DuneAnalytics
import datetime
from utils import dune_from_environment
from queries import build_query_for_affiliate_data

from utils import open_downloaded_history_file


def build_query_for_todays_trading_volume():

    startDate = "'2021-03-01'"
    today = datetime.date.today()
    yesterday = today + datetime.timedelta(days=-1)
    endDate = "'{}'".format(yesterday.strftime("%Y-%m-%d"))

    return build_query_for_affiliate_data(startDate, endDate)


# Entire history does not need to be downloaded again. do not run query, if the download has been done in the past and file exists
file_entire_history = open_downloaded_history_file()

# initialize the enviroment
dune = dune_from_environment()

# build query
query = build_query_for_todays_trading_volume()

# update query in dune
query_id = int(os.getenv('QUERY_ID_ENTIRE_HISTORY_TRADING_DATA', 157348))
dune.initiate_new_query(query_id, query=query)

# run query in dune
dune.execute_query(query_id)
