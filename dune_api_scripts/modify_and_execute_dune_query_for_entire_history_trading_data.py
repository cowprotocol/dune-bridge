from duneanalytics import DuneAnalytics
import datetime
from utils import dune_from_environment
from queries import build_query_for_affiliate_data

from utils import check_whether_entire_history_file_was_already_downloade


def build_query_for_todays_trading_volume():

    startDate = "'2021-03-01'"
    today = datetime.date.today()
    yesterday = today + datetime.timedelta(days=-1)
    endDate = "'{}'".format(yesterday.strftime("%Y-%m-%d"))

    return build_query_for_affiliate_data(startDate, endDate)


# Entire history does not need to be downloaded again. do not run query, if the download has been done in the past and file exists
file_entire_history = check_whether_entire_history_file_was_already_downloade()

# initialize the enviroment
dune = dune_from_environment()

# build query
query = build_query_for_todays_trading_volume()

# update query in dune
dune.initiate_new_query(query_id=157348, query=query)

# run query in dune
dune.execute_query(query_id=157348)
