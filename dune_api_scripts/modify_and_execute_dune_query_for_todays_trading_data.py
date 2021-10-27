import datetime
import os

from queries import build_query_for_affiliate_data
from utils import dune_from_environment

JOB_FREQUENCY_IN_MINUTES = 5


def build_query_for_todays_trading_volume():
    today = datetime.date.today() - datetime.timedelta(minutes=JOB_FREQUENCY_IN_MINUTES)
    tomorrow = today + datetime.timedelta(days=1)
    start_date = "'{}'".format(today.strftime("%Y-%m-%d"))
    end_date = "'{}'".format(tomorrow.strftime("%Y-%m-%d"))

    return build_query_for_affiliate_data(start_date, end_date)


# initialize the environment
dune = dune_from_environment()

# build query
query = build_query_for_todays_trading_volume()

# update query in dune
query_id = int(os.getenv('QUERY_ID_TODAYS_TRADING_DATA', 135804))
dune.initiate_new_query(query_id, query=query)

# run query in dune
dune.execute_query(query_id)
