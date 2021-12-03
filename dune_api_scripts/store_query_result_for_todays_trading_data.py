"""
Stores the result of querying today's trading history in a file called
`user_data_from{today's date}.json`.
Note that this file name is dictated by method `utils.store_as_json_file`.
"""
import os

from .utils import parse_data_from_dune_query, store_as_json_file, \
    dune_from_environment, \
    ensure_that_download_is_recent

if __name__ == "__main__":
    # initialize the environment
    dune = dune_from_environment()

    # fetch query result id using query id
    query_id = int(os.getenv('QUERY_ID_TODAYS_TRADING_DATA', "249240"))
    result_id = dune.query_result_id(query_id)

    # fetch query result
    data = dune.query_result(result_id)

    # parse data
    data_set = parse_data_from_dune_query(data)

    # in case the data is not from within the last 10 mins,
    # we want to wait for a new query result and hence exit:
    ensure_that_download_is_recent(data_set["time_of_download"], 10 * 60)

    # write to file, if non-empty
    if bool(data_set):
        store_as_json_file(data_set)
    else:
        print("query is still calculating")
