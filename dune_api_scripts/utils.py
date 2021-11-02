"""
A collection of utility methods for date manipulation, environment constructors,
parsing, reading and writing files.
"""
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

from duneanalytics import DuneAnalytics


def dune_from_environment():
    """
    Initialize and authenticate a Dune Analytics client from the current environment.
    """
    dune = DuneAnalytics(
        os.environ['DUNE_USER'], os.environ['DUNE_PASSWORD'])
    dune.login()
    dune.fetch_auth_token()
    return dune


def parse_dune_iso_format_to_timestamp(dune_iso_string):
    """
    Dune is incompatible with the iso format, as sometimes the amount of milliseconds
    are not represented with the right amount of digits.
    Hence, we cut of the provided time and set the end of the string to '.000000+00:00'.
    """
    return datetime.fromisoformat(dune_iso_string[0:19] + '.000000+00:00').timestamp()


def parse_data_from_dune_query(data):
    """Parses user data and execution date from query result."""
    user_data = data["data"]["get_result_by_result_id"]
    date_of_data_execution = parse_dune_iso_format_to_timestamp(
        data["data"]["query_results"][0]["generated_at"])
    return {
        "user_data": user_data,
        "time_of_download": int(date_of_data_execution)
    }


def store_as_json_file(data_set):
    """
    Writes data set to json file.
    """
    file_path = Path(os.environ['DUNE_DATA_FOLDER'] + "/user_data/")
    os.makedirs(file_path, exist_ok=True)
    # TODO - use something more like this.
    #  time_stamp_of_day_of_download = datetime.timestamp(
    #     datetime.fromtimestamp(data_set["time_of_download"]).date()
    #  )
    download_day_timestamp = (data_set["time_of_download"] // (24 * 60 * 60)) * (
        24 * 60 * 60)
    file_name = Path(f'user_data_from{download_day_timestamp}.json')
    with open(os.path.join(file_path, file_name), 'w+', encoding='utf-8') as file:
        json.dump(data_set, file, ensure_ascii=False, indent=4)
    print("Written updates to: " + os.path.join(file_path, file_name))


def build_string_for_affiliate_referrals_pairs():
    """Constructs a string of affiliate-referral pairs."""
    file_path = Path(os.environ['APP_DATA_REFERRAL_RELATION_FILE'])
    app_data_referral_link = json.loads(
        # loads one input to create always a valid table.
        '{"0x0000000000000000000000000000000000000000000000000000000000000abc": '
        '"0x0000000000000000000000000000000000000000"}'
    )
    if file_path.is_file():
        with open(file_path, encoding='utf-8') as json_file:
            app_data_referral_link = json.load(json_file)
    else:
        # In this case, we need to wait for the referrals to be created, in order to construct
        # the correct query.
        exit()

    # Building value pairs "(appDataHash, referral),"
    # pylint: disable=consider-using-f-string
    string_of_pair_app_data_referral = [
        "('{hash}','{referral}')".format(
            hash=data_hash,
            referral=app_data_referral_link[data_hash].replace("0", "\\", 1))
        for data_hash in app_data_referral_link
    ]

    return ",".join(string_of_pair_app_data_referral)


def open_downloaded_history_file():
    """Opens and returns the entire user data history file."""
    entire_history_path = Path(os.environ['DUNE_DATA_FOLDER'] + "/user_data")
    os.makedirs(entire_history_path, exist_ok=True)
    file_entire_history = Path(os.path.join(
        entire_history_path, Path("user_data_entire_history.json")))
    if file_entire_history.is_file():
        print("file already downloaded")
        sys.exit()
    return file_entire_history


def ensure_that_download_is_recent(data, max_time_diff):
    """
    Ensures data is recent, or exits the program.
    """
    if data["time_of_download"] < int(time.time()) - max_time_diff:
        print(
            f'query result not from the last {max_time_diff / 60} mins, aborting')
        sys.exit()
