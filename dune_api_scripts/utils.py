"""
A collection of utility methods for date manipulation, environment constructors,
parsing, reading and writing files.
"""
import json
import os
import sys
import time

from datetime import date, timedelta

from pathlib import Path
from typing import Any


def store_as_json_file(data_set: dict[str, object]) -> None:
    """
    Writes data set to json file.
    """
    file_path = Path(os.environ["DUNE_DATA_FOLDER"] + "/user_data/")
    os.makedirs(file_path, exist_ok=True)
    downloaded_data_timestamp = int(time.time())
    download_day_timestamp = (downloaded_data_timestamp // (24 * 60 * 60)) * (
        24 * 60 * 60
    )
    file_name = Path(f"user_data_from{download_day_timestamp}.json")
    with open(os.path.join(file_path, file_name), "w+", encoding="utf-8") as file:
        json.dump(data_set, file, ensure_ascii=False, indent=4)
    print("Written updates to: " + os.path.join(file_path, file_name))


def build_string_for_affiliate_referrals_pairs() -> str:
    """Constructs a string of affiliate-referral pairs."""
    content_dict = load_app_data_content_map()
    # Building value pairs "(appDataHash, referral),"
    pair_list = []
    for app_id, content in content_dict.items():
        # TODO - I feel like this is a bit shady
        referral = content["metadata"]["referrer"]
        if referral:
            pair_list.append(f"('{app_id}','{dune_address(referral['address'])}')")

    return ",".join(pair_list)


def load_app_data_content_map() -> Any:
    """Loads and returns App Data file from persistent storage"""
    file_path = Path(os.environ["APP_DATA_REFERRAL_RELATION_FILE"])
    if not file_path.is_file():
        # Must wait for the app_data-referrals relationships to be created,
        # in order to construct the query correctly.
        print("APP_DATA_REFERRAL_RELATION_FILE not yet created by service")
        sys.exit()

    with open(file_path, encoding="utf-8") as json_file:
        return json.load(json_file)


def dune_address(hex_address: str) -> str:
    """
    transforms hex address (beginning with 0x) to dune compatible
    byeta by replacing `0x` with `\\x`.
    """
    return hex_address.replace("0x", "\\x")


def app_data_entries() -> str:
    """Constructs a string of app data hash => content pairs."""
    content_dict = load_app_data_content_map()
    pair_list = [
        f"('{appId}','{json.dumps(data)}')" for appId, data in content_dict.items()
    ]
    return ",".join(pair_list)


def open_downloaded_history_file() -> Path:
    """Opens and returns the entire user data history file."""
    entire_history_path = Path(os.environ["DUNE_DATA_FOLDER"] + "/user_data")
    os.makedirs(entire_history_path, exist_ok=True)
    file_entire_history = Path(
        os.path.join(entire_history_path, Path("user_data_entire_history.json"))
    )
    if file_entire_history.is_file():
        print("file already downloaded")
        sys.exit()
    return file_entire_history


def ensure_that_download_is_recent(timestamp: int, max_time_diff: int) -> None:
    """
    Ensures data is recent, or exits the program.
    Parameters:
        timestamp (int): Unix timestamp
        max_time_diff (int): Unix timestamp - time delta in seconds
    """
    now = int(time.time())
    if timestamp < now - max_time_diff:
        print(f"query result not from the last {max_time_diff / 60} mins, aborting")
        sys.exit()


def date_range(start: date, end: date) -> list[date]:
    """Returns a list of dates between start and end both inclusive"""
    results = []
    curr_date = start
    while curr_date <= end:
        results.append(curr_date)
        curr_date += timedelta(days=1)
    return results
