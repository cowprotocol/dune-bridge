import json
import os
from duneanalytics import DuneAnalytics
from pathlib import Path
from datetime import datetime


def dune_from_environment():
    """Initialize and authenticate a Dune Analytics client from the current environment."""
    dune = DuneAnalytics(
        os.environ['DUNE_USER'], os.environ['DUNE_PASSWORD'])
    dune.login()
    dune.fetch_auth_token()
    return dune


def parse_data_from_dune_query(data):
    user_data = data["data"]["get_result_by_result_id"]
    date_of_data_creation = datetime.strptime(
        user_data[0]["data"]["day"][0:10], '%Y-%m-%d')
    return {
        "user_data": user_data,
        "time_of_download": date_of_data_creation.strftime("%d/%m/%Y %H:%M:%S")
    }


def store_as_json_file(data_set):
    file_path = Path(os.environ['DUNE_DATA_FOLDER'] + "/user_data/")
    os.makedirs(file_path,  exist_ok=True)
    file_name = Path("user_data_from{}.json".format(datetime.now()))
    with open(os.path.join(file_path, file_name), 'w+', encoding='utf-8') as f:
        json.dump(data_set, f, ensure_ascii=False, indent=4)
    print("Written updates into: " + os.path.join(file_path, file_name))


def build_string_for_affiliate_referrals_pairs():
    file_path = Path(os.environ['APP_DATA_REFERRAL_RELATION_FILE'])
    app_data_referral_link = json.loads(  # loads one input to create always a valid table.
        '{"0x0000000000000000000000000000000000000000000000000000000000000abc": "0x0000000000000000000000000000000000000000"}')
    if file_path.is_file():
        with open(file_path) as json_file:
            app_data_referral_link = json.load(json_file)

    # Building value pairs "(appDataHash, referral),"
    string_of_pair_app_data_referral = ["('{hash}','{referral}')".format(
        hash=hash, referral=app_data_referral_link[hash].replace("0", "/", 1)) for hash in app_data_referral_link]

    return ",".join(string_of_pair_app_data_referral)


def check_whether_entire_history_file_was_already_downloade():
    entire_history_path = Path(os.environ['DUNE_DATA_FOLDER'] + "/user_data")
    os.makedirs(entire_history_path, exist_ok=True)
    file_entire_history = Path(os.path.join(
        entire_history_path, Path("user_data_entire_history.json")))
    if file_entire_history.is_file():
        print("file already downloaded")
        exit()
    return file_entire_history