"""
Stores the result of querying all distinct app data in a file `distinct_app_data.json`
"""
import json
import os
from pathlib import Path

from .utils import dune_from_environment, parse_dune_iso_format_to_timestamp

if __name__ == "__main__":
    entire_history_path = Path(os.environ['DUNE_DATA_FOLDER'] + "/app_data/")
    os.makedirs(entire_history_path, exist_ok=True)

    # initialize the environment
    dune = dune_from_environment()

    # fetch query result id using query id
    query_id = int(os.getenv('QUERY_ID_ALL_APP_DATA', "142824"))
    result_id = dune.query_result_id(query_id)

    # fetch query result
    data = dune.query_result(result_id)

    # parse dat
    app_data = data["data"]["get_result_by_result_id"]
    data_set = {
        "app_data": app_data,
        "time_of_download": int(parse_dune_iso_format_to_timestamp(
            data["data"]["query_results"][0]["generated_at"]))
    }

    # write to file, if non-empty
    if bool(data_set):
        filename = os.path.join(entire_history_path, Path("distinct_app_data.json"))
        with open(filename, 'w+', encoding='utf-8') as f:
            json.dump(data_set, f, ensure_ascii=False, indent=4)
    else:
        print("query is still calculating")
