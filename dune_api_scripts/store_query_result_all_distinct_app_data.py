"""
Queries and stores all distinct app data in a file `distinct_app_data.json`
"""
import json
import os
import time
from pathlib import Path
from dotenv import load_dotenv
from duneapi.api import DuneAPI
from duneapi.types import DuneQuery
from duneapi.util import open_query

if __name__ == "__main__":
    load_dotenv()
    entire_history_path = Path(os.environ["DUNE_DATA_FOLDER"] + "/app_data/")
    os.makedirs(entire_history_path, exist_ok=True)

    # initialize the environment
    dune = DuneAPI.new_from_environment()

    # fetch query result id using query id
    time_of_request = int(time.time())
    dune_query = DuneQuery(
        query_id=int(os.getenv("QUERY_ID_ALL_APP_DATA", "142824")),
        raw_sql=open_query("./dune_api_scripts/queries/all_app_data.sql")
    )

    # fetch query result
    app_data = dune.fetch(dune_query)

    # parse dat
    data_set = {"app_data": app_data, "time_of_download": time_of_request}

    filename = os.path.join(entire_history_path, Path("distinct_app_data.json"))
    with open(filename, "w+", encoding="utf-8") as f:
        json.dump(data_set, f, ensure_ascii=False, indent=4)
