import json
from datetime import datetime
from pathlib import Path
import os
import time
from utils import dune_from_environment


entire_history_path = Path(os.environ['DUNE_DATA_FOLDER'] + "/app_data/")
os.makedirs(entire_history_path, exist_ok=True)

# initialize the enviroment
dune = dune_from_environment()

# fetch query result id using query id
query_id = os.getenv('QUERY_ID_ALL_APP_DATA', 142824)
result_id = dune.query_result_id(query_id)

# fetch query result
data = dune.query_result(result_id)

# parse dat
app_data = data["data"]["get_result_by_result_id"]
data_set = {
    "app_data": app_data,
    "time_of_download": int(datetime.fromisoformat(data["data"]["query_results"][0]["generated_at"]).timestamp())
}

# write to file, if non-empty
if bool(data_set):
    with open(os.path.join(entire_history_path, Path("distinct_app_data.json")), 'w+', encoding='utf-8') as f:
        json.dump(data_set, f, ensure_ascii=False, indent=4)
else:
    print("query is still calculating")
