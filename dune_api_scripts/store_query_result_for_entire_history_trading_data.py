import json
from utils import dune_from_environment
from utils import check_whether_entire_history_file_was_already_downloade
from utils import parse_data_from_dune_query


# Entire history does not need to be downloaded again, if file was already downloaded in the past and exists.
file_entire_history = check_whether_entire_history_file_was_already_downloade()

# initialize the enviroment
dune = dune_from_environment()

# fetch query result id using query id
result_id = dune.query_result_id(query_id=157348)

# fetch query result
data = dune.query_result(result_id)

# parse data
data_set = parse_data_from_dune_query(data)

# write to file, if non-empty
if bool(data_set):
    with open(file_entire_history, 'w+', encoding='utf-8') as f:
        json.dump(data_set, f, ensure_ascii=False, indent=4)
else:
    print("query is still calculating")
