from utils import parse_data_from_dune_query, store_as_json_file, dune_from_environment

# initialize the enviroment
dune = dune_from_environment()

# fetch query result id using query id
result_id = dune.query_result_id(query_id=135804)

# fetch query result
data = dune.query_result(result_id)

# parse data
data_set = parse_data_from_dune_query(data)

# write to file, if non-empty
if bool(data_set):
    store_as_json_file(data_set)
else:
    print("query is still calculating")
