"""Simple interface for executing QUERY_ID_ALL_APP_DATA"""
import os
from duneapi.api import DuneAPI


# initialize the environment
dune = DuneAPI.new_from_environment()

# execute query again
query_id = int(os.getenv('QUERY_ID_ALL_APP_DATA', "142824"))
dune.execute_query(query_id)
