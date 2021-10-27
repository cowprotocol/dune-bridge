"""Simple interface for executing QUERY_ID_ALL_APP_DATA"""
import os

from utils import dune_from_environment

# initialize the environment
dune = dune_from_environment()

# execute query again
query_id = int(os.getenv('QUERY_ID_ALL_APP_DATA', "142824"))
dune.execute_query(query_id)
