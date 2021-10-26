import os
from utils import dune_from_environment

# initialize the enviroment
dune = dune_from_environment()

# execute query again
query_id = os.getenv('QUERY_ID_ALL_APP_DATA', 142824)
dune.execute_query(query_id)
