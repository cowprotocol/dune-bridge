"""Modifies and executed dune query for today's data"""
from os import getenv

from .utils import dune_from_environment, app_data_entries

if __name__ == "__main__":
    # initialize the environment
    dune = dune_from_environment()
    VALUES = app_data_entries()

    # build query
    QUERY = f"""
    CREATE OR REPLACE VIEW
    dune_user_generated.gp_appdata (app_data, referrer)
    AS VALUES {VALUES};"""

    # update query in dune
    query_id = int(getenv('QUERY_ID_ALL_APP_DATA', "257782"))

    dune.initiate_new_query(query_id, query=QUERY)

    # run query in dune
    dune.execute_query(query_id)
    # Check out the results here: https://dune.xyz/queries/257782
