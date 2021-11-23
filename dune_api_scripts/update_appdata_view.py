"""Modifies and executed dune query for today's data"""
from .utils import dune_from_environment, build_string_for_affiliate_referrals_pairs

if __name__ == "__main__":
    # initialize the environment
    dune = dune_from_environment()

    # build query
    QUERY = f"""CREATE OR REPLACE VIEW 
    dune_user_generated.gp_appdata (app_data, referrer) 
    AS VALUES {build_string_for_affiliate_referrals_pairs()}"""

    # update query in dune
    query_id = 257782
    dune.initiate_new_query(query_id, query=QUERY)

    # run query in dune
    dune.execute_query(query_id)
