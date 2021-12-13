"""
A collection of fixed dune queries which, when combined make the entire affiliate query.
"""
from .utils import build_string_for_affiliate_referrals_pairs


def build_query_for_affiliate_data():
    """
    Returns one large query which fetches affiliate data in given date range.
    """
    query_affiliate = """WITH
    -- first table is representing affiliate inputs from outside of dune
    -- This table provides the mapping between affiliate and appData
    mapping_appdata_affiliate as (
        SELECT * FROM (
            VALUES """ + build_string_for_affiliate_referrals_pairs() + """
        ) as t("appData", referrer)
    ),
    """

    query_constant = """
    -- Table with first trade per user. Used to determine their referral
    first_trade_per_owner AS (
        SELECT DISTINCT ON (owner)
            ROW_NUMBER() OVER (Partition By evt_tx_hash ORDER BY evt_index) as evt_position,
            owner,
            evt_tx_hash as tx_hash,
            "evt_block_time" as batch_time
        FROM gnosis_protocol_v2."GPv2Settlement_evt_Trade" trades
       order by owner, evt_block_time ASC
    ),

    -- Table with all the call_data of trades. Used to parse the app_data per trade
    trade_call_data_and_hash as (
    SELECT
        position,
        item_object->'appData' as appdata,
        "call_tx_hash"
        FROM gnosis_protocol_v2."GPv2Settlement_call_settle" call,
        jsonb_array_elements(trades) with ordinality arr(item_object, position)
    ),

    -- Table with first app_data used by user
    first_app_data_used_per_user as (
    Select
        Replace(app_data.appdata::text, '"', '') as "appData",
        first_trade.owner::TEXT
        FROM trade_call_data_and_hash app_data
    inner join first_trade_per_owner first_trade
    on app_data."call_tx_hash" = first_trade."tx_hash"
    and app_data.position= first_trade.evt_position),

    -- Table with mapping between referral and their users
    referral_of_user as (
        Select mapping_appdata_affiliate.referrer as referrer,
               first_app_data_used_per_user.owner
        FROM mapping_appdata_affiliate
                 inner join first_app_data_used_per_user
                            on mapping_appdata_affiliate."appData" =
                               first_app_data_used_per_user."appData"
    ),

    -- Provides users stats within GP
    user_stats_of_gp as (
        SELECT date_trunc('day', block_time) as day,
               count(*)                      as number_of_trades,
               sum(trade_value_usd)          as cowswap_usd_volume,
               trader::TEXT                  as owner
        FROM gnosis_protocol_v2."view_trades"
        GROUP BY 1, owner
        ORDER BY owner DESC),

    -- Table with the affiliate program results
    affiliate_program_results as (
        Select day,
               referrer,
               sum(cowswap_usd_volume)                                as "total_referred_volume",
               ARRAY_AGG(DISTINCT
                         Replace(user_stats_of_gp.owner, '\\x', '0x')) as "referrals"
        from user_stats_of_gp
                 inner join referral_of_user
                            on user_stats_of_gp.owner = referral_of_user.owner
        where referrer is not NULL
        group by 1, referrer
    )

    -- Final table
    Select
        Replace(
            CASE 
                WHEN ar.referrer is NUll THEN tr.owner 
                ELSE ar.referrer  
            END, '\\x', '0x') as owner,
        CASE WHEN ar.day is NUll THEN tr.day ELSE ar.day END as day,
        total_referred_volume,
        referrals,
        number_of_trades,
        cowswap_usd_volume,
        0 as usd_volume_all_exchanges
        -- This value will be set in the future with a new join of tables. 
        -- It's not yet published here as we don't need it at the beginning.
    from affiliate_program_results ar
    full outer join user_stats_of_gp tr 
    on ar.referrer = tr.owner and (ar.day = tr.day or ar.day = null or tr.day = null)
    """
    return query_affiliate + query_constant
