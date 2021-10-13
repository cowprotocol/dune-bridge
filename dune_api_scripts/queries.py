from utils import build_string_for_affiliate_referrals_pairs


def build_query_for_affiliate_data(startDate, endDate):
    queryAffiliate = """ WITH
    -- first table is representing affiliate inputs from outside of dune
    -- This table provides the mapping between affiliate and appData
    mapping_appdata_affiliate as (
        SELECT * FROM (VALUES """ + build_string_for_affiliate_referrals_pairs() + """    )as t("appData", referrer)
        ),
        """

    queryConstant = """
    -- Table with first trade per user. The first trade will be used to determine their referral
    first_trade_per_owner AS (
        SELECT DISTINCT ON (owner)
            ROW_NUMBER() OVER (Partition By evt_tx_hash ORDER BY evt_index) as evt_position,
            owner,
            evt_tx_hash as tx_hash,
            "evt_block_time" as batch_time
        FROM gnosis_protocol_v2."GPv2Settlement_evt_Trade" trades
       order by owner, evt_block_time ASC
    ),

    -- Table with all the call_data of trades. This table is needed in order to parse the appData per trade
    trade_call_data_and_hash as (
    SELECT
        position,
        item_object->'appData' as appdata,
        "call_tx_hash"
        FROM gnosis_protocol_v2."GPv2Settlement_call_settle" call,
        jsonb_array_elements(trades) with ordinality arr(item_object, position)
    ),


    -- Table with first appData used by user
    first_app_data_used_per_user as(
    Select 
        Replace(app_data.appdata::text, '"', '') as "appData",
        frist_trade.owner::TEXT
        FROM trade_call_data_and_hash app_data
    inner join first_trade_per_owner frist_trade
    on app_data."call_tx_hash" = frist_trade."tx_hash"
    and app_data.position= frist_trade.evt_position),
    
    -- Table with mapping between referral and their users
    referral_of_user as(
    Select 
        mapping_appdata_affiliate.referrer as referrer,
        first_app_data_used_per_user.owner
        FROM mapping_appdata_affiliate 
    inner join first_app_data_used_per_user
    on mapping_appdata_affiliate."appData" = first_app_data_used_per_user."appData"
    ),
    
    -- Table with all the trades for the users with prices for sell tokens
    trades_with_sell_price AS (
        SELECT
            ROW_NUMBER() OVER (Partition By evt_tx_hash ORDER BY evt_index) as evt_position,
            evt_tx_hash as tx_hash,
            ("sellAmount" - "feeAmount") as token_sold,
            "evt_block_time" as batch_time,
            evt_tx_hash,
            owner,
            "orderUid",
            "sellToken" as sell_token,
            "buyToken" as buy_token,
            ("sellAmount" - "feeAmount")/ pow(10,p.decimals) as units_sold,
            "buyAmount",
            "sellAmount",
            "feeAmount" / pow(10,p.decimals) as fee,
            price as sell_price
        FROM gnosis_protocol_v2."GPv2Settlement_evt_Trade" trades
        LEFT OUTER JOIN prices.usd as p
            ON trades."sellToken" = p.contract_address
            AND p.minute between {startDate} and {endDate}
            AND date_trunc('minute', p.minute) = date_trunc('minute', evt_block_time)
    Where evt_block_time between {startDate} and {endDate}
    ),

    -- Table with all the trades for the users with prices for sell tokens and buy tokens
    trades_with_prices AS (
        SELECT
            date_trunc('day', batch_time) as day,
            batch_time,
            evt_position,
            evt_tx_hash,
            evt_tx_hash as tx_hash,
            owner,
            token_sold,
            "orderUid",
            sell_token,
            buy_token,
            units_sold,
            "buyAmount" / pow(10,p.decimals) as units_bought,
            fee,
            sell_price,
            price as buy_price,
            (CASE
                WHEN sell_price IS NOT NULL THEN sell_price * units_sold
                WHEN sell_price IS NULL AND price IS NOT NULL THEN price * "buyAmount" / pow(10,p.decimals)
                ELSE  -0.01
            END) as trade_value,
            sell_price * fee as fee_value
        FROM trades_with_sell_price t
        LEFT OUTER JOIN prices.usd as p
            ON p.contract_address = (
                    CASE
                        WHEN t.buy_token = '\\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '\\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                        ELSE t.buy_token
                    END)
            AND  p.minute between {startDate} and {endDate}
            AND date_trunc('minute', p.minute) = date_trunc('minute', batch_time)
    ),

    -- Provides users stats within GP
    user_stats_of_gp as (
    SELECT
        date_trunc('day', batch_time) as day,
        count(*) as number_of_trades,
        sum(trade_value) as cowswap_usd_volume,
        owner::TEXT
    FROM trades_with_prices
    GROUP BY 1, owner
    ORDER BY owner DESC),

    -- Table with the affiliate program results
    affiliate_program_results as (
    Select
        day,
        referrer,
        sum(cowswap_usd_volume) as "total_referred_volume",
        sum(number_of_trades) as "nr_of_referrals" 
        from user_stats_of_gp
        inner join referral_of_user on user_stats_of_gp.owner = referral_of_user.owner
    where referrer is not NULL
    group by 1, referrer
    )
    
    -- Final table
    Select
        Replace(CASE WHEN ar.referrer is NUll THEN tr.owner ELSE ar.referrer  END, '\\x', '0x') as owner,
        CASE WHEN ar.day is NUll THEN tr.day ELSE ar.day END as day,
        total_referred_volume,
        nr_of_referrals,
        number_of_trades,
        cowswap_usd_volume,
        0 as usd_volume_all_exchanges 
        -- This value will be set in the future with a new join of tables. It's not yet published here
        -- as we don't need it at the beginning.
    from affiliate_program_results ar 
    full outer join user_stats_of_gp tr on ar.referrer = tr.owner and (ar.day = tr.day or ar.day = null or tr.day = null)
        """.format(startDate=startDate, endDate=endDate)
    return queryAffiliate + queryConstant
