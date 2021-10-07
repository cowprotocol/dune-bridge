from utils import build_string_for_affiliate_referrals_pairs


def build_query_for_affiliate_data(startDate, endDate):
    queryAffiliate = """ WITH
    -- first table is representing affiliate inputs from outside of dune
    -- This table provides the mapping between affiliate and appData
    mapping_appdata_affiliate as (
        SELECT * FROM (VALUES """ + build_string_for_affiliate_referrals_pairs() + """    )as t("appData", affiliate)
        ),
        """

    queryConstant = """
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
        sum(fee_value) as cowswap_fee_volume,
        owner
    FROM trades_with_prices
    GROUP BY 1, owner
    ORDER BY owner DESC),

    -- Table with all the call_data of trades. This table is needed in order to parse the appData per trade
    trade_call_data_and_hash as (
    SELECT
        position,
        item_object as "trade_call_data",
        "call_tx_hash"
        FROM gnosis_protocol_v2."GPv2Settlement_call_settle" call,
        jsonb_array_elements(trades) with ordinality arr(item_object, position)
        where call.call_block_time between {startDate} and {endDate}
    ),

    -- All trades with the main characteristics and their appData hash
    decoded_trade_call_data_and_hash as (
    SELECT
        trade_call_data->'appData' as appdata,
        position,
        "call_tx_hash"
        FROM trade_call_data_and_hash
    ),

    -- Volumes calculated per appData
    app_data_and_volumes as(
    Select trade_data.day,
        Replace(app_data.appdata::text, '"', '') as "appData",
        sum(trade_data."trade_value") as "sum_usd_volume"
        FROM decoded_trade_call_data_and_hash app_data
    inner join trades_with_prices trade_data
    on app_data."call_tx_hash" = trade_data."tx_hash"
    and app_data.position= trade_data.evt_position
    group by app_data.appdata, trade_data.day),

    -- Number of referrals per appData
    app_data_and_nr_of_referrals as(
    Select
        trade_data.day,
        Replace(app_data.appdata::text, '"', '') as "appData",
        count(distinct(trade_data.owner)) as "nr_of_referrals"
        FROM decoded_trade_call_data_and_hash app_data
    inner join trades_with_prices trade_data
    on app_data."call_tx_hash" = trade_data."tx_hash"
    and app_data.position = trade_data.evt_position
    group by app_data.appdata,  trade_data.day),

    -- Table with the actual affiliate program results
    affiliate_program_results as (
    Select
        CASE WHEN app_data_and_nr_of_referrals.day is Null THEN app_data_and_volumes.day ELSE app_data_and_nr_of_referrals.day END as day,
        affiliate as owner,
        sum(sum_usd_volume) as "total_referred_volume",
        sum(nr_of_referrals) as "nr_of_referrals" from mapping_appdata_affiliate
    inner join app_data_and_volumes on app_data_and_volumes."appData"::text = mapping_appdata_affiliate."appData"::text
    inner join app_data_and_nr_of_referrals on app_data_and_nr_of_referrals."appData"::text = mapping_appdata_affiliate."appData"::text
    group by 1, affiliate
    )

    -- Final table
    Select
        CASE WHEN ar.owner is NUll THEN tr.owner::TEXT ELSE ar.owner END as owner,
        CASE WHEN ar.day is NUll THEN tr.day ELSE ar.day END as day,
        total_referred_volume,
        nr_of_referrals,
        number_of_trades,
        cowswap_usd_volume,
        0 as usd_volume_all_exchanges 
        -- This value will be set in the future with a new join of tables. It's not yet published here
        -- as we don't need it at the beginning.
    from affiliate_program_results ar
    full outer join user_stats_of_gp tr
    on ar.owner = tr.owner::TEXT
    and tr.day = ar.day
        """.format(startDate=startDate, endDate=endDate)
    return queryAffiliate + queryConstant
