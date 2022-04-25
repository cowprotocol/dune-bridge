With trade_call_data_and_hash as (
    SELECT
        jsonb_array_elements(trades) as trade_call_data,
        "call_tx_hash"
    FROM
        gnosis_protocol_v2."GPv2Settlement_call_settle" call
    where
        call_block_time > '2021-10-09'
),
decoded_trade_call_data_and_hash as (
    SELECT
        trade_call_data -> 'appData' as appdata,
        "call_tx_hash"
    FROM
        trade_call_data_and_hash
)
Select
    distinct(appdata)
from
    decoded_trade_call_data_and_hash