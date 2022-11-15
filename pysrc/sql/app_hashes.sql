-- App Hashes: https://dune.com/queries/1608286
with
app_hashes as (
    select
        min(call_block_number) first_block_seen,
        get_json_object(trade, '$.appData') as app_hash
    from gnosis_protocol_v2_ethereum.GPv2Settlement_call_settle
        lateral view explode(trades) as trade
    group by app_hash
)
select app_hash from app_hashes
where first_block_seen > '{{BlockFrom}}'
and first_block_seen <= '{{BlockTo}}'
