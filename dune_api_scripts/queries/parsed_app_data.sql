CREATE OR REPLACE VIEW
    dune_user_generated.gp_appdata (app_data, referrer)
    AS VALUES {{VALUES}};

DROP VIEW dune_user_generated.gnosis_protocol_v2_app_data CASCADE;
CREATE OR REPLACE VIEW
    dune_user_generated.gnosis_protocol_v2_app_data
AS (
    -- The following query is built on top of https://dune.xyz/queries/257782
    with
    partialy_parsed_app_info as (
        SELECT
            app_data as app_id,
            referrer::json -> 'appCode' as app_code,
            referrer::json -> 'version' as app_version,
            -- Notice there are 2 different environment fields.
            -- One of them being utilized more than the other
            referrer::json -> 'environment' as backend_env,
            (referrer::json -> 'metadata')::json -> 'environment' as meta_env,
            ((referrer::json -> 'metadata')::json -> 'referrer')::json -> 'address' as referrer,
            ((referrer::json -> 'metadata')::json -> 'referrer')::json -> 'version' as referrer_version,
            ((referrer::json -> 'metadata')::json -> 'quote')::json -> 'version' as quote_version,
            ((referrer::json -> 'metadata')::json -> 'quote')::json -> 'slippageBips' as slippage_bips
        from dune_user_generated.gp_appdata
    ),

    fully_parsed_app_data as (
        select
            app_id,
            trim('"' from app_code::text) as app_code,
            trim('"' from app_version::text) as app_version,
            -- So we don't get the string 'null' in our result table!
            case
                when meta_env::text = 'null' then null
                else trim('"' from meta_env::text)
            end as meta_env,
            case
                when backend_env::text = 'null' then null
                else trim('"' from backend_env::text)
            end as backend_env,
            trim('"' from referrer::text) as referrer,
            trim('"' from referrer_version::text) as referrer_version,
            trim('"' from quote_version::text) as quote_version,
            trim('"' from slippage_bips::text) as slippage_bips
        from partialy_parsed_app_info
    ),

    -- Fetching the others.
    trade_call_data_and_hash as (
        SELECT
            jsonb_array_elements(trades) as trade_call_data
        FROM gnosis_protocol_v2."GPv2Settlement_call_settle" call
     ),

    decoded_trade_call_data_and_hash as (
        SELECT trim( '"' from (trade_call_data->'appData')::text) as app_data
        FROM trade_call_data_and_hash
    ),

    all_app_hashes as (
        SELECT distinct(app_data) as app_hash
        from decoded_trade_call_data_and_hash
    )

    SELECT
        app_hash,
        app_code,
        app_version,
        backend_env,
        meta_env,
        referrer,
        referrer_version,
        slippage_bips::int
    FROM all_app_hashes
    LEFT OUTER JOIN fully_parsed_app_data
    ON app_hash = app_id
);

select * from dune_user_generated.gnosis_protocol_v2_app_data
