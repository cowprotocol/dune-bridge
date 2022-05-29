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
            (referrer::json -> 'metadata')::json as metadata
        from dune_user_generated.gp_appdata
    ),

    further_parsed_app_info as (
        select
            app_id,
            app_code,
            app_version,
            metadata -> 'environment' as environment,
            (metadata -> 'referrer')::json as referal
        from partialy_parsed_app_info
    ),


    fully_parsed_app_data as (
        select
            app_id,
            app_code::text,
            app_version::text,
            environment::text,
            (referal -> 'address')::text as referrer,
            (referal -> 'version')::text as referal_version
        from further_parsed_app_info
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
        trim('"' from app_code) as app_code,
        trim('"' from app_version) as app_version,
        trim('"' from environment) as environment,
        trim('"' from referrer) as referrer,
        trim('"' from referal_version) as referal_version
    FROM all_app_hashes
    LEFT OUTER JOIN fully_parsed_app_data
    ON app_hash = app_id
);

select * from dune_user_generated.gnosis_protocol_v2_app_data
