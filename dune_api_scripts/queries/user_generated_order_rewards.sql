CREATE OR REPLACE VIEW dune_user_generated.{{TableName}} (
    solver,
    tx_hash,
    order_uid,
    amount,
    safe_liquidity
) AS (
    select solver::bytea, tx_hash::bytea, order_uid::bytea, amount::numeric, safe_liquidity::bool
    from (VALUES
{{Values}}
    ) as _ (solver, tx_hash, order_uid, amount, safe_liquidity)
);