CREATE OR REPLACE VIEW dune_user_generated.cow_order_rewards_{{Environment}} (
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
SELECT * FROM dune_user_generated.cow_order_rewards_{{Environment}}