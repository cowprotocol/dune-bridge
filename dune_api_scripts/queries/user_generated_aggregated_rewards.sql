-- https://dune.com/queries/1476356
CREATE OR REPLACE VIEW dune_user_generated.cow_order_rewards_{{Environment}} (
    solver,
    tx_hash,
    order_uid,
    amount,
    safe_liquidity
) AS (
{{Values}}
);