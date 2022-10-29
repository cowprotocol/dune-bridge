select solver,
       count(*)                                                 as num_trades,
       sum(case when safe_liquidity is True then 1 else 0 end)  as num_safe,
       sum(case when safe_liquidity is False then 1 else 0 end) as num_unsafe,
       sum(amount)                                              as raw_rewards
from dune_user_generated.cow_order_rewards_{{Environment}}
where page < {{MaxPage}}