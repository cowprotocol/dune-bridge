with
-- CowProtocol Users whose first trade was {{NumDays}} days before query DateFor,
-- with boolean flag indicating if they traded in the past {{NumDays}} days
cow_traders as (
    select
        owner                                                          as trader,
        max(evt_block_time) > date('{{DateFor}}') - interval '{{NumDays}} days' as cow_recent
    from gnosis_protocol_v2."GPv2Settlement_evt_Trade"
    where evt_block_time between '2021-04-28' and date('{{DateFor}}')
    group by owner
    having min(evt_block_time) < date('{{DateFor}}') - interval '{{NumDays}} days'
),

-- Extend the above table with further data from other dexes:
-- Includes all from above along with latest trade date from other dexes
retention_data as (
    select
        trader,
        cow_recent,
        max(block_time) as other_latest
    from cow_traders
    left outer join dex.trades
        on trader = trader_a
        and project != 'CoW Protocol'
        and block_time > date('{{DateFor}}') - interval '{{NumDays}} days'
        and block_time <= date('{{DateFor}}')
    group by trader, cow_recent
),
-- Write boolean value other_recent replacing other_latest date
pre_classification as (
    select trader,
        cow_recent,
        other_latest is not null and other_latest > date('{{DateFor}}') - interval '{{NumDays}} days' as other_recent
    from retention_data
),
-- One final intermediary step to classify trader types based on boolean flags cow_recent and other_recent
retention_classificaition as (
    select
        case
            when cow_recent and not other_recent then 'retained'
            when cow_recent and other_recent then 'hybrid'
            when not cow_recent and other_recent then 'lost'
            else 'gone'
        end as trader_type
    from pre_classification
)
-- The last query selects a single row for the query date with counts for (retained, hybrid, lost, gone):
select date('{{DateFor}}')                                       as day,
       sum(case when trader_type = 'retained' then 1 else 0 end) as retained,
       sum(case when trader_type = 'hybrid' then 1 else 0 end)   as hybrid,
       sum(case when trader_type = 'lost' then 1 else 0 end)     as lost,
       sum(case when trader_type = 'gone' then 1 else 0 end)     as gone
from retention_classificaition

