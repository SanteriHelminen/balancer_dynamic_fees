{{ 
    config(
        materialized = 'table'
    ) 
}}

with lvr_calculation as (
    select
        block_number,
        pool_id,
        pool_price,
        price_target,
        open_price,
        reserve_0_usd,
        reserve_1_usd,
        fee_tier,
        sqrt(reserve_0_usd * reserve_1_usd) as liquidity,
        sqrt(reserve_0_usd * reserve_1_usd) * abs((price_target - pool_price)/pool_price) as executed_qty,
        sqrt(pool_price * price_target) as average_price,
        case
            when (pool_price < price_target) and (price_target < open_price) and (price_target / pool_price > 1 + fee_tier) then true
            when (pool_price > price_target) and (price_target > open_price) and (pool_price / price_target > 1 + fee_tier) then true
            else false
        end as can_have_lvr,
        abs(open_price - pool_price) as price_diff
    from {{ ref('int_polygon_lvr_pool_reserves') }}
    where (block_number <= 54445409 or block_number > 55278791)
),

price_diff_percentiles as (
    select
        pool_id,
        percentile_cont(0.9) within group (order by price_diff) as ninetyfifth_percentile
    from lvr_calculation
    group by pool_id
),

lvr_results as (
    select
        l.block_number,
        l.pool_id,
        l.fee_tier,
        l.pool_price,
        l.price_target,
        l.open_price,
        l.liquidity,
        l.executed_qty,
        if(l.can_have_lvr, l.executed_qty * abs((l.open_price - l.average_price)/l.average_price), 0) as lvr_value,
        if(l.can_have_lvr, l.fee_tier * l.executed_qty, 0) as fee,
        l.can_have_lvr,
        l.price_diff
    from
        lvr_calculation l
    join price_diff_percentiles p
        on l.pool_id = p.pool_id
    where
        l.price_diff <= p.ninetyfifth_percentile
        and (l.block_number <= 54445409 or l.block_number > 55278791)
)

select distinct
    pools.pool_name,
    lr.*
from lvr_results lr
left join {{ ref('dim_pools') }} pools
    on pools.pool_id = lr.pool_id
order by lr.pool_id, lr.block_number