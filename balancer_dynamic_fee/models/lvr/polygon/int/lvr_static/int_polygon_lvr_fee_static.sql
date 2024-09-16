{{ 
    config(
        materialized = 'table',
        tags = ['polygon', 'polygon_lvr']
    ) 
}}

with pool_reserves as (
    select * from {{ ref('int_polygon_lvr_fee_static_pool_reserves1') }}
),

lvr_calculation as (
    select
        block_number,
        pool_id,
        pool_price,
        price_target,
        open_price,
        fee_tier,
        fee_type,
        multiplier,
        abs(pool_price - open_price) as price_difference,
        sqrt(reserve_0_usd * reserve_1_usd) as liquidity,
        sqrt(reserve_0_usd * reserve_1_usd) * abs((price_target - pool_price)/pool_price) as executed_qty,
        sqrt(pool_price * price_target) as average_price,
        case
            when (pool_price < price_target) and (price_target < open_price) and (price_target / pool_price > 1 + fee_tier) then true
            when (pool_price > price_target) and (price_target > open_price) and (pool_price / price_target > 1 + fee_tier) then true
            else false
        end as can_have_lvr
    from pool_reserves
    where (block_number <= 188600485 or block_number > 196038995)
),

price_difference_percentiles as (
    select
        pool_id,
        multiplier,
        fee_type,
        percentile_cont(0.9) within group (order by price_difference) as ninetyfifth_percentile
    from lvr_calculation
    group by pool_id, multiplier, fee_type
),

lvr_results as (
    select
        l.block_number,
        l.pool_id,
        l.fee_tier,
        l.fee_type,
        l.multiplier,
        if(l.can_have_lvr, l.executed_qty * abs((l.open_price - l.average_price)/l.average_price), 0) as lvr_value,
        if(l.can_have_lvr, l.fee_tier * l.executed_qty, 0) as fee,
        l.can_have_lvr
    from lvr_calculation l
    join price_difference_percentiles p 
        on l.pool_id = p.pool_id 
        and l.multiplier = p.multiplier 
        and l.fee_type = p.fee_type
    where l.price_difference <= p.ninetyfifth_percentile
)

select distinct
    pools.pool_name,
    lvr.* 
from lvr_results lvr
left join {{ ref('dim_pools') }} pools on pools.pool_id = lvr.pool_id
order by block_number, lvr.pool_id, fee_type, multiplier