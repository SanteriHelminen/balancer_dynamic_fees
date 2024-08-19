{{ 
    config(
        materialized = 'table'
    ) 
}}

with pool_reserves as (
    select * from {{ ref('int_polygon_lvr_fee_volume_pool_reserves1') }}
    union all
    select * from {{ ref('int_polygon_lvr_fee_volume_pool_reserves2') }}
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
    where (block_number <= 54445409 or block_number > 55278791)
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
    'GHST-USDC' as pool_name,
    lvr.*
from lvr_results lvr
order by block_number, fee_type, multiplier