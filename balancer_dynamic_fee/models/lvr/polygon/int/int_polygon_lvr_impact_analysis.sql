{{ 
    config(
        materialized = 'table'
    ) 
}}

{% set base_fees = [0.001, 0.0025, 0.005, 0.10, 0.20] %}

with 
{% for base_fee in base_fees %}
fees_{{ loop.index }} as (
    select
        block_number,
        pool_id,
        id,
        {{ base_fee }} as fee_tier
    from {{ ref('int_polygon_sim_swaps') }}
),

pool_reserves_{{ loop.index }} as (
    select
        reserves.block_number,
        reserves.pool_id,
        swaps.id,
        reserves.weight_0,
        reserves.weight_1,
        reserves.reserve0,
        reserves.reserve0 * prices.price as reserve_0_usd,
        reserves.reserve1,
        case
            when reserves.token1_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                then multiply(reserves.reserve_1, prices.eth_price)
            else reserves.reserve_1
        end as reserve_1_usd,
        fees_{{ loop.index }}.fee_tier,
        swaps.price_target,
        prices.price as pool_price, 
        prices.cex_price as open_price
    from {{ ref('fct_polygon_sim_liquidity') }} as reserves
    left join {{ ref('int_polygon_sim_swaps') }} as swaps
        on
            reserves.block_number = swaps.block_number
            and reserves.pool_id = swaps.pool_id
    left join fees_{{ loop.index }}
        on
            reserves.block_number = fees_{{ loop.index }}.block_number
            and reserves.pool_id = fees_{{ loop.index }}.pool_id
    left join fct_polygon_sim_pool_prices as prices
        on
            reserves.block_number = prices.block_number
            and reserves.pool_id = prices.pool_id
    where prices.cex_price is not null and swaps.price_target is not null
    and (reserves.block_number <= 19400000 or reserves.block_number > 19552226)
),

lvr_calculation_{{ loop.index }} as (
    select
        block_number,
        pool_id,
        id,
        pool_price,
        price_target,
        open_price,
        reserve0,
        reserve1,
        fee_tier,
        -- calculate liquidity (simplified approximation)
        sqrt(reserve_0_usd * reserve_1_usd) as liquidity,
        -- calculate executed quantity (similar to code 1)
        sqrt(reserve_0_usd * reserve_1_usd) * abs((price_target - pool_price)/pool_price) as executed_qty,
        -- calculate average priceda
        sqrt(pool_price * price_target) as average_price,
        -- determine if lvr can occur
        case
            when (pool_price < price_target) and (price_target < open_price) and (price_target / pool_price > 1 + fee_tier) then true
            when (pool_price > price_target) and (price_target > open_price) and (pool_price / price_target > 1 + fee_tier) then true
            else false
        end as can_have_lvr
    from
        pool_reserves_{{ loop.index }}
    where (block_number <= 19400000 or block_number > 19552226)
),

lvr_results_{{ loop.index }} as (
    select
        block_number,
        pool_id,
        id,
        pool_price,
        price_target,
        open_price,
        liquidity,
        executed_qty,
        fee_tier,
        -- calculate lvr value
        if(can_have_lvr, executed_qty * abs((open_price - average_price)/average_price), 0) as lvr_value,
        -- calculate fee
        if(can_have_lvr, fee_tier * executed_qty, 0) as fee,
        can_have_lvr
    from
        lvr_calculation_{{ loop.index }}
    where (block_number <= 19400000 or block_number > 19552226)
){% if not loop.last %},{% endif %}

{% endfor %}

select * from (
    {% for base_fee in base_fees %}
    select
        pools.pool_name,
        lvr.*
    from lvr_results_{{ loop.index }} lvr
    left join {{ ref('dim_pools') }} pools
        on
            pools.pool_id = lvr.pool_id
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
) combined_results
order by block_number, pool_id, fee_tier