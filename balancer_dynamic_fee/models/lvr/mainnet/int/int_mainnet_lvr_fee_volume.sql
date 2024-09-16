{{ 
    config(
        materialized = 'table',
        tags = ['mainnet', 'mainnet_lvr', 'mainnet_volume']
    ) 
}}

{% set fee_sources = [
    'int_mainnet_fees_dex_mean_volume',
    'int_mainnet_fees_dex_volume_logarithmic',
    'int_mainnet_fees_dex_volume_sigmoid',
    'int_mainnet_fees_dex_volume_sqrt',
    'int_mainnet_fees_dex_volume_variance',
    'int_mainnet_fees_dex_volume'
] %}

with 
{% for fee_source in fee_sources %}
fees_{{ loop.index }} as (
    select
        block_number,
        pool_id,
        fee_type,
        multiplier,
        fee_tier
    from {{ fee_source }}
),

pool_reserves_{{ loop.index }} as (
    select
        reserves.block_number,
        reserves.pool_id,
        reserves.weight_0,
        reserves.weight_1,
        reserves.reserve_0,
        reserves.reserve_0 * prices.price as reserve_0_usd,
        reserves.reserve_1,
        case
            when reserves.token1_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                then multiply(reserves.reserve_1, prices.eth_price)
            else reserves.reserve_1
        end as reserve_1_usd,
        fees_{{ loop.index }}.fee_tier,
        fees_{{ loop.index }}.fee_type,
        fees_{{ loop.index }}.multiplier,
        swaps.price_target,
        prices.price as pool_price, 
        prices.cex_price as open_price
    from {{ ref('fct_mainnet_sim_liquidity') }} as reserves
    left join {{ ref('int_mainnet_sim_swaps') }} as swaps
        on
            reserves.block_number = swaps.block_number
            and reserves.pool_id = swaps.pool_id
    left join fees_{{ loop.index }}
        on
            reserves.block_number = fees_{{ loop.index }}.block_number
            and reserves.pool_id = fees_{{ loop.index }}.pool_id
    left join {{ ref('fct_mainnet_sim_pool_prices') }} as prices
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
        pool_price,
        price_target,
        open_price,
        reserve_0,
        reserve_1,
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
    from
        pool_reserves_{{ loop.index }}
    where (block_number <= 19400000 or block_number > 19552226)
){% if not loop.last %},{% endif %}
{% endfor %},

price_difference_percentiles as (
    select
        pool_id,
        multiplier,
        fee_type,
        percentile_cont(0.9) within group (order by price_difference) as ninetyfifth_percentile
    from (
        {% for i in range(1, fee_sources | length + 1) %}
        select pool_id, price_difference from lvr_calculation_{{ i }}
        {% if not loop.last %}union all{% endif %}
        {% endfor %}
    ) all_price_differences
    group by pool_id
),

{% for i in range(1, fee_sources | length + 1) %}
lvr_results_{{ i }} as (
    select
        l.block_number,
        l.pool_id,
        l.pool_price,
        l.price_target,
        l.open_price,
        l.liquidity,
        l.executed_qty,
        l.fee_tier,
        l.fee_type,
        l.multiplier,
        if(can_have_lvr, executed_qty * ABS((open_price - average_price)/average_price), 0) AS lvr_value,
        if(can_have_lvr, fee_tier * executed_qty, 0) AS fee,
        l.can_have_lvr
    from
        lvr_calculation_{{ i }} l
    join price_difference_percentiles p on l.pool_id = p.pool_id
    where (l.block_number <= 19400000 or l.block_number > 19552226)
        and l.price_difference <= p.ninetyfifth_percentile
){% if not loop.last %},{% endif %}
{% endfor %},

final as (
    select distinct * from (
        {% for i in range(1, fee_sources | length + 1) %}
        select
            pools.pool_name,
            lvr.* 
        from lvr_results_{{ i }} lvr
        left join {{ ref('dim_pools') }} pools on pools.pool_id = lvr.pool_id
        {% if not loop.last %}union all{% endif %}
        {% endfor %}
    ) combined_results
    order by block_number, pool_id, fee_type, multiplier
)

select * from final