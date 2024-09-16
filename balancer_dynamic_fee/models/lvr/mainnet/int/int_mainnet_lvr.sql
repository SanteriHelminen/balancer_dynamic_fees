{{ 
    config(
        materialized = 'table',
        tags = ['mainnet', 'mainnet_lvr']
    ) 
}}

with fees as (
    select
        block_number,
        pool_id,
        fee_tier
    from {{ ref('int_mainnet_sim_swaps') }}
),

pool_reserves as (
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
        fees.fee_tier,
        swaps.price_target,
        prices.price as pool_price, 
        prices.cex_price as open_price
    from {{ ref('fct_mainnet_sim_liquidity') }} as reserves
    left join {{ ref('int_mainnet_sim_swaps') }} as swaps
        on
            reserves.block_number = swaps.block_number
            and reserves.pool_id = swaps.pool_id
    left join fees
        on
            reserves.block_number = fees.block_number
            and reserves.pool_id = fees.pool_id
    left join fct_mainnet_sim_pool_prices as prices
        on
            reserves.block_number = prices.block_number
            and reserves.pool_id = prices.pool_id
    where prices.cex_price is not null and swaps.price_target is not null
    and (reserves.block_number <= 19400000 or reserves.block_number > 19552226)
),

lvr_calculation as (
    select
        block_number,
        pool_id,
        pool_price,
        price_target,
        open_price,
        reserve_0,
        reserve_1,
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
    from
        pool_reserves
    where (block_number <= 19400000 or block_number > 19552226)
),

price_diff_percentiles as (
    select
        pool_id,
        percentile_cont(0.90) within group (order by price_diff) as ninetyfifth_percentile
    from lvr_calculation
    group by pool_id
),

lvr_results AS (
    SELECT
        l.block_number,
        l.pool_id,
        l.fee_tier,
        l.pool_price,
        l.price_target,
        l.open_price,
        l.liquidity,
        l.executed_qty,
        l.reserve_0_usd,
        l.reserve_1_usd,
        if(l.can_have_lvr, l.executed_qty * ABS((l.open_price - l.average_price)/l.average_price), 0) AS lvr_value,
        if(l.can_have_lvr, l.fee_tier * l.executed_qty, 0) AS fee,
        l.can_have_lvr,
        l.price_diff
    FROM
        lvr_calculation l
    JOIN price_diff_percentiles p
        ON l.pool_id = p.pool_id
    WHERE
        l.price_diff <= p.ninetyfifth_percentile
        AND (l.block_number <= 19400000 or l.block_number > 19552226)
)

SELECT distinct
    pools.pool_name,
    lr.*
FROM lvr_results lr
LEFT JOIN {{ ref('dim_pools') }} pools
    ON pools.pool_id = lr.pool_id
ORDER BY lr.pool_id, lr.block_number