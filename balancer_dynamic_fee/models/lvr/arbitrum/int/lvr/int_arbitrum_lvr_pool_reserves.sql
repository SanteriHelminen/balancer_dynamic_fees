{{ 
    config(
        materialized = 'table',
        tags = ['arbitrum', 'arbitrum_lvr']
    ) 
}}

with fees as (
    select
        block_number,
        pool_id,
        fee_tier
    from {{ ref('int_arbitrum_sim_swaps') }}
),

pool_reserves as (
    select
        reserves.block_number,
        reserves.pool_id,
        reserves.reserve_0 * prices.price as reserve_0_usd,
        case
            when reserves.token1_address = '0x82af49447d8a07e3bd95bd0d56f35241523fbab1'
                then multiply(reserves.reserve_1, prices.eth_price)
            else reserves.reserve_1
        end as reserve_1_usd,
        fees.fee_tier,
        swaps.price_target,
        prices.price as pool_price, 
        prices.cex_price as open_price
    from {{ ref('fct_arbitrum_sim_liquidity') }} as reserves
    left join {{ ref('int_arbitrum_sim_swaps') }} as swaps
        on
            reserves.block_number = swaps.block_number
            and reserves.pool_id = swaps.pool_id
    left join fees
        on
            reserves.block_number = fees.block_number
            and reserves.pool_id = fees.pool_id
    left join fct_arbitrum_sim_pool_prices as prices
        on
            reserves.block_number = prices.block_number
            and reserves.pool_id = prices.pool_id
    where prices.cex_price is not null and swaps.price_target is not null
    and (reserves.block_number <= 188600485 or reserves.block_number > 196038995)
)

select * from pool_reserves