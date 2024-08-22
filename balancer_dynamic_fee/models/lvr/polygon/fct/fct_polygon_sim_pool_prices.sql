{{ 
    config(
        materialized = 'table',
        tags = ['polygon']
    ) 
}}

with
prices as (
    select 
        *
    from fct_cex_data
    where pool_id = 'ae8f935830f6b418804836eacb0243447b6d977c000200000000000000000ad1'
),

final as (
    select
        reserves.block_number,
        reserves.timestamp,
        reserves.pool_id,
        reserves.token0_address as token0_address,
        reserves.token1_address as token1_address,
        reserves.weight_0,
        reserves.weight_1,
        reserves.reserve_0,
        reserves.reserve_1,
        divide(reserves.reserve_1, reserves.reserve_0) as raw_price,
        eth_price.price as eth_price,
        divide(reserves.reserve_1, reserves.reserve_0) as price,
        prices.price as cex_price
    from {{ ref('fct_polygon_sim_liquidity') }} as reserves
    asof join fct_eth_price as eth_price
        on reserves.timestamp >= eth_price.formatted_timestamp
    asof join prices
        on reserves.timestamp >= prices.formatted_timestamp
    where reserves.pool_id = 'ae8f935830f6b418804836eacb0243447b6d977c000200000000000000000ad1'
    and reserves.timestamp <= (select max(formatted_timestamp) from prices)
    and reserves.block_number >= 46783038
)

select * from final
