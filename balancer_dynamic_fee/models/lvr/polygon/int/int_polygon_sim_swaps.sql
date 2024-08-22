{{ 
    config(
        materialized = 'table',
        tags = ['polygon']
    ) 
}}

{% set pool_id = 'ae8f935830f6b418804836eacb0243447b6d977c000200000000000000000ad1' %}

with cex_data as (
    select
        formatted_timestamp,
        price
    from {{ ref('fct_cex_data') }}
    where pool_id = '{{ pool_id }}'
    order by formatted_timestamp
),

final as (
    select
        swaps.pool_id,
        swaps.block_timestamp,
        swaps.block_number,
        swaps.token_in,
        swaps.token_out,
        swaps.fee_tier,
        swaps.token_in_1,
        swaps.token_out_1,
        cex_data.price as cex_price,
        pool_prices.price as pool_price,
        pool_prices.eth_price as eth_price,
        case 
            when cex_data.price > pool_prices.price 
                then cex_data.price * (1 - swaps.fee_tier)
            else cex_data.price * (1 + swaps.fee_tier)
        end as price_target
    from {{ ref('fct_polygon_sim_swaps') }} swaps
    join {{ ref('fct_polygon_sim_pool_prices') }} pool_prices
        on swaps.pool_id = pool_prices.pool_id
        and swaps.pool_id = '{{ pool_id }}'
        and swaps.block_timestamp = pool_prices.timestamp
    asof join cex_data
        on swaps.block_timestamp >= cex_data.formatted_timestamp
)

select * from final
