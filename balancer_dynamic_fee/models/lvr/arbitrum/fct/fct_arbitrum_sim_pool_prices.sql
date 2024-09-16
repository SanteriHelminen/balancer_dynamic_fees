{{ 
    config(
        materialized = 'table',
        tags = ['arbitrum', 'arbitrum_lvr']
    ) 
}}

{% set pool_ids = [
    '3a4c6d2404b5eb14915041e01f63200a82f4a343000200000000000000000065',
    '32df62dc3aed20d62241930520e665dc181658410002000000000000000003bd'
] %}

with
{% for pool_id in pool_ids %}
prices_{{ loop.index }} as (
    select 
        *
    from {{ref('fct_cex_data')}}
    where pool_id = '{{ pool_id }}'
),

final_{{ loop.index }} as (
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
        case
            when reserves.token1_address = '0x82af49447d8a07e3bd95bd0d56f35241523fbab1'
                then divide(multiply(reserves.reserve_1, eth_price.price), reserves.reserve_0)
            else divide(reserves.reserve_1, reserves.reserve_0)
        end as price,
        prices_{{ loop.index }}.price as cex_price
    from {{ ref('fct_arbitrum_sim_liquidity') }} as reserves
    asof join fct_eth_price as eth_price
        on reserves.timestamp >= eth_price.formatted_timestamp
    asof join prices_{{ loop.index }}
        on reserves.timestamp >= prices_{{ loop.index }}.formatted_timestamp
    where reserves.pool_id = '{{ pool_id }}'
    and reserves.timestamp <= (select max(formatted_timestamp) from prices_{{ loop.index }})
    and reserves.block_number >= 125207823
)
{% if not loop.last %},
{% endif %}
{% endfor %}

select * from (
    {% for pool_id in pool_ids %}
        select * from final_{{ loop.index }}
        {% if not loop.last %}
        union all
        {% endif %}
    {% endfor %}
) combined_final
