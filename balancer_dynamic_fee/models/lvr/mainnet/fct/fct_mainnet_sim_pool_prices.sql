{{ 
    config(
        materialized = 'table'
    ) 
}}

{% set pool_ids = [
    'a6f548df93de924d73be7d25dc02554c6bd66db500020000000000000000000e',
    '3ff3a210e57cfe679d9ad1e9ba6453a716c56a2e0002000000000000000005d5',
    'f4c0dd9b82da36c07605df83c8a416f11724d88b000200000000000000000026',
    'cf7b51ce5755513d4be016b0e28d6edeffa1d52a000200000000000000000617',
    '5c6ee304399dbdb9c8ef030ab642b10820db8f56000200000000000000000014'
] %}

with
{% for pool_id in pool_ids %}
prices_{{ loop.index }} as (
    select 
        *
    from fct_cex_data
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
            when reserves.token1_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                then divide(multiply(reserves.reserve_1, eth_price.price), reserves.reserve_0)
            else divide(reserves.reserve_1, reserves.reserve_0)
        end as price,
        prices_{{ loop.index }}.price as cex_price
    from {{ ref('fct_mainnet_sim_liquidity') }} as reserves
    asof join fct_eth_price as eth_price
        on reserves.timestamp >= eth_price.formatted_timestamp
    asof join prices_{{ loop.index }}
        on reserves.timestamp >= prices_{{ loop.index }}.formatted_timestamp
    where reserves.pool_id = '{{ pool_id }}'
    and reserves.timestamp <= (select max(formatted_timestamp) from prices_{{ loop.index }})
    and reserves.block_number >= 18000000
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
