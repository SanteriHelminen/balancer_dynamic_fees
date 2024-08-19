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
cex_data_{{ loop.index }} as (
    select
        formatted_timestamp,
        price
    from {{ ref('fct_cex_data') }}
    where pool_id = '{{ pool_id }}'
    order by formatted_timestamp
),
final_{{ loop.index }} as (
    select
        swaps.pool_id,
        swaps.block_timestamp,
        swaps.block_number,
        swaps.token_in,
        swaps.token_out,
        swaps.fee_tier,
        swaps.token_in_1,
        swaps.token_out_1,
        cex_data_{{ loop.index }}.price as cex_price,
        pool_prices.price as pool_price,
        pool_prices.eth_price as eth_price,
        case 
            when cex_data_{{ loop.index }}.price > pool_prices.price 
                then cex_data_{{ loop.index }}.price * (1 - swaps.fee_tier)
            else cex_data_{{ loop.index }}.price * (1 + swaps.fee_tier)
        end as price_target
    from {{ ref('fct_mainnet_sim_swaps') }} swaps
    join {{ ref('fct_mainnet_sim_pool_prices') }} pool_prices
        on swaps.pool_id = pool_prices.pool_id
        and swaps.pool_id = '{{ pool_id }}'
        and swaps.block_timestamp = pool_prices.timestamp
    asof join cex_data_{{ loop.index }}
        on swaps.block_timestamp >= cex_data_{{ loop.index }}.formatted_timestamp
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
