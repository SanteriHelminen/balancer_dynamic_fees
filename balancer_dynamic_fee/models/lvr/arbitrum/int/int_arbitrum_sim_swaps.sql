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
cex_data_{{ loop.index }} as (
    select
        formatted_timestamp,
        price
    from fct_cex_data
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
    from fct_arbitrum_sim_swaps swaps
    join fct_arbitrum_sim_pool_prices pool_prices
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
