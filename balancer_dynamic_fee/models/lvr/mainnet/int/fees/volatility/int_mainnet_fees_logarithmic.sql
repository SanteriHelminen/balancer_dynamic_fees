{{ 
    config(
        materialized = 'table',
        tags = ['mainnet', 'mainnet_lvr', 'mainnet_fees']
    ) 
}}

{% set multiplier = [0.005, 0.1, 0.2, 1, 5, 10, 100, 500, 1000, 5000, 10000] %}
{% set base_fee = 0.003 %}

with fees as (
    {% for multiplier in multiplier %}
        select
            swaps.block_number,
            swaps.pool_id,
            'Log Price Volatility' as fee_type,
            '{{ multiplier }}' as multiplier,
            least(greatest({{ base_fee }}, {{ base_fee }} + ({{ multiplier }} * log(volatility.volatility + 1))), 0.01) as fee_tier
        from {{ ref('int_mainnet_sim_swaps') }} as swaps
        asof join {{ ref('int_mainnet_volatility') }} as volatility
            on
                swaps.block_number >= volatility.block_number + 1
                and swaps.pool_id = volatility.pool_id
        {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
)

select * from fees