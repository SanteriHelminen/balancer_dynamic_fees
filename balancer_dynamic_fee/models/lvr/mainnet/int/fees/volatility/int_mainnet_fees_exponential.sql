{{ 
    config(
        materialized = 'table',
        tags = ['mainnet', 'mainnet_lvr', 'mainnet_fees']
    ) 
}}

{% set multiplier = [0.2, 0.5, 1, 5, 25, 100, 250, 500, 1000] %}
{% set base_fee = 0.001 %}

with fees as (
    {% for multiplier in multiplier %}
        select
            swaps.block_number,
            swaps.pool_id,
            'Exp Price Volatility' as fee_type,
            '{{ multiplier }}' as multiplier,
            least({{ base_fee }} + ({{ multiplier }} * (exp(volatility.volatility) - 1)), 0.01) as fee_tier
        from {{ ref('int_mainnet_sim_swaps') }} as swaps
        asof join {{ ref('int_mainnet_volatility') }} as volatility
            on
                swaps.block_number >= volatility.block_number + 1
                and swaps.pool_id = volatility.pool_id
        {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
)

select * from fees