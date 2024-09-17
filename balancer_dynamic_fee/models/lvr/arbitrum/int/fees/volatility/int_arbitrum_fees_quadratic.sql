{{ 
    config(
        materialized = 'table',
        tags = ['arbitrum', 'arbitrum_lvr', 'arbitrum_fees']
    ) 
}}

{% set multiplier = [100, 500, 1000, 2500, 5000] %}
{% set base_fee = 0.003 %}

with fees as (
    {% for multiplier in multiplier %}
        select
            swaps.block_number,
            swaps.pool_id,
            'Quadratic Price Volatility' as fee_type,
            '{{ multiplier }}' as multiplier,
            least(greatest({{ base_fee }}, {{ base_fee }} + ({{ multiplier }} * power(volatility.volatility, 2))), 0.01) as fee_tier
        from {{ ref('int_arbitrum_sim_swaps') }} as swaps
        left join {{ ref('int_arbitrum_volatility') }} as volatility
            on
                swaps.block_number = volatility.block_number
                and swaps.pool_id = volatility.pool_id
        {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
)

select * from fees