{{ 
    config(
        materialized = 'table',
        tags = ['arbitrum', 'arbitrum_lvr', 'arbitrum_fees']
    ) 
}}

{% set multiplier = [0.01, 0.02, 0.03, 0.05, 0.1] %}
{% set base_fee = 0.001 %}

with fees as (
    {% for multiplier in multiplier %}
        select
            swaps.block_number,
            swaps.pool_id,
            'Exp Price Volatility' as fee_type,
            '{{ multiplier }}' as multiplier,
            least({{ base_fee }} + ({{ multiplier }} * (exp(volatility.volatility) - 1)), 0.04) as fee_tier
        from {{ ref('int_arbitrum_sim_swaps') }} as swaps
        left join {{ ref('int_arbitrum_volatility') }} as volatility
            on
                swaps.block_number = volatility.block_number
                and swaps.pool_id = volatility.pool_id
        {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
)

select * from fees