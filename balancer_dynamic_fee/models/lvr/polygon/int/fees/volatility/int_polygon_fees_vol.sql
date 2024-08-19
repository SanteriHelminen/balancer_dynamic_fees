{{ 
    config(
        materialized = 'table'
    ) 
}}

{% set multiplier = [0.1, 0.5, 1, 5, 10] %}
{% set base_fee = 0.003 %}

with fees as (
    {% for multiplier in multiplier %}
        select
            swaps.block_number,
            swaps.pool_id,
            'Price Volatility' as fee_type,
            '{{ multiplier }}' as multiplier,
            volatility.volatility as volatility,
            least(coalesce({{ base_fee }} + ({{ multiplier }} * volatility.volatility), {{base_fee}}), 0.01) as fee_tier
        from {{ ref('int_polygon_sim_swaps') }} as swaps
        left join {{ ref('int_polygon_volatility') }} as volatility
            on
                swaps.block_number = volatility.block_number
                and swaps.pool_id = volatility.pool_id
        {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
)

select * from fees