{{ 
    config(
        materialized = 'table',
        tags = ['polygon', 'polygon_lvr', 'polygon_fees']
    ) 
}}

{% set multipliers = [0.00001, 0.00002, 0.00005, 0.0001, 0.0002, 0.0005] %}
{% set base_fee = 0.001 %}

with gas_fees as (
    select
        block_number,
        gas_fee
    from {{ ref('fct_polygon_gas') }}
),

fees as (
    {% for multiplier in multipliers %}
        select
            swaps.block_number,
            swaps.pool_id,
            'Log Gas' as fee_type,
            '{{ multiplier }}' as multiplier,
            least(coalesce(
                {{ base_fee }} + ({{ multiplier }} * ln(gas_fees.gas_fee + 1)),
                {{ base_fee }}
            ), 0.01) as fee_tier
        from {{ ref('int_polygon_sim_swaps') }} as swaps
        left join gas_fees
            on swaps.block_number = gas_fees.block_number
        {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
)

select * from fees
