{{ 
    config(
        materialized = 'table',
        tags = ['polygon']
    ) 
}}

{% set multipliers = [0.0000001, 0.0000002, 0.0000005, 0.000001, 0.000002, 0.000005] %}
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
            'Exponential Gas' as fee_type,
            '{{ multiplier }}' as multiplier,
            coalesce(
                least({{ base_fee }} + ({{ multiplier }} * exp(0.001 * gas_fees.gas_fee)), 0.01),
                {{ base_fee }}
            ) as fee_tier
        from {{ ref('int_polygon_sim_swaps') }} as swaps
        left join gas_fees
            on swaps.block_number = gas_fees.block_number
        {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
)

select * from fees
