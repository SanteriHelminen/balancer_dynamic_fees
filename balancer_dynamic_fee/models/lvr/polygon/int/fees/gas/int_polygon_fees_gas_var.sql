{{ 
    config(
        materialized = 'table',
        tags = ['polygon']
    ) 
}}

{% set multipliers = [0.00000001, 0.00000002, 0.00000005] %}
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
            'Variance Gas' as fee_type,
            '{{ multiplier }}' as multiplier,
            least(coalesce(
                {{ base_fee }} + ({{ multiplier }} * power(0.1 * gas_fees.gas_fee, 2)),
                {{ base_fee }}
            ), 0.01) as fee_tier
        from {{ ref('int_polygon_sim_swaps') }} as swaps
        left join gas_fees
            on swaps.block_number = gas_fees.block_number
        {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
)

select * from fees
