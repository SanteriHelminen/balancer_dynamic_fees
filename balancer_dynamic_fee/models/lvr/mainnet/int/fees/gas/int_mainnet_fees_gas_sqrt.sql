{{ 
    config(
        materialized = 'table',
        tags = ['mainnet', 'mainnet_lvr', 'mainnet_fees']
    ) 
}}

{% set multipliers = [0.001, 0.002, 0.003, 0.004, 0.005] %}
{% set base_fee = 0.001 %}

with gas_fees as (
    select
        block_number,
        gas_fee
    from {{ ref('fct_mainnet_gas') }}
),

fees as (
    {% for multiplier in multipliers %}
        select
            swaps.block_number,
            swaps.pool_id,
            'Square root Gas' as fee_type,
            '{{ multiplier }}' as multiplier,
            least(coalesce(
                {{ base_fee }} + ({{ multiplier }} * sqrt(gas_fees.gas_fee)),
                {{ base_fee }}
            ), 0.01) as fee_tier
        from {{ ref('int_mainnet_sim_swaps') }} as swaps
        left join gas_fees
            on swaps.block_number = gas_fees.block_number
        {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
)

select * from fees