{{ 
    config(
        materialized = 'table',
        tags = ['mainnet', 'mainnet_gas']
    ) 
}}

{% set multipliers = [0.0000001, 0.0000002, 0.0000005, 0.000001, 0.000002, 0.000005] %}
{% set base_fee = 0.001 %}

with fees as (
    {% for multiplier in multipliers %}
        select
            swaps.block_number,
            swaps.pool_id,
            'Gas' as fee_type,
            '{{ multiplier }}' as multiplier,
            least({{ base_fee }} + (gas_fees.gas_fee * {{ multiplier }}), 0.01) as fee_tier
        from {{ ref('int_mainnet_sim_swaps') }} as swaps
        left join {{ ref('fct_mainnet_gas') }} as gas_fees
            on swaps.block_number = gas_fees.block_number
        {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
)

select * from fees
