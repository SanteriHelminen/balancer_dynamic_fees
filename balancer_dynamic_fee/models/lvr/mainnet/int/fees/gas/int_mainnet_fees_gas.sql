{{ 
    config(
        materialized = 'table',
        tags = ['mainnet', 'mainnet_lvr', 'mainnet_fees']
    ) 
}}

{% set multipliers = [0.00001, 0.00002, 0.00005, 0.0001, 0.0002, 0.0005] %}
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
