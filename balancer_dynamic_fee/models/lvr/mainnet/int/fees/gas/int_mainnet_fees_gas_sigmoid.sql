{{ 
    config(
        materialized = 'table',
        tags = ['mainnet', 'mainnet_lvr', 'mainnet_fees']
    ) 
}}

{% set multipliers = [0.1, 0.2, 0.5, 1] %}
{% set base_fee = 0.001 %}

with gas_fees as (
    select
        block_number,
        gas_fee
    from {{ ref('fct_mainnet_gas') }}
),

median_gas as (
    select
        percentile_cont(0.5) within group (order by gas_fee) as median_gas_fee
    from gas_fees
),

fees as (
    {% for multiplier in multipliers %}
        select
            swaps.block_number,
            swaps.pool_id,
            'Sigmoid Gas' as fee_type,
            '{{ multiplier }}' as multiplier,
            least(coalesce(
                {{ base_fee }} + exp(1 - gas_fees.gas_fee / median_gas.median_gas_fee) * {{ multiplier }},
                {{ base_fee }}
            ), 0.01) as fee_tier
        from {{ ref('int_mainnet_sim_swaps') }} as swaps
        left join gas_fees
            on swaps.block_number = gas_fees.block_number
        cross join median_gas
        {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
)

select * from fees