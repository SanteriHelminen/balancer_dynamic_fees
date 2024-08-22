{{ 
    config(
        materialized = 'table',
        tags = ['mainnet', 'mainnet_gas']
    ) 
}}

{% set multipliers = [0.0000001, 0.0000002, 0.0000005, 0.000001, 0.000002, 0.000005] %}
{% set base_fee = 0.001 %}

with gas_fees as (
    select
        block_number,
        gas_fee,
        avg(gas_fee) over (
            order by block_number
            rows between 299 preceding and current row
        ) as rolling_mean_gas
    from fct_mainnet_gas
),

fees as (
    {% for multiplier in multipliers %}
        select
            swaps.block_number,
            swaps.pool_id,
            'Mean Gas' as fee_type,
            '{{ multiplier }}' as multiplier,
            least(ABS((gas_fees.gas_fee - gas_fees.rolling_mean_gas) * {{ multiplier }} + {{ base_fee }}), 0.01) as fee_tier
        from {{ ref('int_mainnet_sim_swaps') }} as swaps
        left join gas_fees
            on swaps.block_number = gas_fees.block_number
        {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
)

select * from fees