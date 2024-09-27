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
        gas_fee,
        avg(gas_fee) over (
            order by block_number
            rows between 1699 preceding and current row
        ) as rolling_mean_gas
    from fct_polygon_gas
),

fees as (
    {% for multiplier in multipliers %}
        select
            swaps.block_number,
            swaps.pool_id,
            'Mean Gas' as fee_type,
            '{{ multiplier }}' as multiplier,
            least(ABS((gas_fees.gas_fee - gas_fees.rolling_mean_gas) * {{ multiplier }} + {{ base_fee }}), 0.01) as fee_tier
        from {{ ref('int_polygon_sim_swaps') }} as swaps
        left join gas_fees
            on swaps.block_number = gas_fees.block_number
        {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
)

select * from fees