{{ 
    config(
        materialized = 'table'
    ) 
}}

{% set multipliers = [0.001, 0.002, 0.005, 0.01] %}
{% set min_fee = 0.001 %}
{% set max_fee = 0.006 %}

with volatility_stats as (
    select
        block_number,
        pool_id,
        volatility,
        avg(volatility) over (
            partition by pool_id
            order by block_number
            rows between 299 preceding and current row
        ) as rolling_mean_volatility
    from {{ ref('int_mainnet_volatility') }}
),

fees as (
    {% for multiplier in multipliers %}
        select
            swaps.block_number,
            swaps.pool_id,
            'Sigmoid Price Volatility' as fee_type,
            '{{ multiplier }}' as multiplier,
            volatility.volatility as volatility,
            {{ multiplier }} as multiplier,
            least(coalesce(
                {{ base_fee }} + exp(1 - volatility.volatility / volatility.rolling_mean_volatility) * {{ multiplier }},
                {{ base_fee }}
            ), 0.007) as fee_tier
        from {{ ref('int_mainnet_sim_swaps') }} as swaps
        left join volatility_stats as volatility
            on swaps.block_number = volatility.block_number
            and swaps.pool_id = volatility.pool_id
        {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
)

select * from fees
