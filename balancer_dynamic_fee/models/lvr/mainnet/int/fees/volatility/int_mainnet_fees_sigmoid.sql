{{ 
    config(
        materialized = 'table',
        tags = ['mainnet', 'mainnet_lvr', 'mainnet_volatility']
    ) 
}}

{% set multipliers = [0.005, 0.1, 0.2, 1, 5, 10, 100, 500, 1000] %}
{% set min_fee = 0.003 %}
{% set max_fee = 0.01 %}

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
            {{ min_fee }} + ({{ max_fee }} - {{ min_fee }}) * 
            (1 - exp(-{{ multiplier }} * coalesce(volatility.volatility, 0))) / 
            (1 + exp(-{{ multiplier }} * coalesce(volatility.volatility, 0))) as fee_tier
        from {{ ref('int_mainnet_sim_swaps') }} as swaps
        left join volatility_stats as volatility
            on swaps.block_number = volatility.block_number
            and swaps.pool_id = volatility.pool_id
        {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
)

select * from fees
