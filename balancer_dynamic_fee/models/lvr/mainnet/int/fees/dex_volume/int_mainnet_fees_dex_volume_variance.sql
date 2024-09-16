{{ 
    config(
        materialized = 'table',
        tags = ['mainnet', 'mainnet_lvr', 'mainnet_volume']
    ) 
}}

{% set multipliers = [0.01, 0.05, 0.1, 0.2, 0.5, 1, 2.5, 5] %}
{% set base_fee = 0.001 %}
{% set lookback_period = 300 %}

with dex_data as (
    select
        block_number,
        pool_id,
        volume
    from {{ ref('int_mainnet_dex_volume') }}
),

volume_stats as (
    select
        block_number,
        pool_id,
        volume,
        avg(volume) over (
            partition by pool_id
            order by block_number
            rows between {{ lookback_period }} preceding and current row
        ) as avg_volume,
        variance(volume) over (
            partition by pool_id
            order by block_number
            rows between {{ lookback_period }} preceding and current row
        ) as volume_variance
    from dex_data
),

normalized_variance as (
    select
        block_number,
        pool_id,
        volume,
        case 
            when avg_volume > 0 then volume_variance / avg_volume
            else 0
        end as normalized_variance
    from volume_stats
),

fees as (
    {% for multiplier in multipliers %}
        select
            nv.block_number,
            nv.pool_id,
            'DEX Volume Variance' as fee_type,
            '{{ multiplier }}' as multiplier,
            least(
                {{ base_fee }} + ({{ multiplier }} * coalesce(nv.normalized_variance, 0)),
                0.01
            ) as fee_tier
        from normalized_variance nv
        {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
)

select * from fees