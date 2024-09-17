{{ 
    config(
        materialized = 'table',
        tags = ['arbitrum', 'arbitrum_lvr', 'arbitrum_fees']
    ) 
}}

{% set multipliers = [0.01, 0.02, 0.03, 0.05] %}
{% set base_fee = 0.001 %}
{% set lookback_period = 1700 %}

with dex_data as (
    select
        block_number,
        pool_id,
        volume
    from {{ ref('int_arbitrum_dex_volume') }}
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