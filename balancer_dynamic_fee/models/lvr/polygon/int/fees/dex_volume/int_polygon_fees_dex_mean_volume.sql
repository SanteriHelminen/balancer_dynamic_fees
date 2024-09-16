{{ 
    config(
        materialized = 'table',
        tags = ['polygon', 'polygon_lvr']
    ) 
}}

{% set multipliers = [ 5, 10, 25, 50, 100] %}
{% set min_fee = 0.005 %}
{% set max_fee = 0.01 %}
{% set blocks_per_hour = 1700 %}

with dex_data as (
    select
        block_number,
        pool_id,
        volume
    from {{ ref('int_polygon_dex_volume') }}
),

hourly_avg_volume as (
    select
        block_number,
        pool_id,
        volume,
        avg(volume) over (
            partition by pool_id
            order by block_number
            rows between {{ blocks_per_hour - 1 }} preceding and current row
        ) as avg_hourly_volume
    from dex_data
),

volume_difference as (
    select
        block_number,
        pool_id,
        volume - avg_hourly_volume as volume_diff
    from hourly_avg_volume
),

sigmoid_fees as (
    {% for multiplier in multipliers %}
        select
            vd.block_number,
            vd.pool_id,
            'Mean DEX Volume' as fee_type,
            '{{ multiplier }}' as multiplier,
            {{ min_fee }} + ({{ max_fee }} - {{ min_fee }}) * 
            (1 - exp(-{{ multiplier }} * vd.volume_diff)) / 
            (1 + exp(-{{ multiplier }} * vd.volume_diff)) as fee_tier
        from volume_difference vd
        {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
)

select * from sigmoid_fees