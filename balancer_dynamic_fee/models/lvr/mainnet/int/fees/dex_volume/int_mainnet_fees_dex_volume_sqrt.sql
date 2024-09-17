{{ 
    config(
        materialized = 'table',
        tags = ['mainnet', 'mainnet_lvr', 'mainnet_fees']
    ) 
}}

{% set multipliers = [5, 10, 25, 50, 100, 200] %}
{% set base_fee = 0.001 %}
{% set max_fee = 0.01 %}
{% set volume_scale_factor = 1000000 %}

with dex_data as (
    select
        block_number,
        pool_id,
        volume
    from {{ ref('int_mainnet_dex_volume') }}
),

sqrt_volume as (
    select
        block_number,
        pool_id,
        sqrt(greatest(volume, 0) / {{ volume_scale_factor }}) as sqrt_scaled_volume
    from dex_data
),

fees as (
    {% for multiplier in multipliers %}
        select
            sv.block_number,
            sv.pool_id,
            'Sqrt Price DEX Volume' as fee_type,
            '{{ multiplier }}' as multiplier,
            least(
                {{ base_fee }} + ({{ multiplier }} * coalesce(sv.sqrt_scaled_volume, 0)),
                {{ max_fee }}
            ) as fee_tier
        from sqrt_volume sv
        {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
)

select * from fees