{{ 
    config(
        materialized = 'table'
    ) 
}}

{% set multipliers = [50, 100, 250, 500, 750, 1000] %}
{% set min_fee = 0.001 %}
{% set max_fee = 0.01 %}

with dex_data as (
    select
        block_number,
        pool_id,
        volume  -- This is already normalized by reserves
    from {{ ref('int_mainnet_dex_volume') }}
),

sigmoid_fees as (
    {% for multiplier in multipliers %}
        select
            dd.block_number,
            dd.pool_id,
            'Sigmoid DEX Volume' as fee_type,
            '{{ multiplier }}' as multiplier,
            {{ min_fee }} + ({{ max_fee }} - {{ min_fee }}) * 
            (1 - exp(-{{ multiplier }} * coalesce(dd.volume, 0))) / 
            (1 + exp(-{{ multiplier }} * coalesce(dd.volume, 0))) as fee_tier
        from dex_data dd
        {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
)

select * from sigmoid_fees