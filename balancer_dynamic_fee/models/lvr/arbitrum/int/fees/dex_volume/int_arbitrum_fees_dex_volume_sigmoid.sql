{{ 
    config(
        materialized = 'table',
        tags = ['arbitrum', 'arbitrum_lvr', 'arbitrum_fees']
    ) 
}}

{% set multipliers = [10, 25, 50, 75, 100] %}
{% set min_fee = 0.001 %}
{% set max_fee = 0.01 %}

with dex_data as (
    select
        block_number,
        pool_id,
        volume
    from {{ ref('int_arbitrum_dex_volume') }}
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