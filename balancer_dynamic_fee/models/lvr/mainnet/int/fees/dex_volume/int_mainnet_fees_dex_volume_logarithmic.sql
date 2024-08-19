{{ 
    config(
        materialized = 'table'
    ) 
}}

{% set multipliers = [50, 100, 250, 500, 750, 1000] %}
{% set min_fee = 0.001 %}
{% set max_fee = 0.01 %}
{% set volume_offset = 1 %}  -- To handle potential zero volumes

with dex_data as (
    select
        block_number,
        pool_id,
        volume  -- This is already normalized by reserves
    from {{ ref('int_mainnet_dex_volume') }}
),

ln_volume as (
    select
        block_number,
        pool_id,
        ln(greatest(volume + {{ volume_offset }}, 1)) as ln_volume
    from dex_data
),

sigmoid_fees as (
    {% for multiplier in multipliers %}
        select
            lv.block_number,
            lv.pool_id,
            'Log DEX Volume' as fee_type,
            '{{ multiplier }}' as multiplier,
            {{ min_fee }} + ({{ max_fee }} - {{ min_fee }}) * 
            (1 - exp(-{{ multiplier }} * lv.ln_volume)) / 
            (1 + exp(-{{ multiplier }} * lv.ln_volume)) as fee_tier
        from ln_volume lv
        {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
)

select * from sigmoid_fees