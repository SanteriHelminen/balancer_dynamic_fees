{{ 
    config(
        materialized = 'table',
        tags = ['arbitrum']
    ) 
}}

{% set multipliers = [0.05, 0.1, 0.2, 0.3, 0.4] %}
{% set base_fee = 0.001 %}

with dex_data as (
    select
        block_number,
        pool_id,
        volume
    from {{ ref('int_arbitrum_dex_volume') }}
),

fees as (
    {% for multiplier in multipliers %}
        select
            block_number,
            pool_id,
            'DEX Volume' as fee_type,
            '{{ multiplier }}' as multiplier,
            least({{ base_fee }} + ({{ multiplier }} * coalesce(volume, 0)), 0.01) as fee_tier
        from dex_data
        {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
)

select * from fees