{{ 
    config(
        materialized = 'table',
        tags = ['mainnet', 'mainnet_static']
    ) 
}}

{% set fee_tiers = [0.001, 0.002, 0.003, 0.004, 0.005, 0.006, 0.007, 0.008, 0.009, 0.01] %}

with fees as (
    {% for fee_tier in fee_tiers %}
        select
            swaps.block_number,
            swaps.pool_id,
            'Static' as fee_type,
            '{{ fee_tier }}' as multiplier,
            {{ fee_tier }} as fee_tier
        from {{ ref('int_mainnet_sim_swaps') }} as swaps
        {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
)

select * from fees