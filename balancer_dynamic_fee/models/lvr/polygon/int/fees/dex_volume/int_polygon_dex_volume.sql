{{ 
    config(
        materialized = 'table',
        tags = ['polygon']
    ) 
}}

{% set blocks_per_hour = 1700 %}

{% set pool_ids = [
    'ae8f935830f6b418804836eacb0243447b6d977c000200000000000000000ad1'
] %}

with block_groups as (
    select
        *,
        floor(block_number / {{ blocks_per_hour }}) * {{ blocks_per_hour }} as block_group
    from {{ ref('fct_polygon_sim_swaps') }}
    where pool_id in (
        {% for pool_id in pool_ids %}
            '{{ pool_id }}'{% if not loop.last %},{% endif %}
        {% endfor %}
    )
),

liquidity_data as (
    select
        *,
        floor(block_number / {{ blocks_per_hour }}) * {{ blocks_per_hour }} as block_group
    from {{ ref('fct_polygon_sim_liquidity') }}
    where pool_id in (
        {% for pool_id in pool_ids %}
            '{{ pool_id }}'{% if not loop.last %},{% endif %}
        {% endfor %}
    )
),

swap_volumes as (
    select
        bg.pool_id,
        bg.block_group,
        ld.block_number,
        ld.timestamp,
        sum(case 
            when bg.token_in = ld.token0_address then bg.token_in_1 / nullif(ld.reserve_0, 0)
            else bg.token_in_1 / nullif(ld.reserve_1, 0)
        end) as volume_in,
        sum(case 
            when bg.token_out = ld.token0_address then bg.token_out_1 / nullif(ld.reserve_0, 0)
            else bg.token_out_1 / nullif(ld.reserve_1, 0)
        end) as volume_out
    from block_groups bg
    join liquidity_data ld on bg.pool_id = ld.pool_id and bg.block_group = ld.block_group
    group by bg.pool_id, bg.block_group, ld.block_number, ld.timestamp
)

select
    pool_id,
    block_number,
    block_group,
    timestamp,
    (volume_in + volume_out) / 2 as volume
from swap_volumes
order by pool_id, block_number