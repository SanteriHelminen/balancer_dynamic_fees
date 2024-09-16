{{ 
    config(
        materialized = 'table'
        tags = ['mainnet', 'mainnet_lvr']
    ) 
}}

{% set blocks_per_hour = 300 %}

{% set pool_ids = [
    'a6f548df93de924d73be7d25dc02554c6bd66db500020000000000000000000e',
    '3ff3a210e57cfe679d9ad1e9ba6453a716c56a2e0002000000000000000005d5',
    'f4c0dd9b82da36c07605df83c8a416f11724d88b000200000000000000000026',
    'cf7b51ce5755513d4be016b0e28d6edeffa1d52a000200000000000000000617',
    '5c6ee304399dbdb9c8ef030ab642b10820db8f56000200000000000000000014'
] %}

with block_groups as (
    select
        *,
        floor(block_number / {{ blocks_per_hour }}) * {{ blocks_per_hour }} as block_group
    from {{ ref('fct_mainnet_sim_swaps') }}
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
    from {{ ref('fct_mainnet_sim_liquidity') }}
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