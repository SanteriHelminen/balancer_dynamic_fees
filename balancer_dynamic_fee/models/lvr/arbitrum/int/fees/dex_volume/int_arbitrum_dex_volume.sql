{{ 
    config(
        materialized = 'table',
        tags = ['arbitrum', 'arbitrum_lvr', 'arbitrum_fees']
    ) 
}}

{% set blocks_per_hour = 13846 %}

{% set pool_ids = [
    '3a4c6d2404b5eb14915041e01f63200a82f4a343000200000000000000000065',
    '32df62dc3aed20d62241930520e665dc181658410002000000000000000003bd'
] %}

with block_groups as (
    select
        *,
        floor(block_number / {{ blocks_per_hour }}) * {{ blocks_per_hour }} as block_group
    from {{ ref('fct_arbitrum_sim_swaps') }}
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
    from {{ ref('fct_arbitrum_sim_liquidity') }}
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
),

final as (
    select
        pool_id,
        block_number,
        block_group,
        timestamp,
        (volume_in + volume_out) / 2 as volume
    from swap_volumes
    order by pool_id, block_number
)

select * from final