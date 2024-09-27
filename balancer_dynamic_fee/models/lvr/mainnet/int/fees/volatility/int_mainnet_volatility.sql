{{ 
    config(
        materialized = 'table',
        tags = ['mainnet', 'mainnet_lvr', 'mainnet_fees']
    ) 
}}

{% set pool_ids = [
    'a6f548df93de924d73be7d25dc02554c6bd66db500020000000000000000000e',
    '3ff3a210e57cfe679d9ad1e9ba6453a716c56a2e0002000000000000000005d5',
    'f4c0dd9b82da36c07605df83c8a416f11724d88b000200000000000000000026',
    'cf7b51ce5755513d4be016b0e28d6edeffa1d52a000200000000000000000617',
    '5c6ee304399dbdb9c8ef030ab642b10820db8f56000200000000000000000014'
] %}

{% set blocks_per_hour = 300 %}

with block_groups as (
    select
        *,
        floor(block_number / {{ blocks_per_hour }}) as block_group
    from {{ ref('fct_mainnet_sim_pool_prices') }}
    where pool_id in (
        {% for pool_id in pool_ids %}
            '{{ pool_id }}'{% if not loop.last %},{% endif %}
        {% endfor %}
    )
),

block_data as (
    select
        block_group,
        pool_id,
        avg(raw_price) as avg_price,
        stddev(raw_price) as stddev_price,
        min(block_number) as start_block,
        max(block_number) as end_block,
        min(timestamp) as start_timestamp,
        max(timestamp) as end_timestamp,
        avg(price) as avg_market_price
    from block_groups
    group by 1, 2
),

volatility_calc as (
    select
        *,
        coalesce(stddev_price / nullif(avg_price, 0), 0) as volatility
    from block_data
)

select
    end_block as block_number,
    start_timestamp as timestamp,
    pool_id,
    avg_price as raw_price,
    avg_market_price as price,
    volatility,
    end_timestamp
from volatility_calc
order by start_block, pool_id
