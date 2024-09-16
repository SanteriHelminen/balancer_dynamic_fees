{{ 
    config(
        materialized = 'table',
        tags = ['arbitrum', 'arbitrum_lvr']
    ) 
}}

{% set pool_ids = [
    '3a4c6d2404b5eb14915041e01f63200a82f4a343000200000000000000000065',
    '32df62dc3aed20d62241930520e665dc181658410002000000000000000003bd'
] %}

{% set blocks_per_hour = 13846 %}
with block_groups as (
    select
        *,
        floor(block_number / {{ blocks_per_hour }}) as block_group
    from {{ ref('fct_arbitrum_sim_pool_prices') }}
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
    start_block as block_number,
    start_timestamp as timestamp,
    pool_id,
    avg_price as raw_price,
    avg_market_price as price,
    volatility,
    end_block,
    end_timestamp
from volatility_calc
order by start_block, pool_id
