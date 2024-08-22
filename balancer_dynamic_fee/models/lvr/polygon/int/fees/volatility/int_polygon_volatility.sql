{{ 
    config(
        materialized = 'table',
        tags = ['polygon']
    ) 
}}

{% set pool_ids = [
    'ae8f935830f6b418804836eacb0243447b6d977c000200000000000000000ad1'
] %}

{% set blocks_per_hour = 1700 %}

with block_groups as (
    select
        *,
        floor(block_number / {{ blocks_per_hour }}) as block_group
    from {{ ref('fct_polygon_sim_pool_prices') }}
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
