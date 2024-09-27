{{ 
    config(
        materialized = 'table',
        tags = ['polygon', 'polygon_lvr', 'polygon_fees']
    ) 
}}

{% set blocks_per_hour = 1700 %}

with gas_fees as (
    select distinct
        block_number,
        gas_fee / 10**9 as gas_fee
    from block_fees_polygon
),

block_hourly as (
    select
        block_number,
        (block_number / {{ blocks_per_hour }})::int as hour_block,
        gas_fee
    from gas_fees
),

hourly_avg as (
    select
        hour_block,
        avg(gas_fee) as avg_gas_fee
    from block_hourly
    group by hour_block
),

rolling_gas as (
    select
        b.block_number,
        b.hour_block,
        h.avg_gas_fee as gas_fee
    from block_hourly b
    join hourly_avg h
        on b.hour_block = h.hour_block
)

select * from rolling_gas
