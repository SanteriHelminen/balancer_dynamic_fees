{{ 
    config(
        materialized = 'table',
        tags = ['mainnet', 'mainnet_gas']
    ) 
}}

{% set blocks_per_hour = 300 %}

with gas_fees as (
    select distinct
        block_number,
        gas_fee / 10**9 as gas_fee
    from block_fees_mainnet
),

block_hourly as (
    select
        block_number,
        (block_number / {{ blocks_per_hour }})::int as hour_block,
        gas_fee
    from gas_fees
),

hourly_sums as (
    select
        hour_block,
        sum(gas_fee) as total_gas_fee
    from block_hourly
    group by hour_block
),

rolling_gas as (
    select
        b.block_number,
        b.hour_block,
        avg(h.total_gas_fee) over (order by h.hour_block rows between 1 preceding and 0 following) as gas_fee
    from block_hourly b
    join hourly_sums h
    on b.hour_block = h.hour_block
)

select * from rolling_gas
