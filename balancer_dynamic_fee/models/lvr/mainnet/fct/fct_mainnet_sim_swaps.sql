{{ 
    config(
        materialized = 'table',
        tags = ['mainnet', 'mainnet_lvr']
    ) 
}}

with block_timestamps as (
    select
        block_number,
        CAST(SUBSTR(CAST(timestamp AS STRING), 1, 19) AS TIMESTAMP) AS block_timestamp
    from block_timestamp
),

grouped_swaps as (
    select
        swaps.block_number,
        MAX(timestamps.block_timestamp) as block_timestamp,
        swaps.pool_id,
        MAX(swaps.token_in) as token_in,
        MAX(swaps.token_out) as token_out,
        AVG(swaps.fee_tier) as fee_tier,
        MAX(swaps.token_in_1) as token_in_1,
        MAX(swaps.token_out_1) as token_out_1
    from {{ref('metric_mainnet_swaps')}} swaps
    left join block_timestamps timestamps
        on timestamps.block_number = swaps.block_number
    where swaps.block_number >= 18000000
    group by swaps.block_number, swaps.pool_id
),

final as (
    select
        block_timestamp,
        block_number,
        pool_id,
        token_in,
        token_out,
        fee_tier,
        token_in_1,
        token_out_1
    from grouped_swaps
)

select * from final