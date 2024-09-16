{{ 
    config(
        materialized = 'table',
        tags = ['polygon', 'polygon_lvr']
    ) 
}}

-- set base tokens that determine the token price
{% set base_tokens = "'0x2791bca1f2de4661ed88a30c99a7a9449aa84174'" %}

-- convert block_timestamp
with block_timestamps as (
    select
        block_number as blockno,
        CAST(SUBSTR(CAST(timestamp AS STRING), 1, 19) AS TIMESTAMP) AS block_timestamp
    from polygon_timestamps
),

tvl_timestamps as (
    select
        tvl.block_number,
        timestamps.block_timestamp as block_timestamp,
        tvl.pool_id,
        tvl.pool_name,
        tvl.token_address,
        tvl.reserve
    from {{ ref('metric_polygon_tvl') }} tvl
    left join block_timestamps timestamps
        on tvl.block_number = timestamps.blockno
),

token_1_reserves as (
    select
        block_number,
        block_timestamp,
        pool_id,
        pool_name,
        token_address as token_1,
        reserve as reserve_1
    from tvl_timestamps
    where token_address != {{base_tokens}}
),

token_2_reserves as (
    select
        block_number,
        block_timestamp,
        pool_id,
        pool_name,
        token_address as token_2,
        reserve as reserve_2
    from tvl_timestamps
    where token_address = {{base_tokens}}
),

final as (
    select
        token_1.block_number,
        token_1.block_timestamp,
        token_1.pool_id,
        token_1.pool_name,
        token_1.token_1 as token_1,
        token_1.reserve_1 as reserve_1,
        token_2.token_2 as token_2,
        token_2.reserve_2 as reserve_2,
    from token_1_reserves token_1
    left join token_2_reserves token_2
        on
            token_1.block_number = token_2.block_number
            and token_1.pool_id = token_2.pool_id
)

select * from final
