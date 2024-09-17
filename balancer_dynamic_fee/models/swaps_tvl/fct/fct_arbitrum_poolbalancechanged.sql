{{ 
    config(
        materialized = 'table',
        tags = ['swaps_tvl']
    ) 
}}

with raw_balance_change as (
    select
        arbitrum_poolbalancechanged.*,
        pools.pool_name
    from arbitrum_poolbalancechanged
    inner join {{ ref('dim_pools') }} as pools
    on arbitrum_poolbalancechanged.pool_id = pools.pool_id
),

balance_change as (
    select distinct
        cast(block_number as bigint) as block_number,
        transaction_hash,
        pool_name,
        cast(concat(
            lpad(block_number::text, 18, '0'),
            lpad(log_index::text, 4, '0')
        ) as bigint) as id,
        pool_id,
        lower(replace(tokens[1], '''', '')) as token_address,
        cast(delta[1] as hugeint) - cast(protocol_fee[1] as hugeint) as token_delta
    from raw_balance_change
    union all
    select distinct
        cast(block_number as bigint) as block_number,
        transaction_hash,
        pool_name,
        cast(concat(
            lpad(block_number::text, 18, '0'),
            lpad(log_index::text, 4, '0')
        ) as bigint) as id,
        pool_id,
        lower(replace(tokens[2], '''', '')) as token_address,
        cast(delta[2] as hugeint) - cast(protocol_fee[2] as hugeint) as token_delta
    from raw_balance_change
)

select * from balance_change
