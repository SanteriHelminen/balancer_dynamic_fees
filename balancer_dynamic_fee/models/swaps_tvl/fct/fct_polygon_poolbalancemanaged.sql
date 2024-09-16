{{ 
    config(
        materialized = 'view'
        tags = ['swaps_tvl']
    ) 
}}

with balance_manage as (
    select distinct
        cast(block_number as bigint) as block_number,
        transaction_hash as transaction_hash,
        cast(concat(
            lpad(block_number::text, 18, '0'),
            lpad(log_index::text, 4, '0')
        ) as bigint) as id,
        pool_id as pool_id,
        token as token_address,
        cast(cash_delta as hugeint) as token_delta,
    from polygon_poolbalancemanaged
),

final as (
    select
        balance_manage.*,
        pools.pool_name,
    from balance_manage
    inner join {{ ref('dim_pools') }} as pools
    on
        balance_manage.pool_id = pools.pool_id
)

select * from final
