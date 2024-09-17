{{ 
    config(
        materialized = 'view',
        tags = ['swaps_tvl']
    ) 
}}

with swaps as (
    select distinct
        cast(block_number as bigint) as block_number,
        transaction_hash as transaction_hash,
        cast(concat(
            lpad(block_number::text, 18, '0'),
            lpad(log_index::text, 4, '0')
        ) as bigint) as id,
        pool_id as pool_id,
        lower(token_in) as token_in,
        lower(token_out) as token_out,
        cast(amount_in as hugeint) as amount_in,
        -cast(amount_out as hugeint) as amount_out
    from {{ ref('mainnet_swap') }}
),

final as (
    select
        swaps.*,
        pools.pool_name,
    from swaps
    inner join {{ ref('dim_pools') }} as pools
    on
        swaps.pool_id = pools.pool_id
)

select * from final
