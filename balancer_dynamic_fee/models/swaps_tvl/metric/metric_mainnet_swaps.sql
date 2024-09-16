{{ 
    config(
        materialized = 'table'
        tags = ['swaps_tvl']
    ) 
}}

with final as (
    select
        trades.block_number as block_number,
        trades.transaction_hash as transaction_hash,
        trades.id as id,
        trades.pool_id as pool_id,
        trades.token_in as token_in,
        trades.token_out as token_out,
        fee_tier_updates.fee_tier as fee_tier,
        divide(abs(trades.amount_in), pow(10, cast(tokens0.decimals as numeric)))
            as token_in,
        divide(abs(trades.amount_out), pow(10, cast(tokens1.decimals as numeric)))
            as token_out,
    from {{ ref('fct_mainnet_swap') }} as trades
    left join {{ ref('dim_mainnet_decimals') }} as tokens0
        on
            trades.token_in = tokens0.token_address
    left join {{ ref('dim_mainnet_decimals') }} as tokens1
        on
            trades.token_out = tokens1.token_address
    left join {{ ref('fct_mainnet_fee_change') }} as fee_tier_updates
        on
            trades.pool_id = fee_tier_updates.pool_id
            and trades.id >= fee_tier_updates.id
            and (trades.id < fee_tier_updates.next_id or fee_tier_updates.next_id is null)
)

select * from final
order by id
