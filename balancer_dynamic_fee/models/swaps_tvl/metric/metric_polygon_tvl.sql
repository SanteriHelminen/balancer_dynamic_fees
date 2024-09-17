{{ 
    config(
        materialized = 'table',
        tags = ['swaps_tvl']
    ) 
}}

-- Get token reserves for each token.
with reserve_changes as (
    select
        pool_balance_changed.block_number as block_number,
        pool_balance_changed.pool_id as pool_id,
        pool_balance_changed.pool_name as pool_name,
        pool_balance_changed.token_address as token_address,
        multiply(
            cast(pool_balance_changed.token_delta as HUGEINT),
            pow(10, cast(-token_metadata.decimals as HUGEINT))
        ) as token_delta,
    from {{ ref('fct_polygon_poolbalancechanged') }}  as pool_balance_changed
    inner join {{ ref('dim_polygon_decimals') }} as token_metadata
        on
            pool_balance_changed.token_address = token_metadata.token_address
    union all
    select
        pool_balance_managed.block_number as block_number,
        pool_balance_managed.pool_id as pool_id,
        pool_balance_managed.pool_name as pool_name,
        pool_balance_managed.token_address as token_address,
        multiply(
            cast(pool_balance_managed.token_delta as HUGEINT),
            pow(10, cast(-token_metadata.decimals as HUGEINT))
        ) as token_delta,
    from {{ ref('fct_polygon_poolbalancemanaged') }}  as pool_balance_managed
    inner join {{ ref('dim_polygon_decimals') }} as token_metadata
        on
            pool_balance_managed.token_address = token_metadata.token_address
    union all
    select
        swaps.block_number as block_number,
        swaps.pool_id as pool_id,
        swaps.pool_name as pool_name,
        swaps.token_in as token_address,
        multiply(
            cast(swaps.amount_in as HUGEINT), 
            pow(10, cast(-token_metadata.decimals as HUGEINT))
        ) as token_delta,
    from {{ ref('fct_polygon_swap') }} as swaps
    inner join {{ ref('dim_polygon_decimals') }} as token_metadata
        on
            swaps.token_in = token_metadata.token_address
    union all
    select
        swaps.block_number as block_number,
        swaps.pool_id as pool_id,
        swaps.pool_name as pool_name,
        swaps.token_out as token_address,
        multiply(
            cast(swaps.amount_out as HUGEINT), 
            pow(10, cast(-token_metadata.decimals as HUGEINT))
        ) as token_delta,
    from {{ ref('fct_polygon_swap') }} as swaps
    inner join {{ ref('dim_polygon_decimals') }} as token_metadata
        on
            swaps.token_out = token_metadata.token_address
),

reserve_changes_per_block as (
    select
        block_number,
        pool_id,
        pool_name,
        token_address,
        sum(token_delta) as token_delta,
    from reserve_changes
    group by 1, 2, 3, 4
),

reserves as (
    select
        block_number,
        pool_id,
        pool_name,
        token_address,
        sum(token_delta) over (
            partition by pool_id, pool_name, token_address
            order by block_number rows between unbounded preceding and current row
        ) as reserve,
    from reserve_changes_per_block
)

select * from reserves
order by block_number
