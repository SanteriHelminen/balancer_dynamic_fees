{{ 
    config(
        materialized = 'table',
        tags = ['arbitrum', 'arbitrum_lvr']
    ) 
}}

with reserves_per_pool as (
    select
        block_number,
        block_timestamp as timestamp,
        pool_id,
        token_1 as token0_address,
        reserve_1 as reserve0,
        token_2 as token1_address,
        reserve_2 as reserve1
    from {{ref('fct_arbitrum_sim_pool_reserves')}}
),

-- Join with the token weights
final as (
    select
        rp.block_number,
        rp.timestamp,
        rp.pool_id,
        rp.token0_address,
        rp.token1_address,
        dp.weight_0,
        dp.weight_1,
        rp.reserve0,
        rp.reserve1,
        multiply(
            rp.reserve0,
            divide(
                0.5,
                cast(dp.weight_0 as FLOAT8)
            )
        ) as reserve_0,
        multiply(
            rp.reserve1,
            divide(
                0.5,
                cast(dp.weight_1 as FLOAT8)
            )
        ) as reserve_1,
    from reserves_per_pool rp
    inner join dim_pools dp
        on rp.pool_id = dp.pool_id
)

select * from final
