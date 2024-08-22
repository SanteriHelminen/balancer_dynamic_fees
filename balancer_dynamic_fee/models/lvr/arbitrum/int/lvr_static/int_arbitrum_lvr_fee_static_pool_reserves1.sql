{{ 
    config(
        materialized = 'table',
        tags = ['arbitrum']
    ) 
}}

{% set fee_sources = [
    'int_arbitrum_fees_static'
] %}

with fees_data as (
    {% for fee_source in fee_sources %}
    select
        pool_id,
        block_number,
        fee_type,
        multiplier,
        fee_tier,
    from {{ fee_source }}
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
),

reserves_data as (
    select
        pool_id,
        block_number,
        token0_address,
        token1_address,
        reserve_0,
        reserve_1
    from {{ ref('fct_arbitrum_sim_liquidity') }}
),

-- cte for swaps data
swaps_data as (
    select
        pool_id,
        block_number,
        price_target
    from {{ ref('int_arbitrum_sim_swaps') }}
),

-- cte for prices data
prices_data as (
    select
        pool_id,
        block_number,
        price,
        cex_price,
        eth_price
    from {{ ref('fct_arbitrum_sim_pool_prices') }}
    where cex_price is not null
),

-- main pool_reserves cte
pool_reserves as (
    select
        r.block_number,
        r.pool_id,
        r.reserve_0 * p.price as reserve_0_usd,
        case
            when r.token1_address = '0x82af49447d8a07e3bd95bd0d56f35241523fbab1'
                then multiply(r.reserve_1, p.eth_price)
            else r.reserve_1
        end as reserve_1_usd,
        f.fee_tier,
        f.fee_type,
        f.multiplier,
        s.price_target,
        p.price as pool_price, 
        p.cex_price as open_price
    from reserves_data r
    inner join prices_data p on r.block_number = p.block_number
    inner join swaps_data s on r.block_number = s.block_number
    left join fees_data f on r.block_number = f.block_number
    where s.price_target is not null
        and (r.block_number <= 188600485 or r.block_number > 196038995)
)

select * from pool_reserves