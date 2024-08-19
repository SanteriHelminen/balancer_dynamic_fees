{{ 
    config(
        materialized = 'table'
    ) 
}}

{% set fee_sources = [
    'int_polygon_fees_vol_sigmoid'
    'int_polygon_fees_vol'
] %}

with fees_data as (
    {% for fee_source in fee_sources %}
    select
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
        block_number,
        reserve0,
        reserve1
    from {{ ref('fct_polygon_sim_liquidity') }}
),

-- cte for swaps data
swaps_data as (
    select
        block_number,
        price_target
    from {{ ref('int_polygon_sim_swaps') }}
),

-- cte for prices data
prices_data as (
    select
        block_number,
        price,
        cex_price
    from {{ ref('fct_polygon_sim_pool_prices') }}
    where cex_price is not null
),

-- main pool_reserves cte
pool_reserves as (
    select
        r.block_number,
        'ae8f935830f6b418804836eacb0243447b6d977c000200000000000000000ad1' as pool_id,
        r.reserve0 * p.price as reserve_0_usd,
        r.reserve1 as reserve_1_usd,
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
        and (r.block_number <= 54445409 or r.block_number > 55278791)
)

select * from pool_reserves