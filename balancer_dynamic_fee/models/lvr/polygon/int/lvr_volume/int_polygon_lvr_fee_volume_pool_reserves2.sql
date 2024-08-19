{{ 
    config(
        materialized = 'table'
    ) 
}}

{% set fee_sources = [
    'int_polygon_fees_dex_mean_volume',
    'int_polygon_fees_dex_volume_logarithmic',
    'int_polygon_fees_dex_volume_sigmoid',
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

reserves_data AS (
    SELECT
        block_number,
        reserve0,
        reserve1
    FROM {{ ref('fct_polygon_sim_liquidity') }}
),

-- CTE for swaps data
swaps_data AS (
    SELECT
        block_number,
        price_target
    FROM {{ ref('int_polygon_sim_swaps') }}
),

-- CTE for prices data
prices_data AS (
    SELECT
        block_number,
        price,
        cex_price
    FROM {{ ref('fct_polygon_sim_pool_prices') }}
    WHERE cex_price IS NOT NULL
),

-- Main pool_reserves CTE
pool_reserves AS (
    SELECT
        r.block_number,
        'ae8f935830f6b418804836eacb0243447b6d977c000200000000000000000ad1' AS pool_id,
        r.reserve0 * p.price AS reserve_0_usd,
        r.reserve1 AS reserve_1_usd,
        f.fee_tier,
        f.fee_type,
        f.multiplier,
        s.price_target,
        p.price AS pool_price, 
        p.cex_price AS open_price
    FROM reserves_data r
    INNER JOIN prices_data p ON r.block_number = p.block_number
    INNER JOIN swaps_data s ON r.block_number = s.block_number
    LEFT JOIN fees_data f ON r.block_number = f.block_number
    WHERE s.price_target IS NOT NULL
        AND (r.block_number <= 54445409 OR r.block_number > 55278791)
)

SELECT * FROM pool_reserves