{{ 
    config(
        materialized = 'table'
    ) 
}}

WITH pool_reserves AS (
    SELECT * FROM {{ ref('int_arbitrum_lvr_fee_volatility_pool_reserves1') }}
    UNION ALL
    SELECT * FROM {{ ref('int_arbitrum_lvr_fee_volatility_pool_reserves2') }}
),

lvr_calculation AS (
    SELECT
        block_number,
        pool_id,
        pool_price,
        price_target,
        open_price,
        fee_tier,
        fee_type,
        multiplier,
        ABS(pool_price - open_price) AS price_difference,
        SQRT(reserve_0_usd * reserve_1_usd) AS liquidity,
        SQRT(reserve_0_usd * reserve_1_usd) * ABS((price_target - pool_price)/pool_price) AS executed_qty,
        SQRT(pool_price * price_target) AS average_price,
        CASE
            WHEN (pool_price < price_target) AND (price_target < open_price) AND (price_target / pool_price > 1 + fee_tier) THEN TRUE
            WHEN (pool_price > price_target) AND (price_target > open_price) AND (pool_price / price_target > 1 + fee_tier) THEN TRUE
            ELSE FALSE
        END AS can_have_lvr
    FROM pool_reserves
    WHERE (block_number <= 188600485 OR block_number > 196038995)
),

price_difference_percentiles AS (
    SELECT
        pool_id,
        multiplier,
        fee_type,
        PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY price_difference) AS ninetyfifth_percentile
    FROM lvr_calculation
    GROUP BY pool_id, multiplier, fee_type
),

lvr_results AS (
    SELECT
        l.block_number,
        l.pool_id,
        l.fee_tier,
        l.fee_type,
        l.multiplier,
        IF(l.can_have_lvr, l.executed_qty * ABS((l.open_price - l.average_price)/l.average_price), 0) AS lvr_value,
        IF(l.can_have_lvr, l.fee_tier * l.executed_qty, 0) AS fee,
        l.can_have_lvr
    FROM lvr_calculation l
    JOIN price_difference_percentiles p 
        ON l.pool_id = p.pool_id 
        AND l.multiplier = p.multiplier 
        AND l.fee_type = p.fee_type
    WHERE l.price_difference <= p.ninetyfifth_percentile
)

SELECT DISTINCT
    pools.pool_name,
    lvr.* 
FROM lvr_results lvr
left join {{ ref('dim_pools') }} pools on pools.pool_id = lvr.pool_id
ORDER BY block_number, lvr.pool_id, fee_type, multiplier