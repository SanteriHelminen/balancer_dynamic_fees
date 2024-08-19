{{ 
    config(
        materialized = 'table'
    ) 
}}

with baseline as (
    select
        pool_id,
        count(case when can_have_lvr then 1 end) as base_lvr_occurrences,
        sum(lvr_value) as base_total_lvr,
        sum(fee) as base_total_fees,
        avg(fee_tier) as base_avg_fee_tier
    from {{ ref('int_mainnet_lvr') }}
    group by pool_id
),

impact_analysis as (
    select
        pool_id,
        fee_type,
        'volume' as category,
        multiplier,
        count(case when can_have_lvr then 1 end) as lvr_occurrences,
        sum(lvr_value) as total_lvr,
        sum(fee) as total_fees,
        avg(fee_tier) as avg_fee_tier
    from {{ ref('int_mainnet_lvr_fee_volume') }}
    group by 1, 2, 4
union all
    select
        pool_id,
        fee_type,
        'gas' as category,
        multiplier,
        count(case when can_have_lvr then 1 end) as lvr_occurrences,
        sum(lvr_value) as total_lvr,
        sum(fee) as total_fees,
        avg(fee_tier) as avg_fee_tier
    from {{ ref('int_mainnet_lvr_fee_gas') }}
    group by 1, 2, 4
union all
    select
        pool_id,
        fee_type,
        'volatility' as category,
        multiplier,
        count(case when can_have_lvr then 1 end) as lvr_occurrences,
        sum(lvr_value) as total_lvr,
        sum(fee) as total_fees,
        avg(fee_tier) as avg_fee_tier
    from {{ ref('int_mainnet_lvr_fee_volatility') }}
    group by 1, 2, 4
),

final_results as (
    select
        ia.pool_id,
        ia.fee_type,
        ia.category,
        ia.multiplier,
        (ia.lvr_occurrences - b.base_lvr_occurrences) / nullif(b.base_lvr_occurrences, 0) as occurrence_change,
        (ia.total_lvr - b.base_total_lvr) / nullif(b.base_total_lvr, 0) as quantity_change,
        (ia.total_fees - b.base_total_fees) / nullif(b.base_total_fees, 0) as fees_change,
        ia.avg_fee_tier,
        b.base_avg_fee_tier
    from impact_analysis ia
    join baseline b on ia.pool_id = b.pool_id
),

final as (
    select
        f.pool_id,
        pools.pool_name,
        f.fee_type,
        f.category,
        f.multiplier,
        round(coalesce(f.occurrence_change * 100, 0), 3) as occurrence_change,
        round(coalesce(f.quantity_change * 100, 0), 3) as quantity_change,
        round(coalesce(f.fees_change * 100, 0), 3) as fees_change,
        f.avg_fee_tier,
        f.base_avg_fee_tier
    from final_results f
    left join {{ ref('dim_pools') }} pools on f.pool_id = pools.pool_id
    order by f.pool_id, f.fee_type, f.multiplier
)

select * from final