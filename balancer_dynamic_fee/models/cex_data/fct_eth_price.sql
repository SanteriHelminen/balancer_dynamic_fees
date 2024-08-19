{{
    config(
        materialized = 'table'
    )
}}

with final as (
    select
        timestamp,
        to_timestamp(cast(timestamp as double precision)) at time zone 'UTC' as formatted_timestamp,
        open as price,
    from klines_eth_usdt
)

select * from final
