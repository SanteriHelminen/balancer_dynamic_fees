{{ 
    config(
        materialized = 'table',
        cluster_by = ['pool_id']
    ) 
}}

{% set sources = [
    ('klines_stg_usdt', 'mainnet', '3ff3a210e57cfe679d9ad1e9ba6453a716c56a2e0002000000000000000005d5'),
    ('klines_wbtc_usdt', 'mainnet', 'a6f548df93de924d73be7d25dc02554c6bd66db500020000000000000000000e'),
    ('klines_gno_usdt', 'mainnet', 'f4c0dd9b82da36c07605df83c8a416f11724d88b000200000000000000000026'),
    ('klines_rdnt_usdt', 'mainnet', 'cf7b51ce5755513d4be016b0e28d6edeffa1d52a000200000000000000000617'),
    ('klines_bal_usdt', 'mainnet', '5c6ee304399dbdb9c8ef030ab642b10820db8f56000200000000000000000014'),
    ('klines_ghst_usdt', 'arbitrum', 'ae8f935830f6b418804836eacb0243447b6d977c000200000000000000000ad1'),
    ('klines_rdnt_usdt', 'arbitrum', '32df62dc3aed20d62241930520e665dc181658410002000000000000000003bd'),
    ('klines_stg_usdt', 'arbitrum', '3a4c6d2404b5eb14915041e01f63200a82f4a343000200000000000000000065'),
] %}

with final as (
    {% for source, chain, pool_id in sources %}
        select
            timestamp,
            to_timestamp(cast(timestamp as double precision)) at time zone 'UTC' as formatted_timestamp,
            '{{ pool_id }}' as pool_id,
            '{{ chain }}' as chain,
            volume,
            open as price
        from {{ source }}
        {% if not loop.last %} 
        union all 
        {% endif %}
    {% endfor %}
)

select * from final
