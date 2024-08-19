{{ 
    config(
        materialized = 'table'
    ) 
}}

{% set pools = [
    ('BAL-WETH', '5c6ee304399dbdb9c8ef030ab642b10820db8f56000200000000000000000014', 'mainnet', (0.8, 0.2)),
    ('GNO-WETH', 'f4c0dd9b82da36c07605df83c8a416f11724d88b000200000000000000000026', 'mainnet', (0.8, 0.2)),
    ('WBTC-WETH', 'a6f548df93de924d73be7d25dc02554c6bd66db500020000000000000000000e', 'mainnet', (0.5, 0.5)),
    ('RDNT-WETH (Mainnet)', 'cf7b51ce5755513d4be016b0e28d6edeffa1d52a000200000000000000000617', 'mainnet', (0.8, 0.2)),
    ('STG-USDC (Mainnet)', '3ff3a210e57cfe679d9ad1e9ba6453a716c56a2e0002000000000000000005d5', 'mainnet', (0.5, 0.5)),
    ('GHST-USDC (Polygon)', 'ae8f935830f6b418804836eacb0243447b6d977c000200000000000000000ad1', 'polygon', (0.8, 0.2)),
    ('STG-USDC.e (Arbitrum)', '3a4c6d2404b5eb14915041e01f63200a82f4a343000200000000000000000065', 'arbitrum', (0.5, 0.5)),
    ('RDNT-WETH (Arbitrum)', '32df62dc3aed20d62241930520e665dc181658410002000000000000000003bd', 'arbitrum', (0.8, 0.2)),
]
%}

with final as (
    {% for pool_name, pool_id, chain, weights in pools %}
        select
            '{{ pool_name }}' as pool_name,
            '{{ pool_id }}' as pool_id,
            '{{ chain }}' as chain,
            {{ weights[0] }} as weight_0,
            {{ weights[1] }} as weight_1
        {% if not loop.last -%} union all {%- endif %}
    {% endfor %}
)

select * from final
