{{ 
    config(
        materialized = 'view',
        tags = ['swaps_tvl']
    ) 
}}

{% set token_decimals = [
    ('BAL', '0xba100000625a3754423978a60c9317c58a424e3d', 18),
    ('WETH', '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', 18),
    ('GNO', '0x6810e776880c02933d47db1b9fc05908e5386b96', 18),
    ('WBTC', '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599', 8),
    ('USDC', '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48', 6),
    ('STG', '0xaf5191b0de278c7286d6c7cc6ab6bb8a73ba2cd6', 18),
    ('RDNT', '0x137ddb47ee24eaa998a535ab00378d6bfa84f893', 18),
]
%}

with final as (
    {% for token, token_address, decimals in token_decimals %}
        select
            cast('{{ token }}' as string) as token,
            cast('{{ token_address }}' as string) as token_address,
            {{ decimals }} as decimals
        {% if not loop.last -%} union all {%- endif %}
    {% endfor %}
)

select * from final
