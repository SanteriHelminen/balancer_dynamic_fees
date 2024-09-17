{{ 
    config(
        materialized = 'view',
        tags = ['swaps_tvl']
    ) 
}}

{% set token_decimals = [
    ('WETH', '0x82af49447d8a07e3bd95bd0d56f35241523fbab1', 18),
    ('RDNT', '0x3082cc23568ea640225c2467653db90e9250aaa0', 18),
    ('STG', '0x6694340fc020c5e6b96567843da2df01b2ce1eb6', 18),
    ('USDC.e', '0xff970a61a04b1ca14834a43f5de4533ebddb5cc8', 6),
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
