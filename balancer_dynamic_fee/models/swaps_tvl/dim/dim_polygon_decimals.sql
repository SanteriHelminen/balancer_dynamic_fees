{{ 
    config(
        materialized = 'view',
        tags = ['swaps_tvl']
    ) 
}}

{% set token_decimals = [
    ('GHST', '0x385eeac5cb85a38a9a07a70c73e0a3271cfb54a7', 18),
    ('USDC', '0x2791bca1f2de4661ed88a30c99a7a9449aa84174', 6),
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
