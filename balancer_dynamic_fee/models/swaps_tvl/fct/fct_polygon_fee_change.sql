{{ 
    config(
        materialized = 'view'
        tags = ['swaps_tvl']
    ) 
}}

{% set sources = [
    ('polygon_ghstusdc_fee_change', 'ae8f935830f6b418804836eacb0243447b6d977c000200000000000000000ad1'),
] %}

with fee_updates as (
    {% for source, pool_id in sources %}
        select
            cast(block_number as bigint) as block_number,
            transaction_hash,
            cast(concat(
                lpad(cast(block_number as text), 18, '0'),
                lpad(cast(log_index as text), 4, '0')
            ) as bigint) as id,
            '{{ pool_id }}' as pool_id,
            cast(fee as decimal(38, 18)) / cast(pow(10, 18) as decimal(38, 18)) as fee_tier
        from {{ source }}
        {% if not loop.last %} union all {% endif %}
    {% endfor %}
),

-- Get the next id for each update.
final as (
    select
        block_number,
        transaction_hash,
        id,
        lead(id) over (
            partition by pool_id
            order by id
        ) as next_id,
        pool_id,
        fee_tier
    from fee_updates
)

select * from final
