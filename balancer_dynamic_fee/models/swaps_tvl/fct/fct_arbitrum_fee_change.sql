{{ 
    config(
        materialized = 'table'
    ) 
}}

{% set sources = [
    ('arbitrum_rdntweth_fee_change', '32df62dc3aed20d62241930520e665dc181658410002000000000000000003bd'),
    ('arbitrum_stgusdc_fee_change', '3a4c6d2404b5eb14915041e01f63200a82f4a343000200000000000000000065'),
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
