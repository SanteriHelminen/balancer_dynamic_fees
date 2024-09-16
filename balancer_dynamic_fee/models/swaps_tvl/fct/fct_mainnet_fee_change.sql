{{ 
    config(
        materialized = 'view'
        tags = ['swaps_tvl']
    ) 
}}

{% set sources = [
    ('balweth_fee_change', '5c6ee304399dbdb9c8ef030ab642b10820db8f56000200000000000000000014'),
    ('gnoweth_fee_change', 'f4c0dd9b82da36c07605df83c8a416f11724d88b000200000000000000000026'),
    ('rdntweth_fee_change', 'cf7b51ce5755513d4be016b0e28d6edeffa1d52a000200000000000000000617'),
    ('stgusdc_fee_change', '3ff3a210e57cfe679d9ad1e9ba6453a716c56a2e0002000000000000000005d5'),
    ('wbtcweth_fee_change', 'a6f548df93de924d73be7d25dc02554c6bd66db500020000000000000000000e'),
] %}

-- Add missing fee tiers from archive eth calls
{% set teippi = [
    (12376885, '', 0, '5c6ee304399dbdb9c8ef030ab642b10820db8f56000200000000000000000014', 1500000000000000),
    (12376885, '', 0, 'a6f548df93de924d73be7d25dc02554c6bd66db500020000000000000000000e', 4000000000000000),
] %}

with fee_updates as (
    {% for block_number, transaction_hash, log_index, pool_id, fee in teippi %}
    select
        cast({{block_number}} as bigint) as block_number,
        '{{transaction_hash}}' as transaction_hash,
        cast(concat(
            lpad(cast({{block_number}} as text), 18, '0'),
            lpad(cast({{log_index}} as text), 4, '0')
        ) as bigint) as id,
        '{{ pool_id }}' as pool_id,
        cast({{fee}} as decimal(38, 18)) / cast(pow(10, 18) as decimal(38, 18)) as fee_tier
    {% if not loop.last %} union all {% endif %}
    {% endfor %}
    {% if teippi | length > 0 %} union all {% endif %}
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
