{{ config(materialized = 'table') }}

select
    b.brand_id,
    b.brand_name,
    count(distinct r.receipt_id) as total_transactions,
    sum(r.total_spent) as total_spent
from {{ ref('staging_users') }} u
inner join {{ ref('staging_receipts') }} r
    on u.user_id = r.user_id
inner join {{ ref('staging_receipt_items') }} ri
    on r.receipt_id = ri.receipt_id
inner join {{ ref('staging_brands') }} b
    on ri.barcode = b.barcode
where 
    u.created_timestamp >= dateadd(month, -6, '2021-03-01 00:00:00.000')
    and b.brand_name is not null
group by 
    b.brand_id,
    b.brand_name
order by 
    total_spent desc,
    total_transactions desc

