-- BI Analyst Question: What are the top 5 brands by receipts scanned for most recent month?



with receipt_items_with_brand as (
    select
        ri.receipt_id,
        ri.barcode,
        b.brand_id,
        b.brand_name,
        r.scanned_timestamp
    from RAW.PUBLIC_staging.staging_receipt_items ri -- alias for staging_receipt_items
    inner join RAW.PUBLIC_staging.staging_receipts r -- alias for staging_receipts
        on ri.receipt_id = r.receipt_id
    left join RAW.PUBLIC_staging.staging_brands b -- alias for brands
        on ri.barcode = b.barcode
    where b.brand_id is not null
),

-- Get the most recent month's data
recent_month as (
    select
        date_trunc('month', max(scanned_timestamp)) as most_recent_month
    from receipt_items_with_brand
),

-- Count receipts scanned by brand in the most recent month
brand_receipt_counts as (
    select
        brand_id,
        brand_name,
        count(distinct receipt_id) as receipt_count
    from receipt_items_with_brand ri
    cross join recent_month rm
    where date_trunc('month', ri.scanned_timestamp) = rm.most_recent_month
    group by brand_id, brand_name
)

-- Return the top 5 brands
select
    brand_id,
    brand_name,
    receipt_count
from brand_receipt_counts
order by receipt_count desc
limit 5