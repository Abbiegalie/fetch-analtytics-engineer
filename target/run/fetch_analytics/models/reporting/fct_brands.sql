
  
    

        create or replace transient table RAW.PUBLIC.fct_brands
         as
        (-- BI Analyst Questions:
-- 1. What are the top 5 brands by receipts scanned for most recent month?
-- 2. How does the ranking of the top 5 brands by receipts scanned for the recent month compare to the previous month?



-- Join receipt items with brands and users data
with receipt_items_with_details as (
    select
        ri.receipt_id,
        ri.barcode,
        b.brand_id,
        b.brand_name,
        r.scanned_timestamp,
        date_trunc('month', r.scanned_timestamp) as purchase_month
    from RAW.PUBLIC_staging.staging_receipt_items ri 
    inner join RAW.PUBLIC_staging.staging_receipts r
        on ri.receipt_id = r.receipt_id
    left join RAW.PUBLIC_staging.staging_brands b
        on ri.barcode = b.barcode
    where b.brand_id is not null
),

-- Identify most recent and previous month
month_info as (
    select
        max(purchase_month) as most_recent_month,
        dateadd('month', -1, max(purchase_month)) as previous_month
    from receipt_items_with_details
),

-- Calculate brand rankings by month with boolean flag
brand_ranking as (
    select
        rid.brand_id,
        rid.brand_name,
        rid.purchase_month,
        count(distinct rid.receipt_id) as total_receipts,
        rank() over (partition by rid.purchase_month order by count(distinct rid.receipt_id) desc) as rank,
        case
            when rid.purchase_month = mi.most_recent_month then false
            when rid.purchase_month = mi.previous_month then true
            else null
        end as is_previous_month
    from receipt_items_with_details rid
    cross join month_info mi
    where rid.purchase_month in (mi.most_recent_month, mi.previous_month)
    group by rid.brand_id, rid.brand_name, rid.purchase_month, mi.most_recent_month, mi.previous_month
)

-- Final output (most recent months first, then ranked by receipt count)
select
    brand_id,
    brand_name,
    purchase_month,
    total_receipts,
    rank,
    is_previous_month
from brand_ranking
order by purchase_month desc, rank
        );
      
  