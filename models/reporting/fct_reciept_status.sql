{{ config(materialized = 'table') }}

with receipt_metrics as (
    select
        rewards_receipt_status,
        
        -- Handle potential parsing issues with total_spent
        try_cast(total_spent as decimal(18,2)) as parsed_total_spent,
        
        purchased_item_count
    from {{ ref('staging_receipts') }}
    where 
        -- Filter for accepted/rejected receipts
        rewards_receipt_status in ('FINISHED', 'REJECTED')
        
        -- Ensure data quality
        and total_spent is not null
        and purchased_item_count is not null
)

select
    rewards_receipt_status,
    
    -- Average spend metrics
    avg(parsed_total_spent) as avg_total_spent,
    
    -- Count metrics
    sum(purchased_item_count) as total_items_purchased,
    count(*) as receipt_count,
    
    -- Derived metrics
    sum(parsed_total_spent) as total_spent,
    sum(parsed_total_spent) / sum(purchased_item_count) as avg_item_price
from receipt_metrics
group by rewards_receipt_status
order by rewards_receipt_status
