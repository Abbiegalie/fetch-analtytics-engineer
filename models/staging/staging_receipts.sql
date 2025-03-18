{{ config(
    materialized = 'table'
) }}

with source as (
    select * from {{ source('public', 'receipts') }}
),

staging_receipts as (
    select
        -- Extract receipt_id using regex instead of get()
        regexp_substr(_id::string, '"\\$oid"\\s*:\\s*"([^"]+)"', 1, 1, 'e', 1) as receipt_id,
        userId::string as user_id,
        totalSpent::string as total_spent,
        
        -- Convert dates using regex instead of get()
        to_timestamp(
            regexp_substr(purchaseDate::string, '"\\$date"\\s*:\\s*([0-9]+)', 1, 1, 'e', 1)::number / 1000
        ) as purchase_timestamp,
        
        to_timestamp(
            regexp_substr(createDate::string, '"\\$date"\\s*:\\s*([0-9]+)', 1, 1, 'e', 1)::number / 1000
        ) as create_timestamp,
        
        to_timestamp(
            regexp_substr(modifyDate::string, '"\\$date"\\s*:\\s*([0-9]+)', 1, 1, 'e', 1)::number / 1000
        ) as modify_timestamp,
        
        to_timestamp(
            regexp_substr(pointsAwardedDate::string, '"\\$date"\\s*:\\s*([0-9]+)', 1, 1, 'e', 1)::number / 1000
        ) as points_awarded_timestamp,
        
        to_timestamp(
            regexp_substr(finishedDate::string, '"\\$date"\\s*:\\s*([0-9]+)', 1, 1, 'e', 1)::number / 1000
        ) as finished_timestamp,
        
        to_timestamp(
            regexp_substr(dateScanned::string, '"\\$date"\\s*:\\s*([0-9]+)', 1, 1, 'e', 1)::number / 1000
        ) as scanned_timestamp,
        
        pointsEarned::float as points_earned,
        bonusPointsEarned::float as bonus_points_earned,
        bonusPointsEarnedReason::string as bonus_points_earned_reason,
        purchasedItemCount::number as purchased_item_count,
        rewardsReceiptStatus::string as rewards_receipt_status
    from source
)

select * from staging_receipts
