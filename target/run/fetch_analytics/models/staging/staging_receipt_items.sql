
  
    

        create or replace transient table RAW.PUBLIC_staging.staging_receipt_items
         as
        (

with receipts as (
    select * from raw.public.receipts
)

select
    -- Extract receipt_id using regex instead of get()
    regexp_substr(r._id::string, '"\\$oid"\\s*:\\s*"([^"]+)"', 1, 1, 'e', 1) as receipt_id,
    
    -- Item fields using direct value access
    i.value:barcode::string as barcode,
    i.value:description::string as description,
    i.value:finalPrice::string as final_price,
    i.value:itemPrice::string as item_price,
    i.value:needsFetchReview::boolean as needs_fetch_review,
    i.value:partnerItemId::string as partner_item_id,
    i.value:preventTargetGapPoints::boolean as prevent_target_gap_points,
    i.value:quantityPurchased::number as quantity_purchased,
    i.value:userFlaggedBarcode::string as user_flagged_barcode,
    i.value:userFlaggedNewItem::boolean as user_flagged_new_item,
    i.value:userFlaggedPrice::string as user_flagged_price,
    i.value:userFlaggedQuantity::number as user_flagged_quantity,
    i.value:needsFetchReviewReason::string as needs_fetch_review_reason,
    i.value:pointsNotAwardedReason::string as points_not_awarded_reason,
    i.value:pointsPayerId::string as points_payer_id,
    i.value:rewardsGroup::string as rewards_group,
    i.value:rewardsProductPartnerId::string as rewards_product_partner_id,
    i.value:userFlaggedDescription::string as user_flagged_description,
    i.value:originalMetaBriteBarcode::string as original_meta_brite_barcode,
    i.value:originalMetaBriteDescription::string as original_meta_brite_description,
    i.value:brandCode::string as brand_code,
    i.value:competitorRewardsGroup::string as competitor_rewards_group,
    i.value:discountedItemPrice::string as discounted_item_price,
    i.value:originalReceiptItemText::string as original_receipt_item_text,
    i.value:itemNumber::string as item_number,
    i.value:originalMetaBriteQuantityPurchased::number as original_meta_brite_quantity_purchased,
    i.value:pointsEarned::string as points_earned,
    i.value:targetPrice::string as target_price,
    i.value:competitiveProduct::string as competitive_product,
    i.value:originalFinalPrice::string as original_final_price,
    i.value:originalMetaBriteItemPrice::string as original_meta_brite_item_price,
    i.value:deleted::boolean as deleted,
    i.value:priceAfterCoupon::string as price_after_coupon,
    i.value:metabriteCampaignId::string as metabrite_campaign_id
from
    receipts r,
    lateral flatten(input => parse_json(r.rewardsReceiptItemList)) i
where 
    r.rewardsReceiptItemList is not null
        );
      
  