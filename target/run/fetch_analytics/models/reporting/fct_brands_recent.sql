
  
    

        create or replace transient table RAW.PUBLIC.fct_brands_recent
         as
        (

select
    b.brand_id,
    b.brand_name,
    count(distinct r.receipt_id) as total_transactions,
    sum(r.total_spent) as total_spent
from RAW.PUBLIC_staging.staging_users u
inner join RAW.PUBLIC_staging.staging_receipts r
    on u.user_id = r.user_id
inner join RAW.PUBLIC_staging.staging_receipt_items ri
    on r.receipt_id = ri.receipt_id
inner join RAW.PUBLIC_staging.staging_brands b
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
        );
      
  