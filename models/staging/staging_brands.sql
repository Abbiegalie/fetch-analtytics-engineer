{{ config(
    materialized = 'table'
) }}

with source as (
    select * from {{ source('public', 'brands') }}
),

staging_brands as (
    select
        -- Brand ID extraction with regexp to handle string data
        regexp_substr(_id::string, '"\\$oid"\\s*:\\s*"([^"]+)"', 1, 1, 'e', 1) as brand_id,
        
        -- Standard fields (no JSON extraction needed)
        name::string as brand_name,
        barcode::string as barcode,
        brandCode::string as brand_code,
        category::string as category,
        categoryCode::string as category_code,
        
        -- CPG reference extraction with regexp
        regexp_substr(cpg::string, '"\\$ref"\\s*:\\s*"([^"]+)"', 1, 1, 'e', 1) as cpg_ref,
        
        -- CPG ID extraction with regexp
        regexp_substr(cpg::string, '"\\$id".*?"\\$oid"\\s*:\\s*"([^"]+)"', 1, 1, 'e', 1) as cpg_id,
        
        -- Flag field (no JSON extraction needed)
        topBrand::boolean as is_top_brand
    from source
)

select * from staging_brands
