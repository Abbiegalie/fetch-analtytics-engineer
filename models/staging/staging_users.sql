{{ config(
    materialized = 'table'
) }}

with source as (
    select * from {{ source('public', 'users') }}
),

staging_users as (
    select
        -- Extract user_id using regex instead of get()
        regexp_substr(_id::string, '"\\$oid"\\s*:\\s*"([^"]+)"', 1, 1, 'e', 1) as user_id,
        
        -- Boolean handling with coalesce
        coalesce(active, false)::boolean as active,
        
        -- Convert dates using regex instead of get()
        to_timestamp(
            regexp_substr(createdDate::string, '"\\$date"\\s*:\\s*([0-9]+)', 1, 1, 'e', 1)::number / 1000
        ) as created_timestamp,
        
        to_timestamp(
            regexp_substr(lastLogin::string, '"\\$date"\\s*:\\s*([0-9]+)', 1, 1, 'e', 1)::number / 1000
        ) as last_login_timestamp,
        
        -- String fields without JSON extraction
        role::string as role,
        signUpSource::string as sign_up_source,
        state::string as state
    from source
)

select * from staging_users