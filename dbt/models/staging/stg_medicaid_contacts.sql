{{
  config(
    materialized = 'table',
    unique_key = 'medicaid_contact_sk'
  )
}}

with source as (
    select * from {{ source('raw_340b', 'covered_entities') }}
),

authorizing_officials as (
    select 
        ce_id,
        (data->'medicaidNumbers'->'authorizingOfficial')::jsonb as contact_data,
        'AUTHORIZING_OFFICIAL' as contact_type
    from source
    where data->'medicaidNumbers'->'authorizingOfficial' is not null
),

primary_contacts as (
    select 
        ce_id,
        (data->'medicaidNumbers'->'primaryContact')::jsonb as contact_data,
        'PRIMARY_CONTACT' as contact_type
    from source
    where data->'medicaidNumbers'->'primaryContact' is not null
),

unioned_data as (
    select
        ce_id::int,
        contact_type,
        (contact_data->>'name')::text as full_name,
        (contact_data->>'phoneNumber')::text as phone_number,
        (contact_data->>'phoneNumberExtension')::text as phone_extension,
        (contact_data->>'title')::text as title,
        -- Split full name into components
        split_part(trim(contact_data->>'name'), ' ', 1) as first_name,
        case 
            when array_length(string_to_array(trim(contact_data->>'name'), ' '), 1) > 2 
            then split_part(trim(contact_data->>'name'), ' ', 2)
        end as middle_name,
        case 
            when array_length(string_to_array(trim(contact_data->>'name'), ' '), 1) > 2 
            then split_part(trim(contact_data->>'name'), ' ', 3)
            else split_part(trim(contact_data->>'name'), ' ', 2)
        end as last_name,
        current_timestamp as loaded_at
    from authorizing_officials

    union all

    select
        ce_id::int,
        contact_type,
        (contact_data->>'name')::text as full_name,
        (contact_data->>'phoneNumber')::text as phone_number,
        (contact_data->>'phoneNumberExtension')::text as phone_extension,
        (contact_data->>'title')::text as title,
        split_part(trim(contact_data->>'name'), ' ', 1) as first_name,
        case 
            when array_length(string_to_array(trim(contact_data->>'name'), ' '), 1) > 2 
            then split_part(trim(contact_data->>'name'), ' ', 2)
        end as middle_name,
        case 
            when array_length(string_to_array(trim(contact_data->>'name'), ' '), 1) > 2 
            then split_part(trim(contact_data->>'name'), ' ', 3)
            else split_part(trim(contact_data->>'name'), ' ', 2)
        end as last_name,
        current_timestamp as loaded_at
    from primary_contacts
)

select
    {{ dbt_utils.generate_surrogate_key([
        'ce_id',
        'contact_type',
        'full_name',
        'phone_number'
    ]) }} as medicaid_contact_sk,
    *
from unioned_data