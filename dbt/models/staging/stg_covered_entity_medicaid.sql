with source as (
    select 
        ce_id,
        id_340b,
        data,
        -- Extract contact information at the entity level
        data->'authorizingOfficial'->>'name' as auth_official_name,
        data->'authorizingOfficial'->>'phoneNumber' as auth_official_phone,
        data->'authorizingOfficial'->>'title' as auth_official_title,
        data->'primaryContact'->>'name' as primary_contact_name,
        data->'primaryContact'->>'phoneNumber' as primary_contact_phone,
        data->'primaryContact'->>'phoneNumberExtension' as primary_contact_phone_ext,
        data->'primaryContact'->>'title' as primary_contact_title,
        _loaded_at
    from {{ source('raw_340b', 'covered_entities') }}
),

medicaid_details as (
    select
        ce_id,
        id_340b,
        medicaid->>'medicaidNumber' as medicaid_number,
        medicaid->>'state' as state,
        auth_official_name,
        auth_official_phone,
        auth_official_title,
        primary_contact_name,
        primary_contact_phone,
        primary_contact_phone_ext,
        primary_contact_title,
        _loaded_at as loaded_at
    from source,
    jsonb_array_elements(case 
        when jsonb_typeof(data->'medicaidNumbers') = 'array' 
        then data->'medicaidNumbers' 
        else '[]'::jsonb 
    end) as medicaid
),

-- Also grab the primary medicaid number if it exists
primary_medicaid as (
    select
        ce_id,
        id_340b,
        data->>'medicaidNumber' as medicaid_number,
        data->>'state' as state,
        auth_official_name,
        auth_official_phone,
        auth_official_title,
        primary_contact_name,
        primary_contact_phone,
        primary_contact_phone_ext,
        primary_contact_title,
        _loaded_at as loaded_at
    from source
    where data->>'medicaidNumber' is not null
),

unioned as (
    select * from medicaid_details
    union all
    select * from primary_medicaid
)

select 
    {{ dbt_utils.generate_surrogate_key(['ce_id', 'medicaid_number', 'state']) }} as medicaid_id,
    *,
    current_timestamp as dbt_loaded_at
from unioned
where medicaid_number is not null