{{
    config(
        materialized = 'table',
        unique_key = "concat(contact_key, '_', valid_from)"
    )
}}

with standardized_contacts as (
    select
        contact_id,
        ce_id,
        id_340b,
        contact_type,
        contact_name,
        contact_title,
        phone_number,
        phone_extension,
        _loaded_at,
        dbt_loaded_at,
        -- Standardize for consistent key generation
        trim(regexp_replace(lower(contact_name), '[.,]', '')) as std_contact_name,
        regexp_replace(phone_number, '[^0-9]', '') as std_phone_number
    from {{ ref('stg_covered_entity_contacts') }}
),

-- First get distinct contact information changes
contact_changes as (
    select distinct
        contact_name,
        contact_title,
        phone_number,
        phone_extension,
        std_contact_name,
        std_phone_number,
        _loaded_at,
        dbt_loaded_at
    from standardized_contacts
),

-- Track versions of contact information
contact_versions as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'std_contact_name',
            'std_phone_number'
        ]) }} as contact_key,
        contact_name,
        contact_title,
        phone_number,
        phone_extension,
        _loaded_at,
        dbt_loaded_at,
        -- Get actual version changes based on any field updates
        row_number() over (
            partition by std_contact_name, std_phone_number 
            order by _loaded_at, dbt_loaded_at
        ) as version_sequence,
        lead(_loaded_at) over (
            partition by std_contact_name, std_phone_number 
            order by _loaded_at, dbt_loaded_at
        ) as next_loaded_at
    from contact_changes
),

-- Get role information for each contact
contact_roles as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'std_contact_name',
            'std_phone_number'
        ]) }} as contact_key,
        array_agg(distinct contact_type) as role_types,
        count(distinct contact_type) as role_count,
        count(distinct ce_id) as entity_count,
        array_agg(distinct ce_id) as entity_ids,
        array_agg(distinct id_340b) as entity_340b_ids
    from standardized_contacts
    group by 
        std_contact_name,
        std_phone_number
)

select
    cv.contact_key,
    cv.contact_name,
    cv.contact_title,
    cv.phone_number,
    cv.phone_extension,
    cv.version_sequence,
    -- Role information
    array_to_string(cr.role_types, ', ') as roles,
    cr.role_count,
    cr.entity_count,
    array_to_string(cr.entity_ids, ', ') as entity_ids,
    array_to_string(cr.entity_340b_ids, ', ') as entity_340b_ids,
    -- Role type flags
    array_position(cr.role_types, 'authorizing_official') is not null as is_authorizing_official,
    array_position(cr.role_types, 'primary_contact') is not null as is_primary_contact,
    -- Temporal fields
    cv._loaded_at as valid_from,
    coalesce(cv.next_loaded_at, '9999-12-31'::timestamp) as valid_to,
    cv.next_loaded_at is null as is_current,
    -- Metadata
    cv._loaded_at,
    cv.dbt_loaded_at,
    current_timestamp as model_loaded_at
from contact_versions cv
join contact_roles cr on cv.contact_key = cr.contact_key