with source as (
    select 
        ce_id,
        id_340b,
        data,
        _loaded_at
    from {{ source('raw_340b', 'covered_entities') }}
),

authorizing_officials as (
    select
        ce_id,
        id_340b,
        'authorizing_official' as contact_type,
        data->'authorizingOfficial'->>'name' as contact_name,
        data->'authorizingOfficial'->>'phoneNumber' as phone_number,
        data->'authorizingOfficial'->>'title' as contact_title,
        null as phone_extension,  -- Adding null column to match primary_contacts
        _loaded_at
    from source
    where data->'authorizingOfficial' is not null
),

primary_contacts as (
    select
        ce_id,
        id_340b,
        'primary_contact' as contact_type,
        data->'primaryContact'->>'name' as contact_name,
        data->'primaryContact'->>'phoneNumber' as phone_number,
        data->'primaryContact'->>'title' as contact_title,
        data->'primaryContact'->>'phoneNumberExtension' as phone_extension,
        _loaded_at
    from source
    where data->'primaryContact' is not null
),

unioned_contacts as (
    select * from authorizing_officials
    union all
    select * from primary_contacts
)

select 
    {{ dbt_utils.generate_surrogate_key([
        'ce_id', 
        'contact_type', 
        'contact_name'
    ]) }} as contact_id,
    ce_id,
    id_340b,
    contact_type,
    contact_name,
    contact_title,
    phone_number,
    phone_extension,
    _loaded_at,
    current_timestamp as dbt_loaded_at
from unioned_contacts
where contact_name is not null