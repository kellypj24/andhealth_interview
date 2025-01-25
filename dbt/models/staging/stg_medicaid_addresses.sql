with source as (
    select 
        ce_id,
        id_340b,
        data,
        _loaded_at
    from {{ source('raw_340b', 'covered_entities') }}
),

medicaid_base as (
    select
        ce_id as parent_ce_id,
        id_340b as parent_id_340b,
        medicaid->>'ceId' as ce_id,
        medicaid->>'id340B' as id_340b,
        medicaid as medicaid_data,
        _loaded_at
    from source,
    jsonb_array_elements(case 
        when jsonb_typeof(data->'medicaidNumbers') = 'array' 
        then data->'medicaidNumbers' 
        else '[]'::jsonb 
    end) as medicaid
    where medicaid->>'ceId' is not null
),

billing_addresses as (
    select
        parent_ce_id,
        parent_id_340b,
        ce_id,
        id_340b,
        'billing' as address_type,
        medicaid_data->'billingAddress'->>'addressLine1' as address_line1,
        medicaid_data->'billingAddress'->>'addressLine2' as address_line2,
        medicaid_data->'billingAddress'->>'city' as city,
        medicaid_data->'billingAddress'->>'state' as state,
        medicaid_data->'billingAddress'->>'zip' as zip,
        medicaid_data->'billingAddress'->>'organization' as organization,
        _loaded_at
    from medicaid_base
    where medicaid_data->'billingAddress' is not null
),

shipping_addresses as (
    select
        parent_ce_id,
        parent_id_340b,
        ce_id,
        id_340b,
        'shipping' as address_type,
        address->>'addressLine1' as address_line1,
        address->>'addressLine2' as address_line2,
        address->>'city' as city,
        address->>'state' as state,
        address->>'zip' as zip,
        address->>'zip4' as zip4,
        (address->>'is340BStreetAddress')::boolean as is_340b_street_address,
        _loaded_at
    from medicaid_base,
    jsonb_array_elements(case 
        when jsonb_typeof(medicaid_data->'shippingAddresses') = 'array' 
        then medicaid_data->'shippingAddresses' 
        else '[]'::jsonb 
    end) as address
),

street_addresses as (
    select
        parent_ce_id,
        parent_id_340b,
        ce_id,
        id_340b,
        'street' as address_type,
        medicaid_data->'streetAddress'->>'addressLine1' as address_line1,
        medicaid_data->'streetAddress'->>'addressLine2' as address_line2,
        medicaid_data->'streetAddress'->>'city' as city,
        medicaid_data->'streetAddress'->>'state' as state,
        medicaid_data->'streetAddress'->>'zip' as zip,
        medicaid_data->'streetAddress'->>'zip4' as zip4,
        _loaded_at
    from medicaid_base
    where medicaid_data->'streetAddress' is not null
),

unioned_addresses as (
    select
        parent_ce_id::int,
        parent_id_340b,
        ce_id::int,
        id_340b,
        address_type,
        address_line1,
        address_line2,
        city,
        state,
        zip,
        null as zip4,
        organization,
        false as is_340b_street_address,
        _loaded_at
    from billing_addresses
    where address_line1 is not null
    
    union all
    
    select
        parent_ce_id::int,
        parent_id_340b,
        ce_id::int,
        id_340b,
        address_type,
        address_line1,
        address_line2,
        city,
        state,
        zip,
        zip4,
        null as organization,
        is_340b_street_address,
        _loaded_at
    from shipping_addresses
    where address_line1 is not null
    
    union all
    
    select
        parent_ce_id::int,
        parent_id_340b,
        ce_id::int,
        id_340b,
        address_type,
        address_line1,
        address_line2,
        city,
        state,
        zip,
        zip4,
        null as organization,
        false as is_340b_street_address,
        _loaded_at
    from street_addresses
    where address_line1 is not null
)

select 
    {{ dbt_utils.generate_surrogate_key([
        'parent_ce_id',
        'ce_id', 
        'address_type',
        'address_line1'
    ]) }} as medicaid_address_id,
    *,
    current_timestamp as dbt_loaded_at
from unioned_addresses