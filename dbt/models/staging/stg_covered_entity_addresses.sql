with source as (
    select 
        ce_id,
        id_340b,
        data,
        _loaded_at
    from {{ source('raw_340b', 'covered_entities') }}
),

billing_addresses as (
    select
        ce_id,
        id_340b,
        'billing' as address_type,
        data->'billingAddress'->>'addressLine1' as address_line1,
        data->'billingAddress'->>'addressLine2' as address_line2,
        data->'billingAddress'->>'city' as city,
        data->'billingAddress'->>'state' as state,
        data->'billingAddress'->>'zip' as zip,
        data->'billingAddress'->>'organization' as organization,
        false as is_340b_street_address,  -- Added with default false
        _loaded_at as loaded_at
    from source
),

shipping_addresses as (
    select
        ce_id,
        id_340b,
        'shipping' as address_type,
        address->>'addressLine1' as address_line1,
        address->>'addressLine2' as address_line2,
        address->>'city' as city,
        address->>'state' as state,
        address->>'zip' as zip,
        null as organization,
        (address->>'is340BStreetAddress')::boolean as is_340b_street_address,
        _loaded_at as loaded_at
    from source,
    jsonb_array_elements(case 
        when jsonb_typeof(data->'shippingAddresses') = 'array' 
        then data->'shippingAddresses' 
        else '[]'::jsonb 
    end) as address
),

street_addresses as (
    select
        ce_id,
        id_340b,
        'street' as address_type,
        data->'streetAddress'->>'addressLine1' as address_line1,
        data->'streetAddress'->>'addressLine2' as address_line2,
        data->'streetAddress'->>'city' as city,
        data->'streetAddress'->>'state' as state,
        data->'streetAddress'->>'zip' as zip,
        null as organization,
        false as is_340b_street_address,  -- Added with default false
        _loaded_at as loaded_at
    from source
),

unioned as (
    select * from billing_addresses
    union all
    select * from street_addresses
    union all
    select * from shipping_addresses
)

select 
    {{ dbt_utils.generate_surrogate_key(['ce_id', 'address_type', 'address_line1']) }} as address_id,
    *,
    current_timestamp as dbt_loaded_at
from unioned