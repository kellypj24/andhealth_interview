with source as (
    select 
        ce_id,
        id_340b,
        data,
        _loaded_at
    from {{ source('raw_340b', 'covered_entities') }}
),

contract_pharmacies as (
    select
        ce_id,
        id_340b,
        pharmacy->>'pharmacyId' as pharmacy_id,
        pharmacy->>'contractId' as contract_id,
        pharmacy->>'name' as pharmacy_name,
        pharmacy->>'phoneNumber' as phone_number,
        pharmacy->>'comments' as comments,
        (pharmacy->>'beginDate')::timestamp as begin_date,
        (pharmacy->>'certifiedDecertifiedDate')::timestamp as certified_decertified_date,
        (pharmacy->>'editDate')::timestamp as edit_date,
        pharmacy->'address'->>'addressLine1' as address_line1,
        pharmacy->'address'->>'addressLine2' as address_line2,
        pharmacy->'address'->>'city' as city,
        pharmacy->'address'->>'state' as state,
        pharmacy->'address'->>'zip' as zip,
        _loaded_at
    from source,
    jsonb_array_elements(case 
        when jsonb_typeof(data->'contractPharmacies') = 'array' 
        then data->'contractPharmacies' 
        else '[]'::jsonb 
    end) as pharmacy
    where pharmacy->>'pharmacyId' is not null
)

select 
    {{ dbt_utils.generate_surrogate_key([
        'ce_id',
        'pharmacy_id', 
        'contract_id'
    ]) }} as contract_pharmacy_id,
    ce_id,
    id_340b,
    pharmacy_id,
    contract_id,
    pharmacy_name,
    phone_number,
    comments,
    begin_date,
    certified_decertified_date,
    edit_date,
    address_line1,
    address_line2,
    city,
    state,
    zip,
    _loaded_at,
    current_timestamp as dbt_loaded_at
from contract_pharmacies