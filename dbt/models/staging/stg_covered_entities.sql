with source as (
    select 
        ce_id,
        id_340b,
        data,
        _loaded_at
    from {{ source('raw_340b', 'covered_entities') }}
),

extracted as (
    select
        ce_id,
        id_340b,
        data->>'name' as entity_name,
        data->>'entityType' as entity_type,
        data->>'subName' as sub_name,
        (data->>'participating')::boolean as is_participating,
        (data->>'participatingStartDate')::timestamp as participating_start_date,
        data->>'grantNumber' as grant_number,
        data->>'medicaidNumber' as medicaid_number,
        (data->>'certifiedDecertifiedDate')::timestamp as certified_date,
        (data->>'terminationDate')::timestamp as termination_date,
        data->>'terminationReason' as termination_reason,
        data->>'state' as primary_state,
        _loaded_at as loaded_at,
        (data->>'editDate')::timestamp as source_edited_at
    from source
)

select * from extracted