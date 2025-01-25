with source as (
    select 
        ce_id,
        id_340b,
        data,
        _loaded_at
    from {{ source('raw_340b', 'covered_entities') }}
),

base_medicaid_entities as (
    select
        ce_id as parent_ce_id,
        id_340b as parent_id_340b,
        (medicaid->>'ceId')::int as ce_id,
        medicaid->>'id340B' as id_340b,
        medicaid->>'name' as entity_name,
        medicaid->>'subName' as sub_name,
        medicaid->>'entityType' as entity_type,
        medicaid->>'grantNumber' as grant_number,
        medicaid->>'medicaidNumber' as medicaid_number,
        medicaid->>'state' as state,
        (medicaid->>'participating')::boolean as is_participating,
        (medicaid->>'participatingStartDate')::timestamp as participating_start_date,
        (medicaid->>'terminationDate')::timestamp as termination_date,
        medicaid->>'terminationReason' as termination_reason,
        medicaid,
        _loaded_at
    from source,
    jsonb_array_elements(case 
        when jsonb_typeof(data->'medicaidNumbers') = 'array' 
        then data->'medicaidNumbers' 
        else '[]'::jsonb 
    end) as medicaid
),

nested_medicaid_entities as (
    select
        bm.parent_ce_id,
        bm.parent_id_340b,
        bm.ce_id as parent_medicaid_ce_id,
        bm.id_340b as parent_medicaid_id_340b,
        (nested->>'ceId')::int as ce_id,
        nested->>'id340B' as id_340b,
        nested->>'name' as entity_name,
        nested->>'subName' as sub_name,
        nested->>'entityType' as entity_type,
        nested->>'grantNumber' as grant_number,
        (nested->>'participating')::boolean as is_participating,
        (nested->>'participatingStartDate')::timestamp as participating_start_date,
        null::timestamp as termination_date,
        null as termination_reason,
        bm._loaded_at
    from base_medicaid_entities bm,
    jsonb_array_elements(case 
        when jsonb_typeof(bm.medicaid->'medicaidNumbers') = 'array' 
        then bm.medicaid->'medicaidNumbers' 
        else '[]'::jsonb 
    end) as nested
),

unioned_entities as (
    select
        parent_ce_id::int,
        parent_id_340b,
        null::int as parent_medicaid_ce_id,
        null as parent_medicaid_id_340b,
        ce_id::int,
        id_340b,
        entity_name,
        sub_name,
        entity_type,
        grant_number,
        medicaid_number,
        state,
        is_participating,
        participating_start_date,
        termination_date,
        termination_reason,
        _loaded_at
    from base_medicaid_entities
    
    union all
    
    select 
        parent_ce_id::int,
        parent_id_340b,
        parent_medicaid_ce_id::int,
        parent_medicaid_id_340b,
        ce_id::int,
        id_340b,
        entity_name,
        sub_name,
        entity_type,
        grant_number,
        null as medicaid_number,
        null as state,
        is_participating,
        participating_start_date,
        termination_date,
        termination_reason,
        _loaded_at
    from nested_medicaid_entities
)

select 
    {{ dbt_utils.generate_surrogate_key([
        'parent_ce_id',
        'ce_id',
        'coalesce(medicaid_number, id_340b)'
    ]) }} as medicaid_entity_id,
    *,
    current_timestamp as dbt_loaded_at
from unioned_entities
where ce_id is not null