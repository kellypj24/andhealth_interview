{{
    config(
        materialized = 'table',
        unique_key = "concat(entity_key, '_', date_key)"
    )
}}

with deduped_entities as (
    select 
        ce_id,
        id_340b,
        entity_name,
        entity_type,
        is_participating,
        participating_start_date,
        primary_state,
        certified_date,
        termination_date,
        loaded_at,
        source_edited_at,
        row_number() over (
            partition by ce_id 
            order by loaded_at desc, source_edited_at desc
        ) as rn
    from {{ ref('stg_covered_entities') }}
),

latest_entities as (
    select 
        ce_id,
        id_340b,
        entity_name,
        entity_type,
        is_participating,
        participating_start_date,
        primary_state,
        certified_date,
        termination_date,
        loaded_at,
        source_edited_at
    from deduped_entities
    where rn = 1
),

medicaid_metrics as (
    select distinct  -- Ensure no duplicates from medicaid
        ce_id,
        id_340b,
        count(distinct medicaid_number) as medicaid_id_count,
        count(distinct state) as medicaid_state_count,
        string_agg(distinct state, ', ' order by state) as medicaid_states,
        min(loaded_at) as first_medicaid_loaded,
        max(loaded_at) as last_medicaid_loaded
    from {{ ref('stg_covered_entity_medicaid') }}
    group by ce_id, id_340b
),

npi_metrics as (
    select distinct  -- Ensure no duplicates from NPI
        ce_id,
        id_340b,
        count(distinct npi_number) as npi_count,
        count(distinct state) as npi_state_count,
        string_agg(distinct state, ', ' order by state) as npi_states,
        min(loaded_at) as first_npi_loaded,
        max(loaded_at) as last_npi_loaded
    from {{ ref('stg_covered_entity_npi') }}
    group by ce_id, id_340b
),

provider_metrics as (
    select distinct  -- Ensure distinct metrics after joining
        e.ce_id,
        e.id_340b,
        e.primary_state,  -- Explicitly include primary_state
        cast(date_trunc('month', current_date) as date) as snapshot_date,
        {{ dbt_utils.generate_surrogate_key(['e.ce_id']) }} as entity_key,
        -- Medicaid metrics
        coalesce(m.medicaid_id_count, 0) as medicaid_id_count,
        coalesce(m.medicaid_state_count, 0) as medicaid_state_count,
        m.medicaid_states,
        -- NPI metrics
        coalesce(n.npi_count, 0) as npi_count,
        coalesce(n.npi_state_count, 0) as npi_state_count,
        n.npi_states,
        -- Derived metrics
        case 
            when coalesce(m.medicaid_id_count, 0) > 0 and coalesce(n.npi_count, 0) > 0 
            then true 
            else false 
        end as has_both_medicaid_and_npi,
        case
            when e.primary_state = any(string_to_array(coalesce(m.medicaid_states, ''), ', '))
            then true
            else false
        end as primary_state_has_medicaid,
        case
            when e.primary_state = any(string_to_array(coalesce(n.npi_states, ''), ', '))
            then true
            else false
        end as primary_state_has_npi,
        -- Temporal metrics
        nullif(greatest(
            e.participating_start_date,
            m.first_medicaid_loaded,
            n.first_npi_loaded
        ), '9999-12-31'::timestamp) as first_participation_date,
        nullif(least(
            coalesce(e.termination_date, '9999-12-31'::timestamp),
            m.last_medicaid_loaded,
            n.last_npi_loaded
        ), '9999-12-31'::timestamp) as last_participation_date,
        -- Entity status
        e.is_participating,
        e.participating_start_date,
        e.certified_date,
        e.termination_date,
        -- Metadata
        greatest(
            e.loaded_at,
            coalesce(m.last_medicaid_loaded, '1900-01-01'::timestamp),
            coalesce(n.last_npi_loaded, '1900-01-01'::timestamp)
        ) as latest_source_load,
        e.source_edited_at
    from latest_entities e
    left join medicaid_metrics m on e.ce_id = m.ce_id
    left join npi_metrics n on e.ce_id = n.ce_id
)

select distinct
    entity_key,
    snapshot_date as date_key,
    ce_id,
    id_340b,
    primary_state,  -- Ensure primary_state is in the final select
    medicaid_id_count,
    medicaid_state_count,
    medicaid_states,
    npi_count,
    npi_state_count,
    npi_states,
    has_both_medicaid_and_npi,
    primary_state_has_medicaid,
    primary_state_has_npi,
    first_participation_date,
    last_participation_date,
    is_participating,
    participating_start_date,
    certified_date,
    termination_date,
    latest_source_load,
    source_edited_at,
    current_timestamp as dbt_loaded_at,
    -- SCD fields 
    current_timestamp as valid_from,
    null::timestamp as valid_to,
    true as is_current
from provider_metrics