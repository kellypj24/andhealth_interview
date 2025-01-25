{{
    config(
        materialized='table',
        unique_key=['ce_id']
    )
}}

with medicaid_metrics as (
    select
        ce_id,
        count(distinct medicaid_number) as total_medicaid_numbers,
        count(distinct state) as unique_medicaid_states,
        string_agg(distinct state, ', ' order by state) as medicaid_states,
        -- Capture primary contacts for medicaid relationships
        string_agg(distinct primary_contact_name, '; ') as medicaid_contact_names,
        string_agg(distinct primary_contact_title, '; ') as medicaid_contact_titles
    from {{ ref('stg_covered_entity_medicaid') }}
    group by ce_id
),

medicaid_details as (
    select
        ce.ce_id,
        ce.id_340b,
        ce.entity_name,
        ce.entity_type,
        ce.is_participating,
        ce.participating_start_date,
        mm.total_medicaid_numbers,
        mm.unique_medicaid_states,
        mm.medicaid_states,
        mm.medicaid_contact_names,
        mm.medicaid_contact_titles,
        ce.loaded_at,
        ce.source_edited_at,
        current_timestamp as dbt_loaded_at
    from {{ ref('stg_covered_entities') }} ce
    left join medicaid_metrics mm on ce.ce_id = mm.ce_id
    where ce.is_participating = true
)

select * from medicaid_details