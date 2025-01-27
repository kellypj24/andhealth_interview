{{
    config(
        materialized = 'table'
    )
}}

with org_base as (
    -- Get the parent organization info
    select distinct
        entity_name,
        entity_type,
        primary_state
    from {{ ref('stg_covered_entities') }}
    where is_participating = true
),

org_identifiers as (
    -- Collect all associated IDs for each org
    select 
        entity_name,
        array_agg(distinct id_340b) as id_340b_list,
        array_agg(distinct ce_id) as ce_id_list,
        count(distinct ce_id) as location_count
    from {{ ref('stg_covered_entities') }}
    where is_participating = true
    group by entity_name
),

org_medicaid as (
    -- Summarize Medicaid presence
    select 
        e.entity_name,
        count(distinct m.medicaid_number) as total_medicaid_ids,
        array_agg(distinct m.state) as medicaid_states,
        count(distinct m.state) as medicaid_state_count
    from {{ ref('stg_covered_entities') }} e
    left join {{ ref('stg_covered_entity_medicaid') }} m 
        on e.ce_id = m.ce_id
    where e.is_participating = true
    group by e.entity_name
),

org_pharmacies as (
    -- Summarize pharmacy network
    select 
        e.entity_name,
        count(distinct cp.pharmacy_id) as total_pharmacies,
        array_agg(distinct cp.state) as pharmacy_states,
        count(distinct cp.state) as pharmacy_state_count
    from {{ ref('stg_covered_entities') }} e
    left join {{ ref('stg_contract_pharmacies') }} cp 
        on e.ce_id = cp.ce_id
    where e.is_participating = true
    group by e.entity_name
)

select
    -- Organization info
    b.entity_name,
    b.entity_type,
    b.primary_state,
    
    -- Scale metrics
    i.location_count,
    array_length(i.id_340b_list, 1) as id_340b_count,
    i.id_340b_list,
    
    -- Medicaid presence
    m.total_medicaid_ids,
    m.medicaid_state_count,
    m.medicaid_states,
    
    -- Pharmacy network
    p.total_pharmacies,
    p.pharmacy_state_count,
    p.pharmacy_states,
    
    -- Derived metrics
    round(p.total_pharmacies::numeric / i.location_count, 2) as pharmacies_per_location,
    round(m.total_medicaid_ids::numeric / i.location_count, 2) as medicaid_ids_per_location

from org_base b
inner join org_identifiers i 
    on b.entity_name = i.entity_name
left join org_medicaid m 
    on b.entity_name = m.entity_name
left join org_pharmacies p 
    on b.entity_name = p.entity_name

where m.total_medicaid_ids > 0  -- Must have Medicaid presence

order by 
    m.medicaid_state_count desc,
    p.pharmacy_state_count desc,
    i.location_count desc