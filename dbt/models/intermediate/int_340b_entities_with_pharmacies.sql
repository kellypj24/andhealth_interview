{{
    config(
        materialized='table',
        unique_key=['ce_id']
    )
}}

with pharmacy_metrics as (
    select
        ce_id,
        count(distinct pharmacy_id) as total_pharmacies,
        count(distinct case 
            when certified_decertified_date > current_date - interval '90 days'
            then pharmacy_id 
        end) as recently_certified_pharmacies,
        min(begin_date) as first_contract_date,
        max(begin_date) as most_recent_contract_date,
        count(distinct city) as unique_pharmacy_cities,
        count(distinct state) as unique_pharmacy_states,
        string_agg(distinct state, ', ' order by state) as pharmacy_states
    from {{ ref('stg_contract_pharmacies') }}
    group by ce_id
),

pharmacy_details as (
    select
        ce.ce_id,
        ce.id_340b,
        ce.entity_name,
        ce.entity_type,
        ce.is_participating,
        ce.participating_start_date,
        pm.total_pharmacies,
        pm.recently_certified_pharmacies,
        pm.first_contract_date,
        pm.most_recent_contract_date,
        pm.unique_pharmacy_cities,
        pm.unique_pharmacy_states,
        pm.pharmacy_states,
        ce.loaded_at,
        ce.source_edited_at,
        current_timestamp as dbt_loaded_at
    from {{ ref('stg_covered_entities') }} ce
    left join pharmacy_metrics pm on ce.ce_id = pm.ce_id
    where ce.is_participating = true
)

select * from pharmacy_details