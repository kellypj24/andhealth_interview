{{
    config(
        materialized = 'table',
        unique_key = 'pharmacy_relationship_key'
    )
}}

with latest_entity_records as (
    select 
        ce_id,
        id_340b,
        entity_name,
        entity_type,
        is_participating,
        row_number() over(
            partition by ce_id 
            order by loaded_at desc, source_edited_at desc
        ) as rn
    from {{ ref('stg_covered_entities') }}
),

pharmacy_metrics as (
    select
        ce_id,
        cast(date_trunc('month', current_date) as date) as snapshot_date,
        count(distinct pharmacy_id) as total_pharmacies,
        count(distinct case 
            when certified_decertified_date > current_date - interval '90 days'
            then pharmacy_id 
        end) as recently_certified_pharmacies,
        count(distinct city) as unique_pharmacy_cities,
        count(distinct state) as unique_pharmacy_states,
        string_agg(distinct state, ', ' order by state) as pharmacy_states,
        min(begin_date) as first_contract_date,
        max(begin_date) as most_recent_contract_date,
        count(distinct case 
            when certified_decertified_date is not null 
            and certified_decertified_date <= current_date
            then pharmacy_id 
        end) as active_pharmacy_count
    from {{ ref('stg_contract_pharmacies') }}
    group by ce_id
)

select distinct
    {{ dbt_utils.generate_surrogate_key(['e.ce_id', 'pm.snapshot_date']) }} as pharmacy_relationship_key,
    {{ dbt_utils.generate_surrogate_key(['e.ce_id']) }} as entity_key,
    pm.snapshot_date as date_key,
    coalesce(pm.total_pharmacies, 0) as total_pharmacies,
    coalesce(pm.recently_certified_pharmacies, 0) as recently_certified_pharmacies,
    coalesce(pm.unique_pharmacy_cities, 0) as unique_pharmacy_cities,
    coalesce(pm.unique_pharmacy_states, 0) as unique_pharmacy_states,
    pm.pharmacy_states,
    pm.first_contract_date,
    pm.most_recent_contract_date,
    coalesce(pm.active_pharmacy_count, 0) as active_pharmacy_count,
    case 
        when coalesce(pm.total_pharmacies, 0) > 0 
        then cast((pm.active_pharmacy_count * 100.0 / pm.total_pharmacies) as decimal(10,2))
        else 0.0 
    end as active_pharmacy_percentage,
    current_timestamp as valid_from,
    null::timestamp as valid_to,
    true as is_current

from latest_entity_records e
left join pharmacy_metrics pm on e.ce_id = pm.ce_id
where e.rn = 1  -- Only take the most recent entity record