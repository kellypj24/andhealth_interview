{{
    config(
        materialized='table',
        unique_key=['ce_id', 'npi_number']
    )
}}

with source_entities as (
    select * from {{ ref('stg_covered_entities') }}
),

source_npi as (
    select * from {{ ref('stg_covered_entity_npi') }}
),

final as (
    select 
        -- Entity identifiers
        ce.ce_id,
        ce.id_340b,
        ce.entity_name,  -- Changed from ce.name
        ce.entity_type,
        ce.is_participating,  -- Changed from ce.participating
        ce.participating_start_date,
        ce.grant_number,
        
        -- NPI specific fields
        npi.npi_number,
        npi.state as npi_state,
        
        -- Audit fields
        ce.loaded_at,  -- Changed from _loaded_at
        ce.source_edited_at  -- Changed from _extracted_at
        
    from source_entities ce
    left join source_npi npi 
        on ce.ce_id = npi.ce_id
        and ce.id_340b = npi.id_340b  -- Double check relationship
    where ce.is_participating = true  -- Changed from participating
)

select * from final