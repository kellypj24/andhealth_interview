{{
    config(
        materialized = 'table',
        unique_key = 'entity_key'
    )
}}

with latest_entity_records as (
    select 
        ce_id,
        id_340b,
        entity_name,
        entity_type,
        sub_name,
        is_participating,
        participating_start_date,
        grant_number,
        primary_state,
        certified_date,
        termination_date,
        termination_reason,
        loaded_at,
        source_edited_at,
        -- Get the most recent record for each entity
        row_number() over(
            partition by ce_id 
            order by loaded_at desc, source_edited_at desc
        ) as rn
    from {{ ref('stg_covered_entities') }}
),

entity_base as (
    select distinct
        {{ dbt_utils.generate_surrogate_key(['ce.ce_id']) }} as entity_key,
        ce.ce_id,
        ce.id_340b,
        ce.entity_name,
        ce.entity_type,
        ce.sub_name,
        ce.is_participating,
        ce.participating_start_date,
        ce.grant_number,
        ce.primary_state,
        ce.certified_date,
        ce.termination_date,
        ce.termination_reason,
        -- Add key address info
        addr.billing_address_line1,
        addr.billing_city,
        addr.billing_state,
        addr.billing_zip,
        -- Add authorizing official
        cont.authorizing_official_name,
        cont.authorizing_official_title,
        -- Add primary contact
        cont.primary_contact_name,
        cont.primary_contact_title,
        -- Metadata
        ce.loaded_at,
        ce.source_edited_at,
        current_timestamp as valid_from,
        null::timestamp as valid_to,
        true as is_current
    from latest_entity_records ce
    left join {{ ref('int_340b_entities_with_addresses') }} addr 
        on ce.ce_id = addr.ce_id
    left join {{ ref('int_340b_entities_with_contacts') }} cont 
        on ce.ce_id = cont.ce_id
    where ce.rn = 1  -- Only take the most recent record
)

select * from entity_base