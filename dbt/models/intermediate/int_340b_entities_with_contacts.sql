-- models/intermediate/int_340b_entities_with_contacts.sql
{{
  config(
    materialized = 'table',
    unique_key = 'ce_id'
  )
}}

WITH entity_contacts AS (
    SELECT 
        ce_id,
        -- Pivot authorizing official details
        MAX(CASE WHEN contact_type = 'authorizing_official' THEN contact_name END) as authorizing_official_name,
        MAX(CASE WHEN contact_type = 'authorizing_official' THEN phone_number END) as authorizing_official_phone,
        MAX(CASE WHEN contact_type = 'authorizing_official' THEN contact_title END) as authorizing_official_title,
        MAX(CASE WHEN contact_type = 'authorizing_official' THEN phone_extension END) as authorizing_official_phone_ext,
        -- Pivot primary contact details
        MAX(CASE WHEN contact_type = 'primary_contact' THEN contact_name END) as primary_contact_name,
        MAX(CASE WHEN contact_type = 'primary_contact' THEN phone_number END) as primary_contact_phone,
        MAX(CASE WHEN contact_type = 'primary_contact' THEN contact_title END) as primary_contact_title,
        MAX(CASE WHEN contact_type = 'primary_contact' THEN phone_extension END) as primary_contact_phone_ext
    FROM {{ ref('stg_covered_entity_contacts') }}
    GROUP BY ce_id
)

SELECT 
    -- Core entity fields
    ce.ce_id,
    ce.id_340b,
    ce.entity_name,
    ce.entity_type,
    ce.sub_name,
    ce.is_participating,
    ce.participating_start_date,
    ce.grant_number,
    ce.medicaid_number,
    ce.certified_date,
    ce.termination_date,
    ce.termination_reason,
    ce.primary_state,
    -- Contact information
    ec.authorizing_official_name,
    ec.authorizing_official_phone,
    ec.authorizing_official_title,
    ec.authorizing_official_phone_ext,
    ec.primary_contact_name,
    ec.primary_contact_phone,
    ec.primary_contact_title,
    ec.primary_contact_phone_ext,
    -- Metadata
    ce.loaded_at,
    ce.source_edited_at
FROM {{ ref('stg_covered_entities') }} ce
LEFT JOIN entity_contacts ec 
    ON ce.ce_id = ec.ce_id