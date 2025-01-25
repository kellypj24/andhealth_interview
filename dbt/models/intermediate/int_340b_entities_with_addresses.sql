{{
  config(
    materialized = 'table',
    unique_key = 'ce_id'
  )
}}

WITH address_types AS (
    SELECT 
        ce_id,
        -- Pivot billing address details
        MAX(CASE WHEN address_type = 'billing' THEN address_line1 END) as billing_address_line1,
        MAX(CASE WHEN address_type = 'billing' THEN address_line2 END) as billing_address_line2,
        MAX(CASE WHEN address_type = 'billing' THEN city END) as billing_city,
        MAX(CASE WHEN address_type = 'billing' THEN state END) as billing_state,
        MAX(CASE WHEN address_type = 'billing' THEN zip END) as billing_zip,
        MAX(CASE WHEN address_type = 'billing' THEN organization END) as billing_organization,
        -- Pivot shipping address details
        MAX(CASE WHEN address_type = 'shipping' THEN address_line1 END) as shipping_address_line1,
        MAX(CASE WHEN address_type = 'shipping' THEN address_line2 END) as shipping_address_line2,
        MAX(CASE WHEN address_type = 'shipping' THEN city END) as shipping_city,
        MAX(CASE WHEN address_type = 'shipping' THEN state END) as shipping_state,
        MAX(CASE WHEN address_type = 'shipping' THEN zip END) as shipping_zip,
        BOOL_OR(CASE WHEN address_type = 'shipping' THEN is_340b_street_address END) as shipping_is_340b_address,
        -- Pivot street address details
        MAX(CASE WHEN address_type = 'street' THEN address_line1 END) as street_address_line1,
        MAX(CASE WHEN address_type = 'street' THEN address_line2 END) as street_address_line2,
        MAX(CASE WHEN address_type = 'street' THEN city END) as street_city,
        MAX(CASE WHEN address_type = 'street' THEN state END) as street_state,
        MAX(CASE WHEN address_type = 'street' THEN zip END) as street_zip
    FROM {{ ref('stg_covered_entity_addresses') }}
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
    -- Billing address
    addr.billing_address_line1,
    addr.billing_address_line2,
    addr.billing_city,
    addr.billing_state,
    addr.billing_zip,
    addr.billing_organization,
    -- Shipping address
    addr.shipping_address_line1,
    addr.shipping_address_line2,
    addr.shipping_city,
    addr.shipping_state,
    addr.shipping_zip,
    addr.shipping_is_340b_address,
    -- Street address
    addr.street_address_line1,
    addr.street_address_line2,
    addr.street_city,
    addr.street_state,
    addr.street_zip,
    -- Metadata
    ce.loaded_at,
    ce.source_edited_at
FROM {{ ref('stg_covered_entities') }} ce
LEFT JOIN address_types addr
    ON ce.ce_id = addr.ce_id