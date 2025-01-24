with source as (
    select 
        ce_id,
        id_340b,
        data,
        _loaded_at
    from {{ source('raw_340b', 'covered_entities') }}
),

npi_details as (
    select
        ce_id,
        id_340b,
        npi->>'npiNumber' as npi_number,
        npi->>'state' as state,
        _loaded_at as loaded_at
    from source,
    jsonb_array_elements(case 
        when jsonb_typeof(data->'npiNumbers') = 'array' 
        then data->'npiNumbers' 
        else '[]'::jsonb 
    end) as npi
),

-- Also capture the primary NPI if it exists
primary_npi as (
    select
        ce_id,
        id_340b,
        data->>'npiNumber' as npi_number,
        data->>'state' as state,
        _loaded_at as loaded_at
    from source
    where data->>'npiNumber' is not null
),

unioned as (
    select * from npi_details
    union all 
    select * from primary_npi
)

select 
    {{ dbt_utils.generate_surrogate_key(['ce_id', 'npi_number', 'state']) }} as npi_id,
    *,
    current_timestamp as dbt_loaded_at
from unioned
where npi_number is not null  -- Remove any null records