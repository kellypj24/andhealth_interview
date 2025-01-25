{{
    config(
        materialized = 'table',
        unique_key = 'location_key'
    )
}}

with source_addresses as (
    select 
        address_id,
        ce_id,
        address_type,
        address_line1,
        address_line2,
        city,
        state,
        zip,
        organization,
        is_340b_street_address,
        loaded_at,
        dbt_loaded_at,
        row_number() over(
            partition by 
                address_id,
                ce_id,
                address_type
            order by 
                loaded_at desc,
                dbt_loaded_at desc
        ) as rn
    from {{ ref('stg_covered_entity_addresses') }}
),

standardized_addresses as (
    select
        *,
        trim(regexp_replace(lower(address_line1), '[.,]', '')) as std_address_line1,
        trim(regexp_replace(lower(address_line2), '[.,]', '')) as std_address_line2,
        trim(lower(city)) as std_city,
        upper(state) as std_state,
        regexp_replace(zip, '[^0-9]', '') as std_zip
    from source_addresses
    where rn = 1
),

unique_locations as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'std_address_line1',
            'std_city',
            'std_state',
            'std_zip'
        ]) }} as location_key,
        mode() within group (order by address_line1) as address_line1,
        mode() within group (order by address_line2) as address_line2,
        mode() within group (order by city) as city,
        mode() within group (order by state) as state,
        mode() within group (order by zip) as zip,
        state as state_code,
        max(case 
            when length(regexp_replace(std_zip, '[^0-9]', '')) >= 5 
            then left(regexp_replace(std_zip, '[^0-9]', ''), 5)
        end) as zip5,
        max(case 
            when length(regexp_replace(std_zip, '[^0-9]', '')) > 5 
            then regexp_replace(std_zip, '[^0-9]', '')
        end) as zip9,
        count(distinct address_id) as source_address_count,
        min(loaded_at) as first_seen_at,
        max(loaded_at) as last_seen_at,
        max(dbt_loaded_at) as dbt_loaded_at
    from standardized_addresses
    where address_line1 is not null
      and city is not null
      and state is not null
      and zip is not null
    group by 
        std_address_line1,
        std_city,
        std_state,
        std_zip,
        state
),

final as (
    select
        location_key,
        address_line1,
        address_line2,
        city,
        state_code,
        zip5,
        zip9,
        {{ get_region('state_code') }} as region,
        source_address_count,
        first_seen_at,
        last_seen_at,
        dbt_loaded_at,
        current_timestamp as model_loaded_at,
        current_timestamp as valid_from,
        null::timestamp as valid_to,
        true as is_current
    from unique_locations
)

select * from final