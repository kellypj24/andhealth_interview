-- 340B Participation Overview Report
-- Provides a comprehensive view of covered entity participation and key metrics

with participation_overview as (
    select 
        -- Entity Identification
        id_340b,
        ce_id,
        
        -- Participation Status
        is_participating,
        participating_start_date,
        certified_date,
        termination_date,
        
        -- Geographic Insights
        primary_state,
        
        -- Provider Reach Metrics
        medicaid_id_count,
        medicaid_state_count,
        medicaid_states,
        npi_count,
        npi_state_count,
        npi_states,
        
        -- Participation Depth Indicators
        has_both_medicaid_and_npi,
        primary_state_has_medicaid,
        primary_state_has_npi,
        
        -- Temporal Analysis
        first_participation_date,
        last_participation_date,
        
        -- Recency of Data
        latest_source_load,
        
        -- Categorization for AndHealth's Mission
        case 
            when is_participating 
                 and has_both_medicaid_and_npi 
                 and medicaid_state_count > 0 
            then 'High Potential'
            when is_participating 
                 and (has_both_medicaid_and_npi or medicaid_state_count > 0)
            then 'Moderate Potential'
            else 'Limited Potential'
        end as mission_alignment_category,
        
        -- Days in Program Calculation
        case 
            when termination_date is null 
            then date_part('day', current_date - participating_start_date)
            else date_part('day', termination_date - participating_start_date)
        end as days_in_program
    
    from {{ ref('fact_provider_participation') }}
)

-- Final Report Output
select 
    id_340b,
    is_participating,
    primary_state,
    mission_alignment_category,
    medicaid_id_count,
    medicaid_state_count,
    npi_count,
    npi_state_count,
    days_in_program,
    first_participation_date,
    latest_source_load,
    
    -- Aggregation Metrics for Strategic Insights
    count(*) over () as total_entities_in_report,
    count(*) filter (where mission_alignment_category = 'High Potential') over () as high_potential_entities,
    count(*) filter (where mission_alignment_category = 'Moderate Potential') over () as moderate_potential_entities
from participation_overview
where is_participating = true
order by 
    mission_alignment_category,
    days_in_program desc