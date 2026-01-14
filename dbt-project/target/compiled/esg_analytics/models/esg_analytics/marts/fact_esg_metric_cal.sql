

with scope_emissions as (
    select
        f.company_id,
        c.industry_normalized as industry,
        c.sector_normalized as sector,
        f.year,
        sum(f.normalized_value) as scope_1_2_emissions
    from "delta"."default_marts"."fact_esg_metric" f
    join "delta"."default_marts"."dim_company" c on f.company_id = c.company_key
    join "delta"."default_marts"."dim_metric" m on f.metric_id = m.metric_key
    where m.metric_name LIKE 'Scope 1 & 2 Emissions'
        and f.year is not null
        and f.normalized_value is not null
    
    group by f.company_id, c.industry_normalized, c.sector_normalized, f.year
),

emissions_yoy as (
    select
        company_id,
        industry,
        sector,
        year,
        scope_1_2_emissions as current_emissions,
        lag(scope_1_2_emissions) over (partition by company_id order by year) as prev_emissions,
        case 
            when lag(scope_1_2_emissions) over (partition by company_id order by year) > 0
            then ((scope_1_2_emissions - lag(scope_1_2_emissions) over (partition by company_id order by year)) 
                 / lag(scope_1_2_emissions) over (partition by company_id order by year)) * 100
            else null
        end as emissions_yoy_pct
    from scope_emissions
),

industry_emissions_avg as (
    select
        industry,
        year,
        avg(emissions_yoy_pct) as industry_avg_emissions_yoy
    from emissions_yoy
    where emissions_yoy_pct is not null
    group by industry, year
),

decarbonization as (
    select
        e.company_id,
        e.industry,
        e.sector,
        e.year,
        e.current_emissions,
        e.prev_emissions,
        e.emissions_yoy_pct,
        i.industry_avg_emissions_yoy,
        e.emissions_yoy_pct - coalesce(i.industry_avg_emissions_yoy, 0) as decarbonization_gap
    from emissions_yoy e
    left join industry_emissions_avg i on e.industry = i.industry and e.year = i.year
),

employee_turnover as (
    select
        f.company_id,
        c.industry_normalized as industry,
        c.sector_normalized as sector,
        f.year,
        avg(f.normalized_value) as turnover_rate
    from "delta"."default_marts"."fact_esg_metric" f
    join "delta"."default_marts"."dim_company" c on f.company_id = c.company_key
    join "delta"."default_marts"."dim_metric" m on f.metric_id = m.metric_key
    where m.metric_name LIKE 'Employee Turnover Rate - Total'
        and f.year is not null
        and f.normalized_value <= 100
        and f.normalized_value is not null
    
    group by f.company_id, c.industry_normalized, c.sector_normalized, f.year
),

industry_turnover_avg as (
    select
        industry,
        year,
        avg(turnover_rate) as industry_avg_turnover
    from employee_turnover
    group by industry, year
),

talent_stability as (
    select
        t.company_id,
        t.industry,
        t.sector,
        t.year,
        t.turnover_rate,
        i.industry_avg_turnover,
        t.turnover_rate - coalesce(i.industry_avg_turnover, 0) as talent_stability_gap
    from employee_turnover t
    left join industry_turnover_avg i on t.industry = i.industry and t.year = i.year
),

board_independence as (
    select
        f.company_id,
        c.industry_normalized as industry,
        c.sector_normalized as sector,
        f.year,
        avg(f.normalized_value) as board_independence_pct
    from "delta"."default_marts"."fact_esg_metric" f
    join "delta"."default_marts"."dim_company" c on f.company_id = c.company_key
    join "delta"."default_marts"."dim_metric" m on f.metric_id = m.metric_key
    where m.metric_name like 'Board Independence'
        and f.year is not null
        and f.normalized_value is not null
        and f.normalized_value > 0 and f.normalized_value <= 100
    
    group by f.company_id, c.industry_normalized, c.sector_normalized, f.year
),

governance_yoy as (
    select
        company_id,
        industry,
        sector,
        year,
        board_independence_pct as current_independence,
        lag(board_independence_pct) over (partition by company_id order by year) as prev_independence,
        case 
            when lag(board_independence_pct) over (partition by company_id order by year) > 0
            then board_independence_pct - lag(board_independence_pct) over (partition by company_id order by year)
            else null
        end as independence_yoy_change
    from board_independence
),

combined as (
    select
        coalesce(d.company_id, t.company_id, g.company_id) as company_id,
        coalesce(d.industry, t.industry, g.industry) as industry,
        coalesce(d.sector, t.sector, g.sector) as sector,
        coalesce(d.year, t.year, g.year) as year,

        d.current_emissions as scope_1_2_emissions,
        d.prev_emissions as prev_year_emissions,
        d.emissions_yoy_pct,
        d.industry_avg_emissions_yoy,
        d.decarbonization_gap,
        case 
            when d.decarbonization_gap < -5 then 'Leading Decarbonization'
            when d.decarbonization_gap < 0 then 'Above Industry Avg'
            when d.decarbonization_gap <= 5 then 'At Industry Avg'
            else 'Below Industry Avg'
        end as decarbonization_status,
        
        t.turnover_rate as company_turnover_rate,
        t.industry_avg_turnover,
        t.talent_stability_gap,
        case 
            when t.talent_stability_gap < -5 then 'Excellent Retention'
            when t.talent_stability_gap < 0 then 'Better Than Industry'
            when t.talent_stability_gap <= 5 then 'At Industry Avg'
            else 'High Turnover Risk'
        end as talent_stability_status,

        g.current_independence as company_board_independence,
        g.prev_independence as prev_board_independence,
        g.independence_yoy_change as board_independence_yoy,
        case 
            when g.independence_yoy_change > 5 then 'Improving Governance'
            when g.independence_yoy_change > 0 then 'Slight Improvement'
            when g.independence_yoy_change = 0 then 'Stable'
            when g.independence_yoy_change > -5 then 'Slight Decline'
            else 'Declining Governance'
        end as governance_status
        
    from decarbonization d
    full outer join talent_stability t 
        on d.company_id = t.company_id and d.year = t.year
    full outer join governance_yoy g 
        on coalesce(d.company_id, t.company_id) = g.company_id 
        and coalesce(d.year, t.year) = g.year
    
    where coalesce(d.company_id, t.company_id, g.company_id) is not null
        and coalesce(d.year, t.year, g.year) is not null
)

select
    to_hex(md5(to_utf8(concat(
        cast(company_id as varchar),
        cast(year as varchar),
        'DERIVED'
    )))) as derived_key,
    company_id,
    industry,
    sector,
    year,
    
    -- Environmental (E)
    scope_1_2_emissions,
    prev_year_emissions,
    emissions_yoy_pct,
    industry_avg_emissions_yoy,
    decarbonization_gap,
    decarbonization_status,
    
    -- Social (S)
    company_turnover_rate,
    industry_avg_turnover,
    talent_stability_gap,
    talent_stability_status,
    
    -- Governance (G)
    company_board_independence,
    prev_board_independence,
    board_independence_yoy,
    governance_status,
    
    current_timestamp as created_at,
    current_timestamp as updated_at

from combined

