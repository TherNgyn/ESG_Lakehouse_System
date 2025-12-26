{{
  config(
    materialized='table',
    schema='marts'
  )
}}

with level_scores as (
    select
        c.company_key as company_id,
        e.year,
        'esg_level' as source,
        (cast(e.environment_score as double) + cast(e.social_score as double) + cast(e.governance_score as double)) / 3.0 as overall_score,
        null as esg_pulse,
        e.total_level,
        e.total_grade,
        null as total_esg_risk_score,
        null as esg_risk_level,
        null as esg_risk_percentile,
        null as controversy_score,
        null as controversy_level
    from {{ source('silver', 'clean_esg_level_score') }} e
    join {{ ref('dim_company') }} c 
        on upper(trim(e.ticker)) = upper(trim(c.symbol))
        or lower(trim(e.company_name)) = lower(trim(c.company_name_normalized))
    where e.year is not null
),

risk_scores as (
    select
        c.company_key as company_id,
        null as year,
        'sp500_risk' as source,
        cast(r.total_esg_risk_score as double) as overall_score,
        null as esg_pulse,
        r.esg_risk_level as total_level,
        null as total_grade,
        cast(r.total_esg_risk_score as double) as total_esg_risk_score,
        r.esg_risk_level,
        r.esg_risk_percentile,
        cast(r.controversy_score as double) as controversy_score,
        r.controversy_level
    from {{ source('silver', 'clean_sp500_esg_risk') }} r
    join {{ ref('dim_company') }} c 
        on upper(trim(r.ticker)) = upper(trim(c.symbol))
        or lower(trim(r.company_name)) = lower(trim(c.company_name_normalized))
),

rank_scores as (
    select
        c.company_key as company_id,
        r.year_benchmarked as year,
        'sustainability_rank' as source,
        cast(r.total_score as double) as overall_score,
        null as esg_pulse,
        null as total_level,
        null as total_grade,
        null as total_esg_risk_score,
        null as esg_risk_level,
        null as esg_risk_percentile,
        null as controversy_score,
        null as controversy_level
    from {{ source('silver', 'clean_sustainability_rank') }} r
    join {{ ref('dim_company') }} c 
        on trim(r.isin) = trim(c.isin)
        or lower(trim(r.company_name)) = lower(trim(c.company_name_normalized))
    where r.year_benchmarked is not null
        and r.year_benchmarked > 0
),

industrials_scores as (
    select
        c.company_key as company_id,
        year(cast(i."update_date-esg_scores" as timestamp)) as year,
        'industrials' as source,
        null as overall_score,
        cast(i.company_esg_pulse as double) as esg_pulse,
        null as total_level,
        null as total_grade,
        null as total_esg_risk_score,
        null as esg_risk_level,
        null as esg_risk_percentile,
        null as controversy_score,
        null as controversy_level
    from {{ source('silver', 'clean_industrials_esg_score') }} i
    join {{ ref('dim_company') }} c 
        on upper(trim(i.symbol)) = upper(trim(c.symbol))
        or lower(trim(i.company_name)) = lower(trim(c.company_name_normalized))
    where i."update_date-esg_scores" is not null
),

esg_crawled_scores as (
    select
        c.company_key as company_id,
        year(cast(e.last_updated as timestamp)) as year,
        'esg_crawled' as source,
        cast(e.esg_score as double) as overall_score,
        null as esg_pulse,
        null as total_level,
        null as total_grade,
        null as total_esg_risk_score,
        null as esg_risk_level,
        null as esg_risk_percentile,
        null as controversy_score,
        null as controversy_level
    from {{ source('silver', 'clean_esg_score') }} e
    join {{ ref('dim_company') }} c 
        on lower(trim(e.company)) = lower(trim(c.company_name_normalized))
    where e.last_updated is not null
        and e.esg_score is not null
),

all_scores as (
    select * from level_scores
    union all
    select * from risk_scores
    union all
    select * from rank_scores
    union all
    select * from industrials_scores
    union all
    select * from esg_crawled_scores
)

select
    to_hex(md5(to_utf8(concat(
        cast(company_id as varchar),
        coalesce(cast(year as varchar), 'NULL'),
        source
    )))) as score_key,
    company_id,
    year,
    source,
    overall_score,
    esg_pulse,
    total_level,
    total_grade,
    total_esg_risk_score,
    esg_risk_level,
    esg_risk_percentile,
    controversy_score,
    controversy_level,
    current_timestamp as created_at,
    current_timestamp as updated_at
from all_scores
where company_id is not null