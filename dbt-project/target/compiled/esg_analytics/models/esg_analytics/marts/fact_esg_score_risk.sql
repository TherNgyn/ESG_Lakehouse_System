

with level_scores as (
    select
        c.company_key as company_id,
        e.year,
        'esg_level' as source,
        null as overall_score,
        null as esg_pulse,
        e.logo_url,
        e.total_level,
        e.total_grade,
        null as total_esg_risk_score,
        null as esg_risk_level,
        null as esg_risk_percentile,
        null as controversy_score,
        null as controversy_level
    from "delta"."default"."clean_esg_level_score" e
    join "delta"."default_marts"."dim_company" c 
        on upper(trim(e.ticker)) = upper(trim(c.symbol))
        or lower(trim(e.company_name)) = lower(trim(c.company_name_normalized))
    where e.year is not null
),

risk_scores as (
    select
        c.company_key as company_id,
        null as year,
        'sp500_risk' as source,
        null as overall_score,
        null as esg_pulse,
        null as logo_url,
        null as total_level,
        null as total_grade,
        cast(r.total_esg_risk_score as double) as total_esg_risk_score,
        r.esg_risk_level,
        r.esg_risk_percentile,
        cast(r.controversy_score as double) as controversy_score,
        r.controversy_level
    from "delta"."default"."clean_sp500_esg_risk" r
    join "delta"."default_marts"."dim_company" c 
        on upper(trim(r.ticker)) = upper(trim(c.symbol))
        or lower(trim(r.company_name)) = lower(trim(c.company_name_normalized))
),

industrials_scores as (
    select
        c.company_key as company_id,
        year(
            coalesce(
                try_cast(i."update_date-esg_scores" as date),
                date_parse(i."update_date-esg_scores", '%m/%d/%Y'),
                date_parse(i."update_date-esg_scores", '%Y-%m-%d'),
                date_parse(i."update_date-esg_scores", '%d-%b-%Y')
            )
        ) as year,
        'industrials' as source,
        null as overall_score,
        cast(i.Company_ESG_pulse as double) as esg_pulse, 
        null as logo_url,
        null as total_level,
        null as total_grade,
        null as total_esg_risk_score,
        null as esg_risk_level,
        null as esg_risk_percentile,
        null as controversy_score,
        null as controversy_level
    from "delta"."default"."clean_industrials_esg_score" i
    join "delta"."default_marts"."dim_company" c 
        on upper(trim(i.Symbol)) = upper(trim(c.symbol)) 
        or lower(trim(i.Company_name)) = lower(trim(c.company_name_normalized))  
    where i."update_date-esg_scores" is not null
        and lower(trim(cast(i."update_date-esg_scores" as varchar))) not in ('unknown', 'n/a', 'null', '')
),

esg_crawled_scores as (
    select
        c.company_key as company_id,
        year(cast(e.last_updated as timestamp)) as year,
        'esg_crawled' as source,
        cast(e.esg_score as double) as overall_score,
        null as esg_pulse,
        null as logo_url,
        null as total_level,
        null as total_grade,
        null as total_esg_risk_score,
        null as esg_risk_level,
        null as esg_risk_percentile,
        null as controversy_score,
        null as controversy_level
    from "delta"."default"."clean_esg_score" e
    join "delta"."default_marts"."dim_company" c 
        on lower(trim(e.company)) = lower(trim(c.company_name_normalized))
    where e.last_updated is not null
        and lower(trim(cast(e.last_updated as varchar))) not in ('unknown', 'n/a', 'null', '')
        and e.esg_score is not null
),

all_scores as (
    select * from level_scores
    union all
    select * from risk_scores
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
    logo_url,
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