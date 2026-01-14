

with source_data as (
    select
        c.company_key as company_id,
        c.industry_normalized as industry,
        c.sector_normalized as sector,
        r.wba_id,
        r.year_benchmarked,
        r.total_rank,
        r.governance_strategy_rank,
        r.ecosystems_biodiversity_rank,
        r.social_community_rank
    from "delta"."default"."clean_sustainability_rank" r
    join "delta"."default_marts"."dim_company" c 
        on trim(r.isin) = trim(c.isin)
        or lower(trim(r.company_name)) = lower(trim(c.company_name_normalized))
),

industry_ranked as (
    select
        *,
        rank() over (
            partition by industry, year_benchmarked
            order by total_rank asc
        ) as industry_rank,
        count(*) over (
            partition by industry, year_benchmarked
        ) as industry_company_count
    from source_data
),

sector_ranked as (
    select
        *,
        rank() over (
            partition by sector, year_benchmarked
            order by total_rank asc
        ) as sector_rank,
        count(*) over (
            partition by sector, year_benchmarked
        ) as sector_company_count
    from industry_ranked
)

select
    to_hex(md5(to_utf8(concat(
        cast(company_id as varchar),
        cast(year_benchmarked as varchar),
        'WBA'
    )))) as benchmark_key,
    company_id,
    industry,
    sector,
    wba_id,
    year_benchmarked,
    total_rank,
    governance_strategy_rank,
    ecosystems_biodiversity_rank,
    social_community_rank,
    industry_rank,
    industry_company_count,
    sector_rank,
    sector_company_count,
    current_timestamp as loaded_at
from sector_ranked