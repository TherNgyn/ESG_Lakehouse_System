
  
    

    create table "delta"."default_marts"."fact_esg_metric"
      
      
    as (
      

with source_data as (
    select
        name as company_name,
        metric_norm as metric_name,
        year,
        value,
        units as original_unit
    from "delta"."default"."metric_norm_final"
    
),

-- STEP 1: Deduplicate source data
deduped_source as (
    select
        company_name,
        metric_name,
        year,
        value,
        original_unit,
        row_number() over (
            partition by 
                lower(trim(company_name)),
                lower(trim(metric_name)),
                year,
                lower(trim(original_unit))
            order by value desc  -- Keep highest value if duplicates
        ) as rn
    from source_data
    where value is not null
),

-- STEP 2: Join with dimensions (inner join for data quality)
with_company as (
    select
        sd.company_name,
        sd.metric_name,
        sd.year,
        sd.value,
        sd.original_unit,
        c.company_key as company_id
    from deduped_source sd
    inner join "delta"."default_marts"."dim_company" c
        on lower(trim(sd.company_name)) = lower(trim(c.company_name_normalized))
    where sd.rn = 1
),

with_metric as (
    select
        wc.company_name,
        wc.metric_name,
        wc.year,
        wc.value,
        wc.original_unit,
        wc.company_id,
        m.metric_key as metric_id
    from with_company wc
    inner join "delta"."default_marts"."dim_metric" m
        on lower(trim(wc.metric_name)) = lower(trim(m.metric_name))
),

with_unit as (
    select
        wm.company_id,
        wm.metric_id,
        wm.year,
        wm.value,
        wm.original_unit,
        u.unit_key as unit_id,
        u.standard_unit,
        u.conversion_factor,
        u.unit_category
    from with_metric wm
    left join "delta"."default_marts"."dim_unit" u
        on lower(trim(wm.original_unit)) = lower(trim(u.original_unit))
),

-- STEP 3: Create final fact records
final as (
    select
        to_hex(md5(to_utf8(concat(
            cast(company_id as varchar),
            cast(metric_id as varchar),
            cast(year as varchar),
            coalesce(cast(unit_id as varchar), 'no_unit')
        )))) as metric_key,
        company_id,
        metric_id,
        unit_id,
        year,
        value as original_value,
        case 
            when unit_category = 'intensity' then value
            when unit_category = 'volume_mass_mixed' then null
            when conversion_factor is not null then value * conversion_factor
            else value
        end as normalized_value,
        standard_unit,
        current_timestamp as created_at,
        current_timestamp as updated_at
    from with_unit
)

-- STEP 4: Final deduplication (safety net)
select
    metric_key,
    company_id,
    metric_id,
    unit_id,
    year,
    original_value,
    normalized_value,
    standard_unit,
    created_at,
    updated_at
from (
    select
        *,
        row_number() over (
            partition by metric_key
            order by normalized_value desc, created_at desc
        ) as final_rn
    from final
)
where final_rn = 1
    );

  