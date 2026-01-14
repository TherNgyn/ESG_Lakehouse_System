
    
    

with child as (
    select company_id as from_field
    from "delta"."default_marts"."fact_esg_metric"
    where company_id is not null
),

parent as (
    select company_id as to_field
    from "delta"."default_marts"."dim_company"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


