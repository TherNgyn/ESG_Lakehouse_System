
    
    

select
    metric_id as unique_field,
    count(*) as n_records

from "delta"."default_marts"."dim_metric"
where metric_id is not null
group by metric_id
having count(*) > 1


