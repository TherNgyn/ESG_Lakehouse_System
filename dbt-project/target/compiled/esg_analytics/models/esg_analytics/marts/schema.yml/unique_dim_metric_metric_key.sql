
    
    

select
    metric_key as unique_field,
    count(*) as n_records

from "delta"."default_marts"."dim_metric"
where metric_key is not null
group by metric_key
having count(*) > 1


