
  
    

    create table "delta"."default_marts"."dim_metric"
      
      
    as (
      

with metric_dedup as (
    select
        metric_id as metric_key,
        metric_name,
        metric_group,
        topic,
        current_timestamp as created_at,
        current_timestamp as updated_at,
        row_number() over (
            partition by lower(trim(metric_name)) 
            order by 
                case when metric_name is not null then 1 else 2 end,
                current_timestamp desc 
        ) as rn
    from "delta"."default"."staging_metrics"
)

select
    metric_key,
    metric_name,
    upper(substring(metric_group, 1, 1)) || lower(substring(metric_group, 2)) as metric_group,
    upper(substring(topic, 1, 1)) || lower(substring(topic, 2)) as topic,
    created_at,
    updated_at
from metric_dedup
where rn = 1
    );

  