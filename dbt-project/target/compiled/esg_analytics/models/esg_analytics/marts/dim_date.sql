

with date_spine as (
    select date_day
    from "delta"."default_util"."metricflow_time_spine"
),

date_details as (
    select
        date_day,
        extract(year from date_day) as year,
        extract(month from date_day) as month,
        extract(day from date_day) as day_of_month,
        extract(week from date_day) as week_of_year,
        extract(quarter from date_day) as quarter,
        extract(dow from date_day) as day_of_week,
        case 
            when extract(dow from date_day) in (7, 1) then true 
            else false 
        end as is_weekend,
        format_datetime(date_day, 'MMMM') as month_name,
        format_datetime(date_day, 'EEEE') as day_name
    from date_spine
)

select
    date_day as date_id,
    date_day,
    year,
    month,
    day_of_month,
    week_of_year,
    quarter,
    day_of_week,
    is_weekend,
    month_name,
    day_name,
    current_timestamp as created_at,
    current_timestamp as updated_at
from date_details