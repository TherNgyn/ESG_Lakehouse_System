with dates as (
    select sequence(
        date('2015-01-01'),
        date('2030-12-31'),
        interval 1 day
    ) as day
)
select
    day as date,
    extract(year from day) as year,
    extract(month from day) as month,
    extract(day from day) as day_of_month,
    extract(week from day) as week_of_year,
    extract(quarter from day) as quarter
from dates