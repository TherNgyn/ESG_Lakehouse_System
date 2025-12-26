{{
  config(
    materialized='table',
    schema='marts'
  )
}}

with unit_ranking as (
    select
        staging_unit_id as unit_key,
        original_unit,
        standard_unit,
        conversion_factor,
        unit_category,
        note as conversion_note,
        current_timestamp as created_at,
        current_timestamp as updated_at,
          row_number() over (
            partition by original_unit, standard_unit 
            order by 
                case when conversion_factor is not null then 1 else 2 end,
                case when note is not null then 1 else 2 end,
                staging_unit_id desc
        ) as rn
    from {{ source('silver', 'staging_units') }}
)

select
    unit_key,
    original_unit,
    standard_unit,
    conversion_factor,
    unit_category,
    conversion_note,
    created_at,
    updated_at
from unit_ranking
where rn = 1