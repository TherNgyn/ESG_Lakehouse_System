
  
    

    create table "delta"."default_util"."time_spine"
      
      
    as (
      

WITH dates AS (
    SELECT
        d AS date_day
    FROM UNNEST(
        SEQUENCE(
            DATE '2010-01-01',
            DATE '2035-12-31',
            INTERVAL '1' DAY
        )
    ) AS t(d)
)

SELECT
    date_day
FROM dates
    );

  