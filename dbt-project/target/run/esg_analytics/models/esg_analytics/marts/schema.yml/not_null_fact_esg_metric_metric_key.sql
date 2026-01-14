
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select metric_key
from "delta"."default_marts"."fact_esg_metric"
where metric_key is null



  
  
      
    ) dbt_internal_test