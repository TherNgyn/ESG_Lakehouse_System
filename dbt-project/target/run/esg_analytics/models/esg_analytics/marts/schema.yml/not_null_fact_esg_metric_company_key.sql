
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select company_key
from "delta"."default_marts"."fact_esg_metric"
where company_key is null



  
  
      
    ) dbt_internal_test