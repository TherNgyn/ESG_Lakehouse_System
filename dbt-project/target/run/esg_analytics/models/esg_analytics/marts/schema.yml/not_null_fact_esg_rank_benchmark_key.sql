
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select benchmark_key
from "delta"."default_marts"."fact_esg_rank"
where benchmark_key is null



  
  
      
    ) dbt_internal_test