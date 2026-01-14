
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select score_key
from "delta"."default_marts"."fact_esg_score_risk"
where score_key is null



  
  
      
    ) dbt_internal_test