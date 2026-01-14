
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select company_id
from "delta"."default_marts"."dim_company"
where company_id is null



  
  
      
    ) dbt_internal_test