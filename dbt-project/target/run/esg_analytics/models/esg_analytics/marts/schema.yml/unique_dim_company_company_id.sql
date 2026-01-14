
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    company_id as unique_field,
    count(*) as n_records

from "delta"."default_marts"."dim_company"
where company_id is not null
group by company_id
having count(*) > 1



  
  
      
    ) dbt_internal_test