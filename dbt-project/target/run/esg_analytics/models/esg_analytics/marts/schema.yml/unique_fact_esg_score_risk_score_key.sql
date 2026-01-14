
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    score_key as unique_field,
    count(*) as n_records

from "delta"."default_marts"."fact_esg_score_risk"
where score_key is not null
group by score_key
having count(*) > 1



  
  
      
    ) dbt_internal_test