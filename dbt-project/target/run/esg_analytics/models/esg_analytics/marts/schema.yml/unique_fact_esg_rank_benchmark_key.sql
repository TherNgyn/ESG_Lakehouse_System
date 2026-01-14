
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    benchmark_key as unique_field,
    count(*) as n_records

from "delta"."default_marts"."fact_esg_rank"
where benchmark_key is not null
group by benchmark_key
having count(*) > 1



  
  
      
    ) dbt_internal_test