
  
    

    create table "delta"."default_marts"."dim_company"
      
      
    as (
      

with companies as (
    select
        matched_company_id as company_key,
        company_name, 
        symbol,
        isin,
        sector,
        industry,
        sub_industry,
        city,
        country,
        region,
        upper(substring(name_norm, 1, 1)) || lower(substring(name_norm, 2)) as name_norm_init,
        upper(substring(sector_norm, 1, 1)) || lower(substring(sector_norm, 2)) as sector_norm_init,
        upper(substring(industry_norm, 1, 1)) || lower(substring(industry_norm, 2)) as industry_norm_init,
        upper(substring(country_norm, 1, 1)) || lower(substring(country_norm, 2)) as country_norm_init,
        isin_valid,
        row_number() over (
            partition by lower(trim(name_norm)) -- ĐỔI TẠI ĐÂY: Chặn trùng theo tên chuẩn hóa
            order by 
            case 
                when isin_valid = true then 1
                when sector is not null then 2
                when industry is not null then 3
                else 4
            end,
            matched_company_id asc -- Đảm bảo tính ổn định của bản ghi được chọn
        ) as rn
    from "delta"."default"."staging_companies_mapping"
),

deduplicated as (
    select
        company_key,
        company_name,
        symbol,
        isin,
        sector,
        industry,
        sub_industry,
        city,
        country,
        region,
        name_norm_init,
        sector_norm_init,
        industry_norm_init,
        country_norm_init,
        isin_valid
    from companies
    where rn = 1
)

select
    company_key,
    company_name, 
    symbol,
    isin,
    sector,
    industry,
    sub_industry,
    city,
    country,
    region,
    name_norm_init as company_name_normalized,
    sector_norm_init as sector_normalized,
    industry_norm_init as industry_normalized,
    country_norm_init as country_normalized,
    isin_valid as has_valid_isin,
    current_timestamp as created_at,
    current_timestamp as updated_at
from deduplicated
    );

  