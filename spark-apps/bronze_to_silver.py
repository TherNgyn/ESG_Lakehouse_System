from pyspark.sql import SparkSession

#  Cấu hình Spark kết nối với MinIO

spark = SparkSession.builder.appName("Transform_data_from_Bronze_layers")\
                            .config("spark.hadoop.fs.s3a.endpoint", 'http://minio:9000')\
                            .config("spark.hadoop.fs.s3a.access.key", "minioadmin")\
                            .config("spark.hadoop.fs.s3a.secret.key", 'minioadmin')\
                            .config("spark.hadoop.fs.s3a.path.style.access", 'true')\
                            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
                            .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


# 3 loại df tương tự như: ESG_score, ESG_risk, ESG_rank
# ESG_rank_df = spark.read \
#     .option("header", "true") \
#     .option("inferSchema", "true") \
#     .option("encoding", "UTF-8") \
#     .csv("s3a://bronze/raw/ESG_rank/**/*.csv")
ESG_risk_df = spark.read.csv(
    "s3a://bronze/raw/ESG_risk/**/*.csv",
    header=True,
    inferSchema=True,
    multiLine=True,   # QUAN TRỌNG: đọc nhiều dòng trong một ô
    escape='"',       # QUAN TRỌNG: xử lý dấu ngoặc kép
)
# ESG_score_df = spark.read \
#     .option("header", "true") \
#     .option("inferSchema", "true") \
#     .option("encoding", "UTF-8") \
#     .csv("s3a://bronze/raw/ESG_score/**/*.csv")



def processing_esg_score(df):

    print(20*"=")
    print("Starting function transfrom ESG_score_df")
    print(20*"=")
    try:
        df.createOrReplaceTempView("table")

        query = '''
        select distinct
            lower(trim(`Company Name`)) as company_name,
            Date as year,
            ESG_score as esg_score,
            Env_score as environment_score,
            Social_score as social_score,
            Gov_score as governance_score,
            lower(trim(Industry)) as industry,
            
            Case 
                when Scope_1 is Null or Scope_1 < 0 then -1
                else Scope_1
            End as scope_1, -- 

            Case
                when Scope_2 is Null or Scope_2 < 0 then -1
                else Scope_2 
            End as scope_2,

            Case 
                when CO2_emissions < 0  or CO2_emissions is Null then -1
                else CO2_emissions 
            End as co2_emissions,

            Case 
                when Energy_use < 0 or Energy_use is Null then -1
                else Energy_use 
            End as energy_use,

            Case 
                when Water_use < 0  or Water_use is Null then -1
                else Water_use 
            End as water_use,
            
            Case 
                when Water_recycle < 0 or Water_recycle is Null then -1
                else Water_recycle 
            End as water_recycle,

            Case 
                when Injury_rate < 0 or  Injury_rate is Null or Injury_rate > 100 then -1
                else Injury_rate 
            End as injury_rate, 
            
            Case 
                when Women_Employees < 0 or  Women_Employees is Null or Women_Employees > 100 then -1
                else Women_Employees 
            End as women_employees_rate,

            Case 
                when Human_rights != 0 and Human_rights != 1 then -1
                else Human_rights
            End as human_right,

            Case 
                when Turnover_empl < 0 or Turnover_empl > 100 or Turnover_empl is Null then -1
                else Turnover_empl
            End as turnover_rate,

            Case 
                when Board_Size is Null or Board_Size < 0 then -1
                else Board_Size
            End as board_size,

            Case 
                when Bribery is Null or Bribery < 0 then -1
                else Bribery
            End as bribery,

            Case 
                when Strike is Null or Strike < 0 then -1
                else Strike
            End as strike,

            Case 
                when Recycling_Initiatives is Null or Recycling_Initiatives < 0 then -1
                else Recycling_Initiatives
            End as recycling_initiatives
            from table
            where 
                ESG_score is not Null and ESG_score >= 0 and ESG_score <= 100
            and  Env_score is not Null and  Env_score >= 0 and  Env_score <= 100
            and  Gov_score is not Null and  Gov_score >= 0 and  Gov_score <= 100
            and  Social_score is not Null and  Social_score >= 0 and Social_score <= 100
            '''
        spark.sql(query).show(n=20, truncate= False, )
        return spark.sql(query)
    except Exception as e:
        print("An error occurred:",)

def processing_esg_rank(df):
    print(50*"=")
    print("Starting function transfrom ESG_rank_df")
    print(50*"=")
    try:
        df.createOrReplaceTempView("table")
        # loại những column thừa
        filtered_query = '''
    SELECT 
        `name`,
        industry,
        environment_grade,
        environment_level,
        social_grade,
        social_level,
        governance_grade,
        governance_level,
        environment_score,
        social_score,
        governance_score,
        total_score,
        last_processing_date,
        total_grade,
        total_level
    FROM table
    '''

        spark.sql(filtered_query).createOrReplaceTempView("table")

        transform_query = '''
        select distinct
        lower(trim(`name`)) as `company_name`,
        lower(trim(industry)) as industry,
        environment_grade,
        social_grade,
        governance_grade,
        lower(environment_level) as environment_level,
        lower(social_level) as social_level,
        lower(governance_level) as governance_level,
        environment_score / 10 as environment_score,
        social_score / 10 as social_score,
        governance_score / 10 as governance_score,
        year(TO_DATE('19-04-2022', 'dd-MM-yyyy')) AS year,
        total_grade,
        lower(trim(total_level)) as total_level
        from table  
        where 
            environment_score >= 100 and environment_score <= 1000
        and social_score >= 100 and social_score <= 1000
        and governance_score >= 100 and governance_score <= 1000
    '''

        spark.sql(transform_query).show(n=20, truncate= False, )
        print(50*"=")
        print("transfroming ESG_rank_df successfully!!!!")
        print(50*"=")
        return spark.sql(transform_query)
    except Exception as e:
        print(50*"=")
        print("An errorr occured",  e)
        print(50*"=")



def processing_esg_risk(df):
    print(50*"=")
    print("Starting function transfrom ESG_risk_df")
    print(50*"=")
    try:
        
        print(50*"=")
        print("Transfroming ESG_risk_df successfully!!!")
        print(50*"=")
        # return spark.sql(transform_query)
    except Exception as e:
        print(50*"=")
        print("An errorr occured",  e)
        print(50*"=")



# processing_esg_score(ESG_score_df)  
# processing_esg_rank(ESG_rank_df)
processing_esg_risk(ESG_risk_df)

spark.stop()
