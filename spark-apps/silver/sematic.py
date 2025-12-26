from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import yaml
import re

INPUT_PATH = "s3a://silver/classified_metrics"
OUTPUT_PATH = "s3a://silver/semantic_classified"
CHECK_CSV_PATH = "s3a://silver/semantic_check.csv"

spark = SparkSession.builder \
    .appName("ESG-Semantic-Classification") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "admin123456") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

with open('/opt/spark-apps/configs/sematic_core.yaml', 'r') as f:
    SEMANTIC_CORE = yaml.safe_load(f)

PRIMARY_ENTITIES = {'employees', 'organization'}
GOVERNANCE_ENTITIES = {'board', 'executives'}

ENVIRONMENTAL_ACTIVITIES = {
    'emission', 'consumption', 'generation', 'withdrawal', 'discharge',
    'disposal', 'recycling', 'reduction', 'packaging', 'waste', 'water',
    'energy', 'ehs', 'fine', 'inspection', 'violation', 'avoided_emission',
    'fleet', 'commuting', 'travel', 'transport', 'spill', 'air_emission',
    'accident', 'occupational_disease', 'social_environmental_investment',
    'process_safety', 'political_contribution', 'data_privacy', 'eco_friendly',
    'waste_generation', 'water_consumption', 'air_pollution', 'eco_purchase'
}

SOCIAL_ACTIVITIES = {
    'training', 'turnover', 'injury', 'illness', 'composition',
    'hiring', 'retention', 'diversity', 'corruption_training', 'discrimination',
    'complaint', 'harassment', 'performance_assessment', 'human_rights',
    'volunteer', 'donation', 'pay_equity', 'parental_leave', 'education_level',
    'community_investment', 'customer_satisfaction', 'board_governance',
    'attrition', 'economic_contribution', 'erg_participation',
    'ethical_violation', 'data_leakage', 'attendance'
}

ETHNIC_POC_KEYS = {
    'american_indian', 'asian', 'black', 'hispanic_latinx', 'native_hawaiian',
    'two_or_more_races', 'not_declared', 'not_disclosed', 'white', 'poc',
    'u.s. poc'
}

LEADERSHIP_ROLE_KEYS = {
    'junior_leadership', 'top_leadership', 'petrotechnical', 'part_time_employee',
    'investment_team', 'operations_team', 'svp_and_above', 'full_time_employee'
}

DEMOGRAPHIC_KEYS = {'female', 'male', 'under_30', 'age_30_50', 'over_50', 'minority', 'pwd', 'indigenous'}
MANAGEMENT_LEVEL_KEYS = {'junior_management', 'middle_management', 'senior_management'}
FUNCTIONAL_ROLE_KEYS = {
    'individual_contributor', 'people_leader', 'engineering', 'sales',
    'light_industrial', 'business_professional', 'administrative',
    'apprenticeship', 'coordination_supervision', 'internship', 'operational', 'superintendence'
}
CONTRACTOR_KEYS = {'contractor'}
EDUCATION_LEVEL_KEYS = {'below_bachelor', 'bachelor', 'master', 'doctoral'}

def build_patterns():
    patterns = {}
    for component in ['entity', 'activity', 'measure', 'population', 'context']:
        patterns[component] = {}
        for key, keywords in SEMANTIC_CORE.get(component, {}).items():
            if isinstance(keywords, list) and keywords:
                escaped = [r'\b' + re.escape(k.lower()) + r'\b' for k in keywords if isinstance(k, str) and k.strip()]
                if escaped:
                    patterns[component][key] = '|'.join(escaped)
    return patterns

PATTERNS = build_patterns()

@udf(returnType=StructType([
    StructField("entity", StringType()),
    StructField("activity", StringType()),
    StructField("measure", StringType()),
    StructField("population", StringType()),
    StructField("context", StringType())
]))
def classify_semantic(metric_name):
    if not metric_name:
        return {"entity": None, "activity": None, "measure": None, "population": None, "context": None}

    text = metric_name.lower()
    result = {comp: [] for comp in ['entity', 'activity', 'measure', 'context']}

    for component in ['entity', 'activity', 'measure', 'context']:
        for key, pattern in PATTERNS[component].items():
            if re.search(pattern, text):
                result[component].append(key)

    pop_matches = [key for key, pattern in PATTERNS['population'].items() if re.search(pattern, text)]

    entity_list = result['entity'][:]

    if any(e in PRIMARY_ENTITIES for e in entity_list):
        entity_list = [e for e in entity_list if e in PRIMARY_ENTITIES]

    activity_set = set(result['activity'])

    if 'composition' not in activity_set:
        entity_list = [e for e in entity_list if e not in GOVERNANCE_ENTITIES]
    if not entity_list:
        if re.search(r'\b(employee|employees|staff|worker|personnel|workforce|headcount)\b', text):
            entity_list = ['employees']
    if not entity_list:
        if activity_set & ENVIRONMENTAL_ACTIVITIES:
            entity_list = ['organization']
        elif activity_set & SOCIAL_ACTIVITIES:
            entity_list = ['employees']
        else:
            entity_list = ['organization']

    has_gender_context = bool(re.search(r'\b(by gender|female|male|women|men)\b', text))
    has_age_context = bool(re.search(r'\b(by age|under 30|30-50|over 50)\b', text))
    has_diversity_signal = bool(re.search(r'\b(diversity|representation|minorities|inclusion|discrimination|harassment|poc)\b', text))

    # Lọc demographic cơ bản
    if pop_matches and any(p in DEMOGRAPHIC_KEYS for p in pop_matches):
        if not (has_gender_context or has_age_context or has_diversity_signal):
            pop_matches = [p for p in pop_matches if p not in DEMOGRAPHIC_KEYS]

    # Lọc ethnic/POC - chỉ khi có diversity signal
    if pop_matches and any(p in ETHNIC_POC_KEYS for p in pop_matches):
        if not has_diversity_signal:
            pop_matches = [p for p in pop_matches if p not in ETHNIC_POC_KEYS]

    # Lọc management level
    if pop_matches and any(p in MANAGEMENT_LEVEL_KEYS for p in pop_matches):
        if not any(a in ['training', 'turnover', 'composition', 'performance_assessment'] for a in activity_set):
            pop_matches = [p for p in pop_matches if p not in MANAGEMENT_LEVEL_KEYS]

    # Lọc functional role
    if pop_matches and any(p in FUNCTIONAL_ROLE_KEYS for p in pop_matches):
        if not any(a in ['composition', 'training', 'performance_assessment'] for a in activity_set):
            pop_matches = [p for p in pop_matches if p not in FUNCTIONAL_ROLE_KEYS]

    # Lọc leadership/role nâng cao
    if pop_matches and any(p in LEADERSHIP_ROLE_KEYS for p in pop_matches):
        if not any(a in ['composition', 'hiring'] for a in activity_set):
            pop_matches = [p for p in pop_matches if p not in LEADERSHIP_ROLE_KEYS]

    # Lọc education level
    if pop_matches and any(p in EDUCATION_LEVEL_KEYS for p in pop_matches):
        if 'composition' not in activity_set:
            pop_matches = [p for p in pop_matches if p not in EDUCATION_LEVEL_KEYS]

    # Lọc contractor - chỉ khi injury
    if pop_matches and any(p in CONTRACTOR_KEYS for p in pop_matches):
        if 'injury' not in activity_set:
            pop_matches = [p for p in pop_matches if p not in CONTRACTOR_KEYS]

    return {
        "entity": ','.join(sorted(set(entity_list))) if entity_list else None,
        "activity": ','.join(sorted(set(result['activity']))) if result['activity'] else None,
        "measure": ','.join(sorted(set(result['measure']))) if result['measure'] else None,
        "population": ','.join(sorted(set(pop_matches))) if pop_matches else None,
        "context": ','.join(sorted(set(result['context']))) if result['context'] else None
    }

def main():
    df = spark.read.format("delta").load(INPUT_PATH)

    df = df.withColumn("semantic", classify_semantic(col("metric_name"))) \
        .withColumn("entity", col("semantic.entity")) \
        .withColumn("activity", col("semantic.activity")) \
        .withColumn("measure", col("semantic.measure")) \
        .withColumn("population", col("semantic.population")) \
        .withColumn("context", col("semantic.context")) \
        .drop("semantic")

    df.select("year", "metric_name", "entity", "activity", "measure", "population", "context") \
        .coalesce(1) \
        .write.mode("overwrite").option("header", "true") \
        .csv(CHECK_CSV_PATH)

    df.write.format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .partitionBy("year") \
        .save(OUTPUT_PATH)

if __name__ == "__main__":
    try:
        main()
    finally:
        spark.stop()