from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

spark = SparkSession.builder \
    .appName("ESG-Classify-Metrics") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "admin123456") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

INPUT_PATH = "s3a://silver/clean_kpi"
OUTPUT_PATH = "s3a://silver/classified_metrics"
SUMMARY_PATH = "s3a://silver/metric_groups_summary"

df = spark.read.parquet(INPUT_PATH)

print(f"Total rows: {df.count():,}")
print(f"Total columns: {len(df.columns)}")

df.printSchema()
df.show(10, truncate=False)

df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()

if 'source' in df.columns:
    df.groupBy('source').count().orderBy(col('count').desc()).show()

esg_categories = {
    "Scope 1 Emissions": ["scope 1", "direct emission", "direct ghg", "non-energy sources"],
    "Scope 2 Emissions": ["scope 2", "indirect emission", "purchased electricity"],
    "Scope 3 Emissions": ["scope 3", "value chain", "supply chain emission", "downstream", "purchased goods and services emissions","upstream", "category 1", "category 2", "category 3", "category 4", "category 5", "category 6", "category 7", "category 8", "category 11", "category 12", "category 13", "category 14", "category 15", "financed emission"],
    "Total GHG Emissions": ["total emission", "total ghg", "total greenhouse", "total co2", "carbon footprint", "net equity greenhouse", "carbon emission", "co2 from operations", "total location-based emission", "total market-based emission", "amer emission", "total market-based emissions","total location-based emissions","apac emission", "emea emission", "fleet emission", "natural gas emission"],
    "Emission Intensity": ["emission intensity", "carbon intensity", "co2 intensity", "ghg intensity", "emissions per", "methane intensity", "emissions intensity"],
    "Biogenic Emissions": ["biogenic", "biogenic co2", "biomass emissions", "biofuel emissions", "landfill biogenic", "biomass"],
    "Renewable Energy": ["renewable energy", "clean energy", "solar", "wind energy", "green energy", "renewable electricity", "purchased renewable electricity", "renewable power", "percentage of renewable", "renewable source", "global renewable electricity", "electricity derived from renewable", "energy from biomass"],
    "Total Energy Consumption": ["total energy", "energy consumption", "energy use", "building energy usage", "energy inputs", "fuel and heat", "purchased steam", "natural gas", "purchased fuel", " energy sources consumption"],
    "Energy Intensity": ["energy intensity", "energy per", "energy efficiency"],
    "Electricity Consumption": ["electricity consumption", "power consumption", "electricity use"],
    "Water Consumption": ["total water", "water consumption", "water use", "water consumed", "total water consumed", "water consumed in process", "water consumed total", "water consumed new water", "water consumed underground", "water consumed rainwater", "water consumed concessionaries"],
    "Water Withdrawal": ["total water withdrawal", "water withdrawal", "water usage", "total water usage", "tpww", "surface water", "groundwater", "combination of surface water and groundwater"],
    "Water Intensity": ["water intensity", "water per"],
    "Water Recycled": ["water recycled", "water reused", "recycled water", "reclaimed water", "water reclaimed", "total reclaimed water", "water recycling ratio"],
    "Water Discharge": ["water discharge", "discharged water", "wastewater", "water pollutant", "bod", "cod", "biochemical oxygen demand", "chemical oxygen demand"],
    "Water Management": ["water management strategies", "water management elements", "in-scope facilities"],
    "Water Stress": ["areas with water stress", "water-stressed areas", "high baseline water stress", "extremely high baseline water stress", "regions with high water stress"],
    "Total Waste": ["total waste", "waste generated", "waste production", "solid waste", "waste at the beginning of the reporting year", "waste at the end of the reporting year", "waste eliminated", "waste neutralization", "waste disposed", "waste processed", "waste received from third parties", "waste management", "waste reduction", "waste to energy", "waste sent to landfill", "waste incinerated", "waste landfilled", "waste processed – landfilled", "waste processed – incinerated", "waste processed – other methods"],
    "Waste Recycled": ["waste recycled", "recycled wastes","recycling rate", "waste diverted", "waste diversion rate", "waste processed – recycled", "waste recovery", "waste reused", "waste composted", "waste processed – recovery", "waste processed – recycled", "waste processed – composted"],
    "Hazardous Waste": ["hazardous waste", "toxic waste", "dangerous waste", "waste reduction (hazardous)", "hazardous waste reduction"],
    "Waste Intensity": ["waste intensity", "waste per", "waste generation intensity"],
    "Biodiversity": ["biodiversity", "ecosystem", "habitat", "species"],
    "Air Quality": ["air quality", "air pollutant", "air pollution", "nox", "sox", "particulate", "sulphur oxides", "dust", "air emissions", "hydrocarbons"],
    "Raw Materials": ["raw material", "virgin material", "material consumption"],
    "Packaging": ["packaging", "packaging material"],
    "Total Employees": ["full-Time employees","employees", "total employee", "workforce size", "headcount", "number of employee", "composition of employees", "composition of new hires", "by age group", "by gender"],
    "Employee Turnover": ["turnover", "attrition", "retention rate"],
    "Women Employees": ["women employee", "female employee", "gender diversity", "women by job group", "women by professional category", "percentage of engineers who are women", "percentage of individual contributors who are women", "percentage of production & maintenance workers who are women", "percentage of workforce members who received promotions who are women", "women by professional category administrative", "women by professional category apprenticeship", "women by professional category board", "women by professional category coordination", "women by professional category internship", "women by professional category management", "women by professional category operational", "women by professional category superintendence", "management women", "management who are women", "% management women", "% of management who are women", "% women"],
    "Employee Training": ["training hour", "training program", "employee development", "average hours of training", "training per employee", "training per group", "training per category", "training per gender", "training per management", "training per ethnicity", "average training hours", "hours of learning"],
    "Gender Diversity": ["gender diversity", "women in", "female representation", "by gender"],
    "Board Diversity": ["board diversity", "women on board", "women directors","board gender", "female board directors", "female directors", "ratio of women managers"],
    "Minority Representation": ["minority", "ethnic diversity", "racial diversity", "management minorities", "management who are minorities", "% management minorities", "% of management who are minorities", "% minorities"],
    "Lost Time Injury": ["lost time injury", "lti", "ltifr", "lost time incident"],
    "Recordable Injury": ["recordable injury", "trir", "total recordable", "recordable work-related injuries","recordable injuries"],
    "Fatalities": ["fatality", "fatalities", "death", "fatal accident"],
    "Safety Training": ["safety training", "health and safety training"],
    "Community Investment": ["community investment", "social investment", "charitable"],
    "Volunteering": ["volunteer", "volunteering hour", "community service"],
    "Local Employment": ["local employment", "local hire", "local workforce"],
    "Board Size": ["board size", "number of director", "board member"],
    "Independent Directors": ["independent director", "board independence", "independent Members"],
    "Board Meetings": ["board meeting", "board attendance", "attendance rate", "internal directors", "board of directors meetings"],
    "Board Committees": ["board committee", "audit committee", "compensation committee"],
    "Ethics Training": ["ethics training", "code of conduct training", "compliance training"],
    "Whistleblower Reports": ["whistleblower", "ethics hotline", "compliance report"],
    "Anti-Corruption": ["anti-corruption", "anti-bribery", "corruption"],
    "Data Privacy": ["data privacy", "data protection", "gdpr", "personal data", "data security"],
    "ESG Reporting": ["esg report", "sustainability report", "csr report"],
    "Sustainability Goals": ["sustainability goal", "target", "commitment"],
    "EHS Compliance": ["ehs fines", "environmental fines", "health and safety fines", "ehs inspection", "government inspection", "agency inspection", "notice of violation", "violation notice", "regulatory violation"]
}

@udf("string")
def classify_metric(metric_name, metric_category, topic):
    if not metric_name:
        return "Other"
    metric_lower = metric_name.lower()
    for category, keywords in esg_categories.items():
        for keyword in keywords:
            if keyword in metric_lower:
                return category
    return "Other"

df_classified = df.withColumn("metric_group", classify_metric(col("metric_name"), col("metric_category"), col("topic")))

metric_summary = df_classified.groupBy("metric_group", "topic").agg(
    count("*").alias("record_count"),
    countDistinct("metric_name").alias("unique_metrics"),
    countDistinct("name").alias("companies")
).orderBy(col("record_count").desc())

print(f"\nTotal metric groups: {metric_summary.count()}")
metric_summary.show(100, truncate=False)

top_groups = metric_summary.filter(col("metric_group") != "Other").orderBy(col("record_count").desc()).limit(30).collect()

for i, row in enumerate(top_groups, 1):
    group_name = row['metric_group']
    print(f"\nGROUP {i}: {group_name}")
    print(f"Topic: {row['topic']}")
    print(f"Records: {row['record_count']:,}")
    print(f"Unique metrics: {row['unique_metrics']}")
    print(f"Companies: {row['companies']}")

    group_metrics = df_classified.filter(col("metric_group") == group_name) \
        .select("metric_name").distinct().orderBy("metric_name").limit(15).collect()
    
    print("\nSample metric names:")
    for metric in group_metrics:
        print(f"  - {metric['metric_name']}")

other_metrics = df_classified.filter(col("metric_group") == "Other")
other_count = other_metrics.count()
print(f"\nTotal Other records: {other_count:,}")

if other_count > 0:
    print("\nTop 20 metrics in Other group:")
    other_metrics.groupBy("metric_name", "topic").count().orderBy(col("count").desc()).show(20, truncate=False)

for topic in ["Environmental", "Social", "Governance"]:
    print(f"\n{topic}:")
    df_classified.filter(col("topic") == topic).groupBy("metric_group").agg(
        count("*").alias("records"),
        countDistinct("metric_name").alias("unique_metrics")
    ).orderBy(col("records").desc()).show(20, truncate=False)

extract_date = datetime.now().strftime("%Y-%m-%d")
df_classified = df_classified.withColumn("extract_date", lit(extract_date))

df_classified.write.format("delta").mode("overwrite").partitionBy("year").save(OUTPUT_PATH)

metric_summary_with_samples = df_classified.groupBy("metric_group", "topic").agg(
    count("*").alias("record_count"),
    countDistinct("metric_name").alias("unique_metrics"),
    countDistinct("name").alias("companies"),
    concat_ws("; ", collect_set("metric_name")).alias("sample_metrics")
).orderBy(col("record_count").desc())


total_groups = metric_summary.filter(col("metric_group") != "Other").count()
total_other = metric_summary.filter(col("metric_group") == "Other").select("record_count").collect()
other_count = total_other[0]['record_count'] if total_other else 0

print(f"Total classified groups: {total_groups}")
print(f"Total classified records: {df_classified.filter(col('metric_group') != 'Other').count():,}")
print(f"Total Other records: {other_count:,}")
print(f"Classification coverage: {(1 - other_count/df_classified.count())*100:.2f}%")

print(f"\nSaved to {OUTPUT_PATH}")
print(f"Summary saved to {SUMMARY_PATH}")

spark.stop()