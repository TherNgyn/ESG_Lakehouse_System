from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType
from datetime import datetime
import re

try:
    from sentence_transformers import SentenceTransformer
    from sklearn.metrics.pairwise import cosine_similarity
    import numpy as np
    EMBEDDING_AVAILABLE = True
except ImportError:
    EMBEDDING_AVAILABLE = False
    print("WARNING: sentence-transformers not available, skipping embedding-based normalization")

spark = SparkSession.builder \
    .appName("ESG-Normalize-Metrics") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "admin123456") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

INPUT_PATH = "s3a://silver/classified_metrics"
OUTPUT_PATH = "s3a://silver/normalized_metrics"

df = spark.read.format("delta").load(INPUT_PATH)
df = df.withColumn("metric_name", lower(col("metric_name")))

if EMBEDDING_AVAILABLE:
    model = SentenceTransformer("all-MiniLM-L6-v2")

def embed_mapping(group_df, standard_names, rule_based_fn=None):
    if not EMBEDDING_AVAILABLE:
        return None, None
    
    raw_names = [r[0] for r in group_df.select("metric_name").distinct().collect()]
    if not raw_names:
        return None, None

    embed_raw = model.encode(raw_names)
    embed_std = model.encode(standard_names)
    sim = cosine_similarity(embed_raw, embed_std)
    best_idx = np.argmax(sim, axis=1)
    best_score = np.max(sim, axis=1)

    mapping = []
    for i, raw in enumerate(raw_names):
        chosen_idx = best_idx[i]
        if rule_based_fn:
            chosen_idx = rule_based_fn(raw.lower(), chosen_idx, standard_names, best_score[i])
        mapping.append((raw, standard_names[chosen_idx], float(best_score[i])))
    return raw_names, mapping

def apply_mapping(df, mapping_list, new_col="new_metric_norm"):
    if not mapping_list:
        return df
    
    mapping_df = spark.createDataFrame(mapping_list, ["metric_name", new_col, "similarity"])
    return df.join(mapping_df.select("metric_name", new_col), "metric_name", "left") \
             .withColumn("metric_norm", coalesce(col(new_col), col("metric_norm"))) \
             .drop(new_col)

@udf(StringType())
def normalize_metric_name(metric_name):
    if not metric_name:
        return ""
    n = metric_name.lower()
    if "nox" in n or "nitrogen oxides" in n: 
        return "NOx"
    if "sox" in n or "sulfur oxides" in n: 
        return "SOx"
    if "carbon monoxide" in n or re.search(r'\bco\b', n): 
        return "CO"
    if "nitrogen monoxide" in n or re.search(r'\bno\b', n): 
        return "NO"
    if "particulate matter" in n or "pm" in n: 
        return "PMx"
    return metric_name

df = df.withColumn("metric_norm", normalize_metric_name(col("metric_name")))

if EMBEDDING_AVAILABLE:
    anti_df = df.filter(col("metric_group") == "anti-corruption")
    std_anti = ["Anti-Corruption Training - Employees","Anti-Corruption Training - Management","Anti-Corruption Training - Directors & Senior Management","Anti-Corruption Training - Relevant Employees","Anti-Corruption Training - Number of Trained Employees","Anti-Corruption Training Completion Rate","Corruption Risk Assessment - Sites","Sites Exposed to Major Corruption Risk - Count","Corruption Cases - Reported","Corruption Cases - Resolved","Corruption Cases - Valid","Corruption Cases - Valid and Resolved","Legal & Regulatory Violations - Anti-Corruption","Employees Receiving Anti-Corruption Training - Count"]
    
    def anti_rule(name, idx, std, score):
        if "valid" in name and "resolved" in name and "Corruption Cases - Valid and Resolved" in std: 
            return std.index("Corruption Cases - Valid and Resolved")
        if "resolved" in name: 
            return std.index("Corruption Cases - Resolved")
        if "valid" in name: 
            return std.index("Corruption Cases - Valid")
        if "reported" in name: 
            return std.index("Corruption Cases - Reported")
        if any(w in name for w in ["law","regulation","violation"]): 
            return std.index("Legal & Regulatory Violations - Anti-Corruption")
        if any(w in name for w in ["management","director","senior management"]): 
            return std.index("Anti-Corruption Training - Management")
        if "relevant" in name: 
            return std.index("Anti-Corruption Training - Relevant Employees")
        if ("number" in name or "percentage" in name) and "trained" in name: 
            return std.index("Anti-Corruption Training - Number of Trained Employees")
        if ("employee" in name or "personnel" in name) and "training" in name: 
            return std.index("Anti-Corruption Training - Employees")
        return idx
    
    _, anti_map = embed_mapping(anti_df, std_anti, anti_rule)
    df = apply_mapping(df, anti_map)

df = df.withColumn("metric_full", col("metric_name"))

board_committee = [
    ("audit committee","Audit Committee"),
    ("number of audit committee meetings","Audit Committee Meetings"),
    ("human resources and compensation committee","Human Resources & Compensation Committee"),
    ("audit committee - independent members of the board of directors committees","Independent Members of Audit Committee"),
    ("average board member age","Average Board Member Age"),
    ("average board member tenure","Average Board Member Tenure"),
    ("board members under 30","Board Members Under 30"),
    ("board members aged 30–50","Board Members Aged 30–50"),
    ("board members over 50","Board Members Over 50"),
    ("total number of board members","Total Number of Board Members"),
    ("board representation – underrepresented ethnic/racial groups","Board Representation – Underrepresented Ethnic/Racial Groups")
]

for raw, norm in board_committee:
    df = df.withColumn("metric_norm", when(lower(col("metric_full")).contains(raw.lower()), norm).otherwise(col("metric_norm")))

df = df.withColumn("topic", 
    when((col("topic") == "all indicators") & col("metric_name").contains("community"), "Social")
    .otherwise(col("topic"))
)

df = df.withColumn("metric_norm",
    when(lower(col("metric_name")).contains("data leakage") & lower(col("metric_name")).contains("total number"), "Data Breach – Incident Count")
    .when(lower(col("metric_name")).contains("data leakage") & lower(col("metric_name")).contains("users affected"), "Data Breach – Affected Users")
    .when(lower(col("metric_name")).contains("data leakage") & lower(col("metric_name")).contains("percentage"), "Data Breach – Consumer-related Percentage")
    .when(lower(col("metric_name")).contains("iso certification"), "Information Security Certification – ISO (%)")
    .when(lower(col("metric_name")).contains("isms certification"), "Information Security Certification – ISMS (%)")
    .otherwise(col("metric_norm"))
)

df = df.withColumn("metric_norm",
    when((col("metric_group") == "electricity consumption") & 
         col("metric_name").rlike("(?i)(total electricity consumption|energy - total electricity consumption|electricity consumption)"), 
         "Electricity Consumption – Total")
    .otherwise(col("metric_norm"))
)

df = df.withColumn("metric_norm",
    when((lower(col("metric_group")) == "emission intensity") & 
         lower(col("metric_name")).rlike(r"greenhouse gas emission intensity:\s*(cell|module|pack|others)"), lit(None))
    .when((lower(col("metric_group")) == "emission intensity") & 
          lower(col("metric_name")).rlike(r"^greenhouse gas emission intensity$"), "Greenhouse Gas Emission Intensity – Total")
    .when((lower(col("metric_group")) == "emission intensity") & 
          lower(col("metric_name")).rlike(r"(average carbon intensity|^carbon intensity$|average carbon intensity of our sold energy products)"), 
          "Carbon Intensity – Average")
    .when((lower(col("metric_group")) == "emission intensity") & lower(col("metric_name")).contains("methane"), "Methane Intensity")
    .when((lower(col("metric_group")) == "emission intensity") & lower(col("metric_name")).rlike(r"\bpm\b|\bpmx\b"), "PMx")
    .when((lower(col("metric_group")) == "emission intensity") & lower(col("metric_name")).contains("nox"), "NOx")
    .when((lower(col("metric_group")) == "emission intensity") & lower(col("metric_name")).contains("sox"), "SOx")
    .when((lower(col("metric_group")) == "emission intensity") & lower(col("metric_name")).contains("voc"), "VOC")
    .otherwise(col("metric_norm"))
)

def contains_word(text, word):
    return bool(re.search(r'\b' + re.escape(word) + r'\b', text, re.IGNORECASE))

if EMBEDDING_AVAILABLE:
    turnover_df = df.filter(col("metric_group") == "employee turnover")
    std_turn = ["Employee Turnover Rate – Total","Employee Turnover Rate – Voluntary","Employee Turnover Rate – Involuntary","Employee Turnover Rate – Women","Employee Turnover Rate – Men","Employee Turnover Rate – Age Under 30","Employee Turnover Rate – Age 30-50","Employee Turnover Rate – Age Over 50","Employee Turnover Rate – Northeast","Employee Turnover Rate – Central-West","Employee Turnover Rate – North","Employee Turnover Rate – Southeast","Employee Turnover Rate – South","Employee Turnover Rate – Management","Employee Turnover Rate – Operational","Employee Turnover Rate – Administrative","Employee Turnover Rate – Coordination/Supervision","Employee Turnover Rate – Apprenticeship","Employee Turnover Rate – Superintendence","Employee Turnover Rate – White","Employee Turnover Rate – Asian","Employee Turnover Rate – Black","Employee Turnover Rate – Indigenous","Employee Turnover Rate – Not Declared","Voluntary Turnover Rate – Total","Voluntary Turnover Rate – Women","Voluntary Turnover Rate – Men","Voluntary Turnover Rate – POC","Voluntary Turnover Rate – <5 Years of Tenure","Voluntary Turnover Rate – Age Under 30","Voluntary Turnover Rate – Age 30-50","Voluntary Turnover Rate – Age Over 50","Voluntary Turnover Rate – Management","Voluntary Turnover Rate – Operational","Voluntary Turnover Rate – Administrative","Voluntary Turnover Rate – Coordination/Supervision","Voluntary Turnover Rate – Apprenticeship","Voluntary Turnover Rate – Superintendence","Voluntary Turnover Rate – White","Voluntary Turnover Rate – Asian","Voluntary Turnover Rate – Black","Voluntary Turnover Rate – Indigenous","Voluntary Turnover Rate – Not Declared","Retention Rate","Retention Rate – Women","Retention Rate – Men","Retention Rate – After Parental Leave","Attrition Rate","Voluntary Attrition Rate","Voluntary Attrition Rate – Women","Voluntary Attrition Rate – Men","Voluntary Attrition Rate – POC","Voluntary Attrition Rate – <5 Years of Tenure"]
    
    def turnover_rule(name, idx, std, score):
        if "retention" in name:
            if "parental leave" in name or "returning from" in name:
                return std.index("Retention Rate – After Parental Leave")
            if "women" in name or "female" in name:
                return std.index("Retention Rate – Women")
            if contains_word(name, "men") or "male" in name:
                return std.index("Retention Rate – Men")
            return std.index("Retention Rate")
        
        if "attrition" in name and "voluntary" in name:
            if "women" in name or "female" in name:
                return std.index("Voluntary Attrition Rate – Women")
            if contains_word(name, "men") or "male" in name:
                return std.index("Voluntary Attrition Rate – Men")
            if "poc" in name or "u.s. poc" in name:
                return std.index("Voluntary Attrition Rate – POC")
            if any(p in name for p in ["less than 5", "<5", "under 5"]):
                return std.index("Voluntary Attrition Rate – <5 Years of Tenure")
            return std.index("Voluntary Attrition Rate")
        
        if "attrition" in name:
            return std.index("Attrition Rate")
        
        if "involuntary" in name:
            return std.index("Employee Turnover Rate – Involuntary")
        
        if "voluntary" in name:
            if "function" in name:
                if "management" in name: return std.index("Voluntary Turnover Rate – Management")
                if "operational" in name: return std.index("Voluntary Turnover Rate – Operational")
                if "administrative" in name: return std.index("Voluntary Turnover Rate – Administrative")
                if "coordination" in name or "supervision" in name: return std.index("Voluntary Turnover Rate – Coordination/Supervision")
                if "apprenticeship" in name: return std.index("Voluntary Turnover Rate – Apprenticeship")
                if "superintendence" in name: return std.index("Voluntary Turnover Rate – Superintendence")
            if "age" in name:
                if "under 30" in name or "<30" in name: return std.index("Voluntary Turnover Rate – Age Under 30")
                if "30" in name and "50" in name: return std.index("Voluntary Turnover Rate – Age 30-50")
                if "over 50" in name or ">50" in name: return std.index("Voluntary Turnover Rate – Age Over 50")
            if "color" in name or "ethnicity" in name or "race" in name:
                if "white" in name: return std.index("Voluntary Turnover Rate – White")
                if "asian" in name: return std.index("Voluntary Turnover Rate – Asian")
                if "black" in name: return std.index("Voluntary Turnover Rate – Black")
                if "indigenous" in name: return std.index("Voluntary Turnover Rate – Indigenous")
                if "not declared" in name or "undeclared" in name: return std.index("Voluntary Turnover Rate – Not Declared")
            if "women" in name or "female" in name:
                return std.index("Voluntary Turnover Rate – Women")
            if contains_word(name, "men") or "male" in name:
                return std.index("Voluntary Turnover Rate – Men")
            return std.index("Voluntary Turnover Rate – Total")
        
        if "turnover" in name:
            if "function" in name:
                if "management" in name: return std.index("Employee Turnover Rate – Management")
                if "operational" in name: return std.index("Employee Turnover Rate – Operational")
                if "administrative" in name: return std.index("Employee Turnover Rate – Administrative")
                if "coordination" in name or "supervision" in name: return std.index("Employee Turnover Rate – Coordination/Supervision")
                if "apprenticeship" in name: return std.index("Employee Turnover Rate – Apprenticeship")
                if "superintendence" in name: return std.index("Employee Turnover Rate – Superintendence")
            if "age" in name:
                if "under 30" in name or "<30" in name: return std.index("Employee Turnover Rate – Age Under 30")
                if "30" in name and "50" in name: return std.index("Employee Turnover Rate – Age 30-50")
                if "over 50" in name or ">50" in name: return std.index("Employee Turnover Rate – Age Over 50")
            if "region" in name:
                if "northeast" in name: return std.index("Employee Turnover Rate – Northeast")
                if "central" in name or "west" in name: return std.index("Employee Turnover Rate – Central-West")
                if "north" in name and "east" not in name: return std.index("Employee Turnover Rate – North")
                if "southeast" in name or "south-east" in name: return std.index("Employee Turnover Rate – Southeast")
                if "south" in name and "east" not in name: return std.index("Employee Turnover Rate – South")
            if "color" in name or "ethnicity" in name or "race" in name:
                if "white" in name: return std.index("Employee Turnover Rate – White")
                if "asian" in name: return std.index("Employee Turnover Rate – Asian")
                if "black" in name: return std.index("Employee Turnover Rate – Black")
                if "indigenous" in name: return std.index("Employee Turnover Rate – Indigenous")
                if "not declared" in name or "undeclared" in name: return std.index("Employee Turnover Rate – Not Declared")
            if "women" in name or "female" in name:
                return std.index("Employee Turnover Rate – Women")
            if contains_word(name, "men") or "male" in name:
                return std.index("Employee Turnover Rate – Men")
            return std.index("Employee Turnover Rate – Total")
        
        return idx
    
    _, turn_map = embed_mapping(turnover_df, std_turn, turnover_rule)
    df = apply_mapping(df, turn_map, "standard_name")

if EMBEDDING_AVAILABLE:
    groups = [
        ("biogenic emissions", ["Biogenic Emissions – Total","Biogenic Emissions – Purchased Steam Includes Renewable Sources","Biogenic Emissions – By Energy Type","Biogenic Emissions – CO2 (Metric Tons)","Biogenic Emissions – CH4 (Metric Tons)","Biogenic Emissions – N2O (Metric Tons)"]),
        ("employee training", ["Average Training Hours per Employee – Total","Total Training Hours","Average Training Hours per Employee – Female","Average Training Hours per Employee – Male","Average Training Hours per Employee – Under 30","Average Training Hours per Employee – 30-50","Average Training Hours per Employee – Over 50","Average Training Hours per Employee – Front-line Employee","Average Training Hours per Employee – Front-line Management","Average Training Hours per Employee – Middle Management","Average Training Hours per Employee – Senior Management","Number of Participants – Management Training (Leaders)","Number of Participants – Management Training (Managers)"]),
        ("ehs compliance", ["EHS Fines","EHS Inspections","Fair Trade Violations","Information Security Violations","Marketing & Labeling Violations"]),
        ("gender diversity", ["Female Representation – Overall (%)","Female Representation – Total Workforce (#)","Female Representation – Board of Directors","Female Representation – Executive Council","Female Representation – Executives","Female Representation – Senior Management","Female Representation – Management","Female Representation – Managers","Female Representation – Management and Professional Staff","Female Representation – New Hires (#)","Female Representation – New Hires (%)","Women in Enterprise","Women in Leadership"]),
        ("hazardous waste", ["Total Hazardous Waste","Total Non-Hazardous Waste","Hazardous Waste – Disposal/Directed to Disposal","Hazardous Waste – Incineration without Energy Recovery","Hazardous Waste – Incineration with Energy Recovery","Hazardous Waste – Landfill","Hazardous Waste – Other Disposal Methods","Hazardous Waste – Reused","Hazardous Waste – Recovered in Other Ways","Hazardous Waste – Total Reused and Recycled","Non-Hazardous Waste – Disposal/Directed to Disposal","Non-Hazardous Waste – Incineration without Energy Recovery","Non-Hazardous Waste – Incineration with Energy Recovery","Non-Hazardous Waste – Landfill","Non-Hazardous Waste – Other Disposal Methods"]),
        ("independent directors", ["Board Independence","Independent Directors – Representation","Number of Independent Directors","Percentage of Independent Directors on Board","Percentage of Independent Directors in Audit Committee","Attendance Rate – Independent Directors at Board Meetings","Attendance Rate – Independent Directors at Audit Committee","Attendance Rate – Independent Directors at Compensation Committee","Attendance Rate – Independent Directors at Candidate Recommendation Committee","Number of Agenda Items Objected/Amended by Independent Directors","Limitations on Other Jobs for Independent Directors","Total Compensation for Independent Directors","Percentage of Non-Independent Directors with Anti-Corruption Training"]),
        ("lost time injury", ["Lost Time Injury Rate (LTIR) – Total","Lost Time Injury Rate (LTIR) – Employees Only","Lost Time Injury Rate (LTIR) – Contractors","Lost Time Injury Rate (LTIR) – Combined","Lost Time Injury Rate (LTIR) – U.S. Only","Lost Time Injury Rate (LTIR) – Worldwide","Lost Time Injury Rate (LTIR) – U.S. Courier and Express Delivery","Lost Time Injury Rate (LTIR) – U.S. General Warehousing","Lost Time Injury Frequency Rate (LTIFR)","Number of Lost-Time Injuries (LTI)","Number of Lost-Time Injuries Including Microtraumas","Number of Microtrauma Cases","Accidents at Work","Deaths Resulting from Accidents at Work","Environmental Fines or Penalties"]),
        ("scope 3 emissions", ["Scope 3 Emissions – Total","Scope 3 Emissions Intensity","Scope 3 Emissions Reduction from Initiatives","Scope 3 GHG Emissions – Total","Scope 3 GHG Emissions – Biogenic","Scope 3 Emissions – Category 1: Purchased Goods and Services","Scope 3 Emissions – Category 2: Capital Goods","Scope 3 Emissions – Category 3: Fuel and Energy-Related Activities","Scope 3 Emissions – Category 4: Upstream Transportation and Distribution","Scope 3 Emissions – Category 5: Waste Generated in Operations","Scope 3 Emissions – Category 6: Business Travel","Scope 3 Emissions – Category 7: Employee Commuting","Scope 3 Emissions – Category 8: Upstream Leased Assets","Scope 3 Emissions – Category 9: Downstream Transportation and Distribution","Scope 3 Emissions – Category 11: Use of Sold Products","Scope 3 Emissions – Category 12: End-of-Life Treatment of Sold Products","Scope 3 Emissions – Category 13: Downstream Leased Assets","Scope 3 Emissions – Category 14: Franchises","Scope 3 Emissions – Category 15: Investments","Scope 3 Emissions – Use of Sold Products (Construction Industries)","Scope 3 Emissions – Use of Sold Products (Energy & Transportation)","Scope 3 Emissions – Use of Sold Products (Resource Industries)","Scope 3 Emissions – Use of Sold Products (Enterprise)","Scope 3 Emissions – Financed Emissions","Scope 3 Emissions – Upstream Oil and Gas Production","Scope 3 Target-Based Intensity"]),
        ("total ghg emissions", ["Total Greenhouse Gas Emissions","Total GHG Emissions – Location-Based","Total GHG Emissions – Market-Based","Total GHG Emissions – Net Equity","Total GHG Emissions – By Geography (EMEA)","Total GHG Emissions – By Geography (AMER)","Total GHG Emissions – By Geography (APAC)","Total GHG Emissions – Fleet","Total GHG Emissions – Natural Gas","Total GHG Emissions Intensity"]),
        ("total waste", ["Total Waste Generated","Total Hazardous Waste Generated","Total Non-Hazardous Waste Generated","Total Waste Directed to Disposal","Total Waste Diverted from Disposal","Hazardous Waste – Landfill","Hazardous Waste – Incineration with Energy Recovery","Hazardous Waste – Incineration without Energy Recovery","Hazardous Waste – Other Disposal","Hazardous Waste – Reduction","Non-Hazardous Waste – Landfill","Non-Hazardous Waste – Composting","Non-Hazardous Waste – Recycling","Non-Hazardous Waste – Incineration with Energy Recovery","Non-Hazardous Waste – Incineration without Energy Recovery","Total Waste – Landfill","Total Waste – Recycling","Total Waste – Composting","Total Waste – Incineration with Energy Recovery","Total Waste – Incineration without Energy Recovery","Total Waste – Reused","Total Waste – Otherwise Disposed","Total Waste – Recovered (Other Ways)","Total Waste – Neutralization and Disposal","Total Waste – Utilized","Total Waste – Recovery","Waste – Reduction","Waste – To Energy","Waste – Recycled/Reused/Composted Percentage","Waste – Landfilled at Own Facilities Percentage","Waste – By Geography (Russian Entities)","Waste – By Geography (Foreign Entities)","Waste – By Hazard Class and Geography","Waste – By Source (Third Parties)","Waste – Transferred to Third Parties","Waste – Inventory (Beginning)","Waste – Inventory (End)","Waste – Eliminated","Waste – Disposed","General Industrial Solid Waste – Total","General Industrial Solid Waste – Recycled","General Industrial Solid Waste – Landfill","General Industrial Solid Waste – Disposed","General Industrial Solid Waste – Incineration with Energy Recovery","General Industrial Solid Waste – Reused","General Industrial Solid Waste – Recovered (Other Ways)","Waste Generated in Operations – Emissions"]),
        ("total employees", ["Total Employees","New Employee Hires – Total","Employee Turnover – Total","Average Training Hours – Total","Employee Training Coverage – Total (%)","Gender Pay Ratio","Employees Entitled to Parental Leave – Total","Employees Who Took Parental Leave – Total","Employees Still Employed 12 Months After Parental Leave Return – Total","Number of Employee Fatalities"]),
        ("volunteering", ["Total Volunteer Hours","Number of Employee Volunteers","Occupational Disease Cases"]),
        ("waste recycled", ["Total Waste Recycled","Hazardous Waste Recycled","Non-Hazardous Waste Recycled","Solid Waste Recycled Reused and Composted","Percentage of Hazardous Waste Recycled","Percentage of Non-Hazardous Waste Recycled","Percentage of Solid Waste Recycled Reused and Composted","Water Recycling Rate"]),
        ("water consumption", ["Total Water Withdrawal","Total Water Consumption","Total Water Usage","Total Water Discharge","Total Freshwater Withdrawal","Total Freshwater Consumption","Water Withdrawal – Groundwater","Water Withdrawal – Surface Water","Water Withdrawal – Municipal Water","Water Withdrawal – Third-Party Water","Water Withdrawal – Seawater","Water Withdrawal – Rainwater","Water Withdrawal – Recycled Water","Water Withdrawal – Other Sources","Water Consumption – Groundwater","Water Consumption – Surface Water","Water Consumption – Municipal Water","Water Consumption – Rainwater","Water Consumption – Recycled Water","Water Consumption – New Water","Water Consumption – In Process","Water Consumption – Concessionaries","Total Water Withdrawal – Water-Stressed Areas","Total Water Consumption – Water-Stressed Areas","Percentage of Water Withdrawal in Water-Stressed Areas","Percentage of Water Consumption in Water-Stressed Areas","Water Consumption Intensity","Conventional Freshwater Consumption","Unconventional Freshwater Consumption","Water Use Reduction"]),
        ("water discharge", ["Total Water Discharge","Total Water Discharge – Water-Stressed Areas","Produced Water Discharged Offshore"]),
        ("water recycled", ["Total Water Recycled and Reused","Produced Water Recycled or Reused","Municipal Wastewater Reused","Third-Party Recycled Water"]),
        ("women employees", ["Total Female Employees","Total Male Employees","Percentage of Female Employees","Percentage of Male Employees"])
    ]
    
    for group, std_names in groups:
        gdf = df.filter(col("metric_group") == group)
        _, m = embed_mapping(gdf, std_names)
        df = apply_mapping(df, m)

df = df.withColumn("metric_norm",
    when((col("metric_group") == "fatalities") & lower(col("metric_name")).contains("employees and contractors"), "Employee & Contractor Fatalities")
    .when((col("metric_group") == "fatalities") & lower(col("metric_name")).rlike("fatalities contractors|contractor fatalities"), "Contractor Fatalities")
    .when((col("metric_group") == "fatalities") & lower(col("metric_name")).rlike("employee fatalities|fatalities – employees"), "Employee Fatalities")
    .when((col("metric_group") == "fatalities") & lower(col("metric_name")).contains("workforce fatalities"), "Workforce Fatalities")
    .when((col("metric_group") == "fatalities") & lower(col("metric_name")).contains("high-consequence") & lower(col("metric_name")).contains("injuries") & lower(col("metric_name")).contains("incident count"), "High-Consequences Work-Related Injuries Count (Excl. Fatalities)")
    .when((col("metric_group") == "fatalities") & lower(col("metric_name")).contains("high-consequence") & lower(col("metric_name")).contains("injuries") & lower(col("metric_name")).contains("incident rate"), "High-Consequences Work-Related Injuries Rate (Excl. Fatalities)")
    .when((col("metric_group") == "fatalities") & lower(col("metric_name")).rlike("high-consequence|serious consequences"), "High-Consequences Work-Related Injuries (Excl. Fatalities)")
    .when((col("metric_group") == "fatalities") & lower(col("metric_name")).rlike("number of work-related fatalities|number of fatalities|occupational disease deaths|fatalities"), "Work-Related Fatalities")
    .otherwise(col("metric_norm"))
)

df = df.withColumn("metric_norm", coalesce(col("metric_norm"), col("metric_name")))

for c in ["name","topic","metric_category","category_group","category_detail","metric_group","units"]:
    if c in df.columns:
        df = df.withColumn(c, initcap(col(c)))

if "source" in df.columns:
    df = df.withColumn("source", lower(col("source")))

extract_date = datetime.now().strftime("%Y-%m-%d")
df = df.withColumn("extract_date", lit(extract_date))

df = df.filter(col("year").isNotNull())

df.show(10, truncate=False)

df.write.format("delta").mode("overwrite").partitionBy("year").save(OUTPUT_PATH)

print(f"Normalized metrics saved to {OUTPUT_PATH}")

spark.stop()