from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import re

spark = SparkSession.builder \
    .appName("Silver: Staging Units") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "admin123456") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

print("Loading normalized metrics...")

df = spark.read.format("delta").load("s3a://silver/classified_metrics")

total_records = df.count()
print(f"Total records: {total_records}")

units_list = df.select("units").filter(col("units").isNotNull()).distinct().rdd.map(lambda x: x[0]).collect()

units_list = [u for u in units_list if 
    not re.match(r'^\d+$', u.strip()) and 
    not re.search(r'\(-?\d+\)', u) and
    not re.match(r'^-?\d+$', u.strip())]

print(f"Distinct units found after filtering: {len(units_list)}")

def normalize_unit_text(unit):
    unit = unit.strip()
    unit = re.sub(r'\bAnd\b', 'and', unit, flags=re.IGNORECASE)
    unit = unit.replace(' Per ', '/').replace(' per ', '/').replace('Per ', '/').replace('per ', '/')
    unit = unit.replace('Hour', 'hours').replace('hour', 'hours')
    unit = unit.replace('Number', '#').replace('number', '#')
    unit = unit.replace('No.', '#').replace('no.', '#')
    unit = unit.replace('Millions', 'million').replace('millions', 'million')
    return unit

def extract_financial_shorthand(unit_lower):
    if "$m" in unit_lower and "mm" not in unit_lower and "million" not in unit_lower:
        return 1000000.0
    elif "$ (in mm)" in unit_lower or "$mm" in unit_lower:
        return 1000000.0
    elif "$b" in unit_lower and "billion" not in unit_lower:
        return 1000000000.0
    elif "krw 100m" in unit_lower:
        return 100000000.0
    elif "krw 1m" in unit_lower and "100m" not in unit_lower:
        return 1000000.0
    return None

def extract_prefix(unit_lower):
    shorthand = extract_financial_shorthand(unit_lower)
    if shorthand:
        return shorthand
    
    if "trillion" in unit_lower:
        return 1000000000000.0
    elif "billion" in unit_lower or " bn " in unit_lower or unit_lower.endswith(" bn"):
        return 1000000000.0
    elif "million" in unit_lower or " mln " in unit_lower:
        return 1000000.0
    elif "'000s" in unit_lower or "000s" in unit_lower:
        return 1000.0
    elif "thousand" in unit_lower or " k " in unit_lower or unit_lower.endswith(" k"):
        return 1000.0
    elif "10 thousand" in unit_lower:
        return 10000.0
    else:
        return 1.0

def standardize_unit(unit):
    if not unit:
        return ("unknown", 1.0, "unknown", None)
    
    unit = normalize_unit_text(unit)
    unit_lower = unit.lower().strip()
    
    if "|" in unit_lower:
        return (None, 1.0, "invalid", "Contains pipe character")
    
    if "/" in unit_lower or "//" in unit_lower:
        return (unit_lower, 1.0, "intensity", None)
    
    prefix = extract_prefix(unit_lower)
    
    if "mmt" in unit_lower and "co2" in unit_lower:
        return ("tco2e", 1000000.0, "emissions", "Million Metric Tonnes CO2")
    
    elif ("mtco2" in unit_lower) and "mmt" not in unit_lower and "metric ton" not in unit_lower:
        return ("tco2e", 1000000.0, "emissions", "Million Tonnes CO2")
    
    elif any(k in unit_lower for k in ["ktco2", "kt-co2", "kt co2", "kt_co2"]):
        return ("tco2e", 1000.0, "emissions", "Kilotonne CO2")
    
    elif any(k in unit_lower for k in ["ktons co2", "kton co2"]):
        return ("tco2e", 1000.0, "emissions", "Kilotonne CO2e")
    
    elif "1000 metric tons co2" in unit_lower or "1,000 metric tons co2" in unit_lower:
        return ("tco2e", 1000.0, "emissions", "1000 Metric Tonnes CO2")
    
    elif any(k in unit_lower for k in ["tco2e", "tco₂e", "co2e", "co2 equivalent"]):
        return ("tco2e", prefix, "emissions", "Direct CO2e")
    
    elif any(k in unit_lower for k in ["co2", "carbon dioxide"]) and "co2e" not in unit_lower and "kt" not in unit_lower and "mt" not in unit_lower:
        return ("tco2e", prefix * 1.0, "emissions", "CO2 (GWP=1)")
    
    elif "ch4" in unit_lower or "methane" in unit_lower:
        gwp_ch4 = 28.0
        return ("tco2e", prefix * gwp_ch4, "emissions", f"CH4 (GWP={gwp_ch4})")
    
    elif "n2o" in unit_lower or "nitrous oxide" in unit_lower:
        gwp_n2o = 265.0
        return ("tco2e", prefix * gwp_n2o, "emissions", f"N2O (GWP={gwp_n2o})")
    
    elif "and 3 emissions" in unit_lower:
        return (None, 1.0, "invalid", "Malformed unit")
    
    elif any(k in unit_lower for k in ["gwh", "gigawatt-hours", "gigawatt hours"]) and "/" not in unit_lower:
        return ("gwh", prefix, "energy", None)
    
    elif any(k in unit_lower for k in ["mwh", "megawatt-hours", "megawatt hours"]) and "/" not in unit_lower:
        return ("gwh", prefix * 0.001, "energy", None)
    
    elif any(k in unit_lower for k in ["megawatts", "megawatt", " mw "]) and "/" not in unit_lower and (unit_lower.endswith(" mw") or "megawatt" in unit_lower or unit_lower.strip() == "mw"):
        return ("mw", prefix, "power", None)
    
    elif any(k in unit_lower for k in ["kwh", "kilowatt-hours", "kilowatt hours"]) and "/" not in unit_lower:
        return ("gwh", prefix * 0.000001, "energy", None)
    
    elif ("terajoules" in unit_lower or " tj " in unit_lower or unit_lower.endswith(" tj") or unit_lower.strip() == "tj") and "/" not in unit_lower:
        if "million" in unit_lower:
            return ("gj", 1000000.0 * 1000.0, "energy", "Million Terajoules")
        else:
            return ("gj", prefix * 1000.0, "energy", None)
    
    elif ("petajoules" in unit_lower or " pj " in unit_lower or unit_lower.endswith(" pj") or unit_lower.strip() == "pj") and "/" not in unit_lower:
        return ("gj", prefix * 1000000.0, "energy", None)
    
    elif ("gigajoules" in unit_lower or " gj " in unit_lower or unit_lower.endswith(" gj") or unit_lower.strip() == "gj") and "/" not in unit_lower:
        if "million" in unit_lower:
            return ("gj", 1000000.0, "energy", "Million Gigajoules")
        elif "1000" in unit_lower or "1,000" in unit_lower:
            return ("gj", 1000.0, "energy", "1000 Gigajoules")
        else:
            return ("gj", prefix, "energy", None)
    
    elif "btu" in unit_lower and "/" not in unit_lower:
        gj_per_btu = 0.0000010551
        return ("gj", prefix * gj_per_btu, "energy", None)
    
    elif any(k in unit_lower for k in ["cubic meter", "m3", "m³"]):
        return ("m3", prefix, "volume", None)
    
    elif any(k in unit_lower for k in ["megaliter", "ml"]) and "million" not in unit_lower:
        return ("m3", prefix * 1000.0, "volume", None)
    
    elif any(k in unit_lower for k in ["liter", "litre"]) and "mega" not in unit_lower and "kilo" not in unit_lower:
        return ("m3", prefix * 0.001, "volume", None)
    
    elif "kiloliter" in unit_lower or unit_lower.strip() == "kl":
        return ("m3", prefix, "volume", None)
    
    elif "ton(m3)" in unit_lower or "ton (m3)" in unit_lower:
        return (unit_lower, 1.0, "volume_mass_mixed", "Requires density conversion")
    
    elif " kt " in unit_lower or unit_lower.endswith(" kt") or unit_lower.startswith("kt") or "kilotonne" in unit_lower:
        return ("tonne", 1000.0, "mass", "Kilotonne")
    
    elif unit_lower.strip() == "t" or (unit_lower.endswith(" t") and "co2" not in unit_lower and "/" not in unit_lower):
        return ("tonne", prefix, "mass", "Tonne")
    
    elif unit_lower.strip() == "mt" or " mt " in unit_lower or unit_lower.endswith(" mt"):
        return ("tonne", prefix, "mass", "MT")
    
    elif any(k in unit_lower for k in ["tonne", "ton", "metric ton"]) and "/" not in unit_lower and "co2" not in unit_lower and " kt" not in unit_lower and unit_lower.strip() != "t" and unit_lower.strip() != "mt":
        return ("tonne", prefix, "mass", None)
    
    elif "short ton" in unit_lower and "/" not in unit_lower:
        short_ton_to_metric = 0.907185
        return ("tonne", prefix * short_ton_to_metric, "mass", None)
    
    elif "acres" in unit_lower or "acre" in unit_lower:
        return ("acres", prefix, "area", None)
    
    elif any(k in unit_lower for k in ["percent", "%"]):
        return ("percent", 1.0, "ratio", None)
    
    elif any(k in unit_lower for k in ["employee", "worker", "staff", "headcount"]):
        return ("count", prefix, "social_hr", "Total employees")
    
    elif any(k in unit_lower for k in ["hire", "recruitment"]):
        return ("count", prefix, "social_hr", "Total hires")
    
    elif "female" in unit_lower and "%" in unit_lower:
        return ("percent", 1.0, "social_hr", "Female percentage")
    
    elif "million devices" in unit_lower:
        return ("count", 1000000.0, "count", "Million devices")
    
    elif any(k in unit_lower for k in ["device", "unit", "#"]) and "/" not in unit_lower:
        return ("count", prefix, "count", None)
    
    elif "year" in unit_lower or "years" in unit_lower:
        return ("year", 1.0, "time", None)
    
    elif "hours" in unit_lower or "hour" in unit_lower:
        return ("hours", 1.0, "time", None)
    
    elif "usd" in unit_lower or ("$" in unit_lower and "r$" not in unit_lower):
        return ("usd", prefix, "currency", None)
    
    elif "brl" in unit_lower or "r$" in unit_lower:
        return ("brl", prefix, "currency", None)
    
    elif "eur" in unit_lower or "euro" in unit_lower or "€" in unit_lower:
        return ("eur", prefix, "currency", None)
    
    elif "gbp" in unit_lower or "pound" in unit_lower or "£" in unit_lower:
        return ("gbp", prefix, "currency", None)
    
    elif "krw" in unit_lower or "won" in unit_lower:
        return ("krw", prefix, "currency", None)
    
    elif "rmb" in unit_lower or "cny" in unit_lower or "yuan" in unit_lower:
        if "10 thousand" in unit_lower:
            return ("cny", 10000.0, "currency", "RMB 10 Thousand")
        elif "thousand" in unit_lower:
            return ("cny", 1000.0, "currency", "RMB Thousand")
        elif "million" in unit_lower:
            return ("cny", 1000000.0, "currency", "RMB Million")
        else:
            return ("cny", prefix, "currency", None)
    
    elif "jpy" in unit_lower or "yen" in unit_lower or "¥" in unit_lower:
        return ("jpy", prefix, "currency", None)
    
    elif "vnd" in unit_lower or "dong" in unit_lower:
        return ("vnd", prefix, "currency", None)
    
    else:
        return (unit_lower, 1.0, "other", "Needs manual classification")

unit_standardization = []

for unit in units_list:
    standard_unit, conversion, category, note = standardize_unit(unit)
    if standard_unit is not None:
        unit_standardization.append((unit, standard_unit, conversion, category, note))

schema = StructType([
    StructField("original_unit", StringType(), True),
    StructField("standard_unit", StringType(), True),
    StructField("conversion_factor", DoubleType(), False),
    StructField("unit_category", StringType(), True),
    StructField("note", StringType(), True)
])

staging_units = spark.createDataFrame(unit_standardization, schema=schema)

# THỐNG NHẤT ID: UNT-00001
windowUnits = Window.orderBy("original_unit")
staging_units = staging_units.withColumn("staging_unit_id", 
    concat(lit("UNT-"), lpad(row_number().over(windowUnits).cast("string"), 5, "0")))

print("\nUnit categories summary:")
staging_units.groupBy("unit_category").count().orderBy(desc("count")).show(50, truncate=False)

print("\nKilotonne (Kt) units:")
staging_units.filter(
    col("note").contains("Kilotonne") | col("original_unit").contains("Kt")
).select("original_unit", "standard_unit", "conversion_factor", "note").show(100, truncate=False)

print("\nMillion tonne (Mt/Mmt) units:")
staging_units.filter(
    col("note").contains("Million") & col("unit_category").isin("emissions", "energy", "mass")
).select("original_unit", "standard_unit", "conversion_factor", "note").show(100, truncate=False)

print("\nRMB/CNY currency units:")
staging_units.filter(col("standard_unit") == "cny").select(
    "original_unit", "standard_unit", "conversion_factor", "note"
).show(100, truncate=False)

print("\nEnergy units (GJ/TJ):")
staging_units.filter(
    (col("standard_unit") == "gj") | (col("original_unit").contains("joule"))
).select("original_unit", "standard_unit", "conversion_factor", "note").orderBy(desc("conversion_factor")).show(100, truncate=False)

print("\nEmissions with prefixes:")
staging_units.filter(col("unit_category") == "emissions").select(
    "original_unit", "standard_unit", "conversion_factor", "note"
).orderBy(desc("conversion_factor")).show(100, truncate=False)

print("\nIntensity units:")
staging_units.filter(col("unit_category") == "intensity").select(
    "original_unit", "standard_unit", "conversion_factor"
).show(100, truncate=False)

print("\nCurrency units:")
staging_units.filter(col("unit_category") == "currency").select(
    "original_unit", "standard_unit", "conversion_factor", "note"
).orderBy("standard_unit", desc("conversion_factor")).show(100, truncate=False)

print("\nOther/Unknown units:")
staging_units.filter(col("unit_category") == "other").select(
    "original_unit", "standard_unit", "conversion_factor", "note"
).show(100, truncate=False)

print("\nSaving staging units table...")

staging_units.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("s3a://silver/staging_units")

print("Successfully saved staging units")

print("\nApplying unit standardization to metrics...")

null_units_count = df.filter(col("units").isNull()).count()
if null_units_count > 0:
    print(f"WARNING: {null_units_count} records have NULL units")

df_with_units = df.join(
    staging_units.select("original_unit", "standard_unit", "conversion_factor", "unit_category", "note", "staging_unit_id"),
    df.units == staging_units.original_unit,
    "left"
)

df_with_units = df_with_units.withColumn(
    "value_normalized",
    when(col("unit_category") == "intensity", col("value"))
    .when(col("unit_category") == "volume_mass_mixed", lit(None))
    .otherwise(col("value") * coalesce(col("conversion_factor"), lit(1.0)))
)

missing_units = df_with_units.filter(col("standard_unit").isNull()).select("units").distinct().count()
if missing_units > 0:
    print(f"\nWARNING: {missing_units} units not found in staging_units")
    df_with_units.filter(col("standard_unit").isNull()).select("units").distinct().show(50, truncate=False)

spark.stop()