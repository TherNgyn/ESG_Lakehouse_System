# POWER BI - COMPLETE IMPLEMENTATION GUIDE

## 📊 TABLE OF CONTENTS
1. Enhanced API Endpoints
2. Connection Methods
3. Table Specifications for Power BI
4. Complete Dashboard Designs
5. DAX Measures Library
6. Deployment Steps

---

## 1️⃣ ENHANCED API ENDPOINTS

### **New Power BI Optimized Endpoints:**

```
✅ /api/powerbi/companies               → Full company master
✅ /api/powerbi/esg-scores              → ESG scores with company join
✅ /api/powerbi/esg-metrics             → Metrics with all dimensions
✅ /api/powerbi/aggregated/company-scores     → Pre-aggregated KPIs
✅ /api/powerbi/aggregated/sector-benchmarks  → Sector averages
✅ /api/powerbi/aggregated/metric-summary     → Metric coverage
✅ /api/powerbi/time-series/esg-trend         → Time series data
✅ /api/powerbi/filters/*                     → Dynamic filters
```

### **Why These Endpoints?**
- ✅ Pre-joined with dimensions (no complex Power BI relationships needed)
- ✅ Optimized column names (no underscores, PascalCase)
- ✅ Filtered data (reduce data volume)
- ✅ Aggregated views (fast dashboard loading)

---

## 2️⃣ CONNECTION METHODS

### **Method A: Direct Trino Connection (BEST PERFORMANCE)**

#### **Setup ODBC:**

1. Download Trino ODBC Driver:
   ```
   https://trino.io/download.html
   ```

2. Install & Configure DSN:
   ```
   DSN Name: ESG_Trino_PowerBI
   Host: localhost
   Port: 8080
   Catalog: delta
   Schema: default_marts
   Authentication: None
   ```

3. Power BI Connection:
   ```
   Home → Get Data → ODBC → ESG_Trino_PowerBI
   ```

#### **Import These Tables:**

```sql
-- Dimension Tables (Import Mode)
✅ dim_company          → Full table (150 rows)
✅ dim_metric           → Full table (500 rows)
✅ dim_unit             → Full table (100 rows)
✅ dim_date             → Year 2000-2030

-- Fact Tables (DirectQuery or Import)
✅ fact_esg_score_risk  → Import with filter: year >= 2020
✅ fact_esg_metric      → DirectQuery (large table)
```

---

### **Method B: Enhanced API (EASIEST FOR BEGINNERS)**

#### **Step 1: Deploy Enhanced API**

```bash
# Copy new API
cp api_server_powerbi_enhanced.py metricflow/api_server.py

# Rebuild container
docker-compose build metricflow
docker-compose up -d metricflow

# Test
curl http://localhost:8083/health
```

#### **Step 2: Power BI Web Connections**

**Query 1: Companies**
```
URL: http://localhost:8083/api/powerbi/companies
Method: GET
```

**Query 2: ESG Scores**
```
URL: http://localhost:8083/api/powerbi/esg-scores
Method: POST
Body:
{
  "year": 2024
}
```

**Query 3: Aggregated Company Scores** (RECOMMENDED)
```
URL: http://localhost:8083/api/powerbi/aggregated/company-scores
Method: GET
```

**Query 4: Sector Benchmarks**
```
URL: http://localhost:8083/api/powerbi/aggregated/sector-benchmarks
Method: GET
```

---

## 3️⃣ TABLE SPECIFICATIONS FOR POWER BI

### **📊 Table 1: Companies (Dimension)**

```
Source: /api/powerbi/companies OR dim_company

Power BI Table Name: Companies
Load Mode: Import
Rows: ~150

Columns:
┌─────────────────────┬──────────┬────────────────────────────┐
│ Column              │ Type     │ Use                        │
├─────────────────────┼──────────┼────────────────────────────┤
│ CompanyKey          │ Text     │ Primary Key                │
│ CompanyName         │ Text     │ Display name, Slicer       │
│ StockSymbol         │ Text     │ Filter, Display            │
│ ISIN                │ Text     │ Filter, Unique ID          │
│ Sector              │ Text     │ Slicer, Group By           │
│ Industry            │ Text     │ Slicer, Group By           │
│ SubIndustry         │ Text     │ Drill-down                 │
│ City                │ Text     │ Filter                     │
│ Country             │ Text     │ Slicer, Map Visual         │
│ Region              │ Text     │ Slicer, Group By           │
│ SectorNormalized    │ Text     │ Relationships              │
│ IndustryNormalized  │ Text     │ Relationships              │
│ CountryNormalized   │ Text     │ Relationships              │
│ HasValidISIN        │ Boolean  │ Filter, KPI                │
└─────────────────────┴──────────┴────────────────────────────┘

Relationships:
→ ESGScores[CompanyKey]
→ ESGMetrics[CompanyKey]

Hierarchies to Create:
1. Geographic: Region → Country → City
2. Business: Sector → Industry → SubIndustry
```

---

### **📊 Table 2: ESG Scores (Fact)**

```
Source: /api/powerbi/esg-scores OR fact_esg_score_risk joined with dim_company

Power BI Table Name: ESGScores
Load Mode: Import (with year >= 2020 filter)
Rows: ~1,500

Columns:
┌─────────────────────┬──────────┬────────────────────────────┐
│ Column              │ Type     │ Use                        │
├─────────────────────┼──────────┼────────────────────────────┤
│ ScoreKey            │ Text     │ Primary Key                │
│ CompanyKey          │ Text     │ Foreign Key → Companies    │
│ CompanyName         │ Text     │ Display (denormalized)     │
│ Sector              │ Text     │ Group By (denormalized)    │
│ Industry            │ Text     │ Group By (denormalized)    │
│ Country             │ Text     │ Filter (denormalized)      │
│ Year                │ Integer  │ Time filter                │
│ DataSource          │ Text     │ Source filter              │
│ OverallScore        │ Decimal  │ Main KPI (0-100)           │
│ ESGPulse            │ Decimal  │ Secondary KPI              │
│ TotalLevel          │ Text     │ Risk level (high/med/low)  │
│ TotalGrade          │ Text     │ Grade (A+/A/B/C/D)         │
│ TotalRiskScore      │ Decimal  │ Risk metric                │
│ RiskLevel           │ Text     │ Slicer, Conditional Format │
│ RiskPercentile      │ Text     │ Benchmark                  │
│ ControversyScore    │ Decimal  │ Alert metric               │
│ ControversyLevel    │ Text     │ Alert category             │
└─────────────────────┴──────────┴────────────────────────────┘

Relationships:
→ Companies[CompanyKey] = ESGScores[CompanyKey]

Measures to Create:
- Avg ESG Score = AVERAGE(ESGScores[OverallScore])
- High Risk % = DIVIDE(COUNTROWS(FILTER(ESGScores, ESGScores[RiskLevel]="high")), COUNTROWS(ESGScores))
```

---

### **📊 Table 3: ESG Metrics (Fact)**

```
Source: /api/powerbi/esg-metrics OR fact_esg_metric with joins

Power BI Table Name: ESGMetrics
Load Mode: DirectQuery (large) OR Import with filters
Rows: ~50,000+

Columns:
┌─────────────────────┬──────────┬────────────────────────────┐
│ Column              │ Type     │ Use                        │
├─────────────────────┼──────────┼────────────────────────────┤
│ MetricKey           │ Text     │ Primary Key                │
│ CompanyKey          │ Text     │ Foreign Key                │
│ CompanyName         │ Text     │ Display                    │
│ Sector              │ Text     │ Group By                   │
│ Industry            │ Text     │ Group By                   │
│ MetricKey_Dim       │ Text     │ Metric ID                  │
│ MetricName          │ Text     │ Display, Slicer            │
│ MetricGroup         │ Text     │ Slicer, Group By           │
│ Topic               │ Text     │ Slicer (E/S/G)             │
│ UnitKey             │ Text     │ Unit ID                    │
│ OriginalUnit        │ Text     │ Display                    │
│ StandardUnit        │ Text     │ Display                    │
│ Year                │ Integer  │ Time filter                │
│ OriginalValue       │ Decimal  │ Raw value                  │
│ NormalizedValue     │ Decimal  │ Standardized value (USE)   │
└─────────────────────┴──────────┴────────────────────────────┘

Filters to Apply (reduce data volume):
- Year >= 2020
- Topic IN ('Environmental', 'Social', 'Governance')
- NormalizedValue IS NOT NULL

Relationships:
→ Companies[CompanyKey] = ESGMetrics[CompanyKey]

Measures to Create:
- Total Emissions = CALCULATE(SUM(ESGMetrics[NormalizedValue]), ESGMetrics[MetricGroup]="Scope 1 Emissions")
- Avg Water Usage = CALCULATE(AVERAGE(ESGMetrics[NormalizedValue]), ESGMetrics[MetricGroup]="Water Consumption")
```

---

### **📊 Table 4: Aggregated Company Scores (STAR TABLE)**

```
Source: /api/powerbi/aggregated/company-scores

Power BI Table Name: CompanyScoresAgg
Load Mode: Import
Rows: ~150

Columns:
┌─────────────────────┬──────────┬────────────────────────────┐
│ Column              │ Type     │ Use                        │
├─────────────────────┼──────────┼────────────────────────────┤
│ CompanyKey          │ Text     │ Primary Key                │
│ CompanyName         │ Text     │ Display                    │
│ Sector              │ Text     │ Group By                   │
│ Industry            │ Text     │ Group By                   │
│ Country             │ Text     │ Filter                     │
│ LatestYear          │ Integer  │ Most recent data year      │
│ AvgESGScore         │ Decimal  │ Main KPI                   │
│ AvgESGPulse         │ Decimal  │ Activity metric            │
│ AvgRiskScore        │ Decimal  │ Risk KPI                   │
│ CurrentRiskLevel    │ Text     │ Status (high/med/low)      │
│ CurrentGrade        │ Text     │ Letter grade               │
│ DataSourceCount     │ Integer  │ Data coverage indicator    │
└─────────────────────┴──────────┴────────────────────────────┘

**WHY THIS TABLE?**
✅ Pre-aggregated = Fast loading
✅ No complex relationships needed
✅ Perfect for Executive Dashboard
✅ Easy to understand for business users

Use for:
- Company scorecard
- Sector comparison
- Top/Bottom performers
```

---

### **📊 Table 5: Sector Benchmarks (STAR TABLE)**

```
Source: /api/powerbi/aggregated/sector-benchmarks

Power BI Table Name: SectorBenchmarks
Load Mode: Import
Rows: ~15

Columns:
┌─────────────────────┬──────────┬────────────────────────────┐
│ Column              │ Type     │ Use                        │
├─────────────────────┼──────────┼────────────────────────────┤
│ Sector              │ Text     │ Primary Key                │
│ CompanyCount        │ Integer  │ KPI                        │
│ AvgOverallScore     │ Decimal  │ Benchmark value            │
│ MinScore            │ Decimal  │ Range min                  │
│ MaxScore            │ Decimal  │ Range max                  │
│ MedianScore         │ Decimal  │ Better benchmark           │
│ AvgRiskScore        │ Decimal  │ Sector risk                │
│ HighRiskCount       │ Integer  │ Alert KPI                  │
│ LowRiskCount        │ Integer  │ Good performer KPI         │
└─────────────────────┴──────────┴────────────────────────────┘

Use for:
- Sector comparison charts
- Benchmarking analysis
- Industry risk heatmap
```

---

## 4️⃣ COMPLETE DASHBOARD DESIGNS

### **📊 DASHBOARD 1: EXECUTIVE SUMMARY**

```
┌─────────────────────────────────────────────────────────────────┐
│  🏢 ESG EXECUTIVE DASHBOARD                          Q4 2024   │
├────────────┬────────────┬────────────┬────────────┬────────────┤
│            │            │            │            │            │
│  📈 Total  │  🎯 Avg    │  ⚠️ High   │  ✅ Low    │  📊 Data   │
│  Companies │  ESG Score │  Risk      │  Risk      │  Coverage  │
│    150     │   67.5     │   23       │   89       │    95%     │
│  [Card]    │  [Card]    │  [Card]    │  [Card]    │  [Card]    │
│            │            │            │            │            │
├────────────┴────────────┴────────────┴────────────┴────────────┤
│                                                                 │
│  📊 ESG Score Distribution by Sector                            │
│  [Clustered Column Chart]                                       │
│  X-axis: Sector | Y-axis: Avg ESG Score | Data Labels: ON      │
│                                                                 │
│  Technology     ████████████████ 72.5                           │
│  Financials     ███████████████ 68.3                            │
│  Healthcare     ██████████████ 65.8                             │
│  Energy         ███████████ 58.2                                │
│  Materials      ██████████ 55.1                                 │
│                                                                 │
├──────────────────────────────┬──────────────────────────────────┤
│                              │                                  │
│  🗺️ Risk Heatmap by Country  │  📈 Top 10 ESG Performers        │
│  [Map Visual]                │  [Table with Conditional Format] │
│                              │                                  │
│  • High Risk: 🔴            │  Rank │ Company    │ Score │ ↕    │
│  • Medium Risk: 🟡          │  ──────┼───────────┼───────┼──── │
│  • Low Risk: 🟢            │   1   │ Microsoft  │ 89.5  │ +2   │
│                              │   2   │ Apple      │ 87.2  │ +1   │
│  Interactive: Click country  │   3   │ Tesla      │ 85.1  │ -1   │
│  → Drill to companies        │   4   │ Alphabet   │ 84.3  │ 0    │
│                              │   ...                            │
│                              │                                  │
└──────────────────────────────┴──────────────────────────────────┘

Slicers (Left Panel):
☐ Year: [2020 2021 2022 2023 2024]
☐ Sector: [All ▼]
☐ Risk Level: [All ▼]
☐ Data Source: [All ▼]

Filters (Page Level):
- Year >= 2020
- OverallScore IS NOT NULL

Tables Used:
- CompanyScoresAgg (main)
- SectorBenchmarks (comparison)
```

**DAX Measures:**

```dax
Total Companies = DISTINCTCOUNT(CompanyScoresAgg[CompanyKey])

Avg ESG Score = 
AVERAGE(CompanyScoresAgg[AvgESGScore])

High Risk Count = 
CALCULATE(
    COUNTROWS(CompanyScoresAgg),
    CompanyScoresAgg[CurrentRiskLevel] = "high"
)

Low Risk Count = 
CALCULATE(
    COUNTROWS(CompanyScoresAgg),
    CompanyScoresAgg[CurrentRiskLevel] = "low"
)

Data Coverage % = 
VAR TotalCompanies = [Total Companies]
VAR CompaniesWithData = 
    CALCULATE(
        COUNTROWS(CompanyScoresAgg),
        CompanyScoresAgg[DataSourceCount] > 0
    )
RETURN DIVIDE(CompaniesWithData, TotalCompanies, 0)

Risk Status Color = 
SWITCH(
    SELECTEDVALUE(CompanyScoresAgg[CurrentRiskLevel]),
    "high", "🔴 High Risk",
    "medium", "🟡 Medium Risk",
    "low", "🟢 Low Risk",
    "⚪ Unknown"
)
```

---

### **📊 DASHBOARD 2: ENVIRONMENTAL PERFORMANCE**

```
┌─────────────────────────────────────────────────────────────────┐
│  🌍 ENVIRONMENTAL METRICS                                       │
├─────────────┬───────────────────────────────────────────────────┤
│             │                                                   │
│  Filters:   │  📉 Total GHG Emissions Trend (2020-2024)         │
│             │  [Line Chart with Markers]                        │
│  □ Sector   │                                                   │
│  □ Industry │  2024: 15.2M tCO2e ↓12% YoY                       │
│  □ Country  │                                                   │
│  □ Company  │  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━                    │
│             │                                                   │
├─────────────┴───────────────────────────────────────────────────┤
│                                                                 │
│  📊 Emissions Breakdown (Scope 1, 2, 3)                         │
│  [Stacked Bar Chart]                                            │
│                                                                 │
│  Scope 1   ██████████████ 6.8M (45%)                            │
│  Scope 2   ████████ 4.6M (30%)                                  │
│  Scope 3   ██████ 3.8M (25%)                                    │
│                                                                 │
├──────────────────────────┬──────────────────────────────────────┤
│                          │                                      │
│  💧 Water Consumption    │  ♻️ Waste Recycling Performance      │
│  [Gauge Chart]           │  [Donut Chart]                       │
│                          │                                      │
│  125M m³                 │  ● Recycled: 68%                     │
│  Target: 100M m³         │  ● Landfill: 22%                     │
│  ↓ 8% vs Last Year       │  ● Incinerated: 10%                  │
│                          │                                      │
├──────────────────────────┴──────────────────────────────────────┤
│                                                                 │
│  🔋 Renewable Energy Adoption by Sector                         │
│  [Clustered Column + Line Combo Chart]                          │
│  Columns: Renewable % | Line: YoY Growth                        │
│                                                                 │
│  Technology:  ████████████ 85% ↑15%                             │
│  Finance:     ██████████ 72% ↑8%                                │
│  Healthcare:  ████████ 65% ↑12%                                 │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

Tables Used:
- ESGMetrics (filtered: Topic = 'Environmental')
- CompanyScoresAgg

Key Metrics to Filter:
- MetricGroup = 'Scope 1 Emissions'
- MetricGroup = 'Scope 2 Emissions'
- MetricGroup = 'Scope 3 Emissions'
- MetricGroup = 'Water Consumption'
- MetricGroup = 'Renewable Energy'
- MetricGroup = 'Waste Recycled'
```

**DAX Measures:**

```dax
Total Scope 1 = 
CALCULATE(
    SUM(ESGMetrics[NormalizedValue]),
    ESGMetrics[MetricGroup] = "Scope 1 Emissions"
)

Total Scope 2 = 
CALCULATE(
    SUM(ESGMetrics[NormalizedValue]),
    ESGMetrics[MetricGroup] = "Scope 2 Emissions"
)

Total Scope 3 = 
CALCULATE(
    SUM(ESGMetrics[NormalizedValue]),
    ESGMetrics[MetricGroup] = "Scope 3 Emissions"
)

Total GHG Emissions = 
[Total Scope 1] + [Total Scope 2] + [Total Scope 3]

YoY Emissions Change % = 
VAR CurrentYear = MAX(ESGMetrics[Year])
VAR CurrentEmissions = 
    CALCULATE([Total GHG Emissions], ESGMetrics[Year] = CurrentYear)
VAR PriorEmissions = 
    CALCULATE([Total GHG Emissions], ESGMetrics[Year] = CurrentYear - 1)
RETURN 
DIVIDE(CurrentEmissions - PriorEmissions, PriorEmissions, 0)

Water Consumption Total = 
CALCULATE(
    SUM(ESGMetrics[NormalizedValue]),
    ESGMetrics[MetricGroup] = "Water Consumption"
)

Renewable Energy % = 
VAR Renewable = 
    CALCULATE(
        SUM(ESGMetrics[NormalizedValue]),
        ESGMetrics[MetricGroup] = "Renewable Energy"
    )
VAR Total = 
    CALCULATE(
        SUM(ESGMetrics[NormalizedValue]),
        ESGMetrics[MetricGroup] = "Total Energy Consumption"
    )
RETURN DIVIDE(Renewable, Total, 0) * 100

Waste Recycling Rate = 
VAR Recycled = 
    CALCULATE(
        SUM(ESGMetrics[NormalizedValue]),
        ESGMetrics[MetricGroup] = "Waste Recycled"
    )
VAR Total = 
    CALCULATE(
        SUM(ESGMetrics[NormalizedValue]),
        ESGMetrics[MetricGroup] = "Total Waste"
    )
RETURN DIVIDE(Recycled, Total, 0) * 100
```

---

### **📊 DASHBOARD 3: SOCIAL & GOVERNANCE**

```
┌─────────────────────────────────────────────────────────────────┐
│  👥 SOCIAL & GOVERNANCE METRICS                                 │
├──────────────────────────┬──────────────────────────────────────┤
│                          │                                      │
│  👔 Board Composition    │  💼 Workforce Metrics                │
│  [Stacked Bar Chart]     │  [Cards + KPIs]                      │
│                          │                                      │
│  Women Directors: 38%    │  Total Employees: 125,000            │
│  Independent: 72%        │  Turnover Rate: 12.5% ↓ 2% YoY      │
│  Board Size: Avg 11      │  Women Employees: 42% ↑ 3% YoY      │
│                          │  Training Hours/Year: 45             │
│                          │                                      │
├──────────────────────────┴──────────────────────────────────────┤
│                                                                 │
│  🏭 Safety Performance by Industry                              │
│  [Matrix with Conditional Formatting]                           │
│                                                                 │
│  Industry       │ LTIFR │ TRIR │ Fatalities │ Status          │
│  ───────────────┼───────┼──────┼────────────┼─────────────── │
│  Manufacturing  │  1.2  │ 3.5  │     0      │ 🟢 Good         │
│  Energy         │  0.8  │ 2.1  │     1      │ 🟡 Caution      │
│  Technology     │  0.3  │ 0.9  │     0      │ 🟢 Excellent    │
│  Construction   │  2.5  │ 5.8  │     2      │ 🔴 Alert        │
│                                                                 │
├──────────────────────────┬──────────────────────────────────────┤
│                          │                                      │
│  ⚖️ Compliance Overview   │  🎓 Training Investment Trend        │
│  [Funnel Chart]          │  [Area Chart]                        │
│                          │                                      │
│  Incidents: 45 ▼         │  2024: $2.5M ↑ 15% YoY              │
│  Investigated: 42        │                                      │
│  Resolved: 39            │  ━━━━━━━━━━━━━━━━━━━━               │
│  Penalties: 3            │  2020 → 2024 Trend                   │
│                          │                                      │
└──────────────────────────┴──────────────────────────────────────┘

Tables Used:
- ESGMetrics (filtered: Topic IN ('Social', 'Governance'))

Key Metrics:
- MetricGroup = 'Board Diversity'
- MetricGroup = 'Women Employees'
- MetricGroup = 'Employee Turnover'
- MetricGroup = 'Lost Time Injury'
- MetricGroup = 'Employee Training'
```

---

### **📊 DASHBOARD 4: COMPANY DEEP DIVE**

```
┌─────────────────────────────────────────────────────────────────┐
│  🔍 COMPANY PROFILE                                             │
├────────────┬────────────┬────────────┬────────────┬────────────┤
│            │            │            │            │            │
│  Company:  │  Sector:   │  Industry: │  Country:  │  Latest:   │
│  [Slicer]  │  Tech      │  Software  │  USA       │  2024      │
│            │            │            │            │            │
├────────────┴────────────┴────────────┴────────────┴────────────┤
│                                                                 │
│  📈 ESG Score Evolution (5 Years)                               │
│  [Line Chart with Markers + Trend Line]                         │
│                                                                 │
│  2020: 68.5 → 2021: 71.2 → 2022: 74.8 → 2023: 78.1            │
│  → 2024: 82.3  [+13.8 points in 5 years]                       │
│                                                                 │
├──────────────────────────┬──────────────────────────────────────┤
│                          │                                      │
│  🎯 E-S-G Breakdown       │  📊 Key Metrics Performance          │
│  [Radar Chart]           │  [Table with Sparklines]             │
│                          │                                      │
│  Environmental: 85 🟢    │  Metric          │ 5Y Trend │ Value  │
│  Social: 78 🟡          │  ────────────────┼─────────┼────────│
│  Governance: 84 🟢      │  Scope 1         │ ↓↓↓↓→   │ 1.2M   │
│                          │  Renewable %     │ ↑↑↑↑↑   │ 85%    │
│  [Interactive]           │  Turnover Rate   │ →↓↓→↑   │ 12%    │
│                          │  Board Diversity │ ↑↑↑→↑   │ 38%    │
│                          │                                      │
├──────────────────────────┴──────────────────────────────────────┤
│                                                                 │
│  🏆 Peer Comparison (Industry Benchmark)                        │
│  [Clustered Bar Chart + Average Line]                           │
│                                                                 │
│  ● Selected Company     ━━━ Industry Avg: 70.5                │
│                                                                 │
│  Tesla          ████████████████████ 82.3                       │
│  BYD            ███████████████ 75.8                            │
│  NIO            ████████████ 68.5                               │
│  Rivian         ███████████ 65.2                                │
│  Lucid          ██████████ 62.9                                 │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

Features:
✅ Company selector slicer (single select)
✅ Auto-filter all visuals by selected company
✅ Drill-through from other pages
✅ Export to PDF button
✅ Bookmark for snapshot
```

---

## 5️⃣ DEPLOYMENT STEPS

### **Step 1: Deploy Enhanced API**

```bash
# Copy new API
cp api_server_powerbi_enhanced.py metricflow/api_server.py

# Update docker-compose.yml (if needed - already correct)

# Rebuild
docker-compose build metricflow

# Start
docker-compose up -d metricflow

# Verify
curl http://localhost:8083/health
curl http://localhost:8083/api/powerbi/companies | jq
```

### **Step 2: Create Power BI File**

1. Open Power BI Desktop

2. Get Data from API:
   ```
   Query 1: Companies
   URL: http://localhost:8083/api/powerbi/companies
   
   Query 2: CompanyScoresAgg
   URL: http://localhost:8083/api/powerbi/aggregated/company-scores
   
   Query 3: SectorBenchmarks
   URL: http://localhost:8083/api/powerbi/aggregated/sector-benchmarks
   ```

3. Transform Data:
   - List → To Table
   - Expand columns
   - Set data types
   - Rename friendly

4. Close & Apply

### **Step 3: Create Relationships**

Model View:
```
CompanyScoresAgg[CompanyKey] → Companies[CompanyKey]  (Many-to-One)
ESGScores[CompanyKey] → Companies[CompanyKey]  (Many-to-One)
ESGMetrics[CompanyKey] → Companies[CompanyKey]  (Many-to-One)
```

### **Step 4: Create Measures**

Copy all DAX measures from this guide into a measures table.

### **Step 5: Build Dashboards**

Follow the 4 dashboard designs above.

---

## 6️⃣ FINAL CHECKLIST

```
✅ Enhanced API deployed and tested
✅ All 5 tables imported to Power BI
✅ Relationships configured correctly
✅ DAX measures created and tested
✅ Dashboard 1: Executive Summary completed
✅ Dashboard 2: Environmental completed
✅ Dashboard 3: Social & Governance completed
✅ Dashboard 4: Company Deep Dive completed
✅ Slicers and filters working
✅ Performance optimized (< 3 sec load)
✅ Published to Power BI Service (optional)
✅ Scheduled refresh configured (optional)
```

---

**🎯 START WITH TABLE 4 (CompanyScoresAgg) - EASIEST TO BUILD EXECUTIVE DASHBOARD!**