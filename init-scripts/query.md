DASHBOARD QUERY PATTERNS - ESG Analytics
========================================

CONCEPT: 1 Company x 1 Year x N Metrics
- Backend returns: long format (company | year | metric_name | value)
- Frontend pivots: metric_name → columns
- Missing metrics → NULL (automatic)

========================================
1. SINGLE COMPANY DASHBOARD
========================================

MetricFlow Query:
```bash
mf query \
  --metrics scope_1_emissions,scope_2_emissions,scope_3_emissions,renewable_energy,water_consumption \
  --group-by company__company_name,year \
  --where "company__company_name = 'Tesla Inc' AND year = 2024"
```

Expected Response (long format):
```
company_name | year | metric_name          | value
-------------|------|---------------------|-------
Tesla Inc    | 2024 | scope_1_emissions   | 120.5
Tesla Inc    | 2024 | scope_2_emissions   | NULL
Tesla Inc    | 2024 | scope_3_emissions   | 450.2
Tesla Inc    | 2024 | renewable_energy    | 1500.0
Tesla Inc    | 2024 | water_consumption   | NULL
```

Frontend Pivot Result:
```
Company: Tesla Inc, Year: 2024
├─ Scope 1 Emissions: 120.5 tCO2e
├─ Scope 2 Emissions: - (NULL)
├─ Scope 3 Emissions: 450.2 tCO2e
├─ Renewable Energy: 1500.0 GJ
└─ Water Consumption: - (NULL)
```

========================================
2. MULTI-YEAR COMPARISON
========================================

MetricFlow Query:
```bash
mf query \
  --metrics total_emissions,renewable_energy \
  --group-by company__company_name,year \
  --where "company__company_name = 'Apple Inc' AND year IN (2022,2023,2024)" \
  --order-by year
```

Response:
```
company_name | year | metric_name      | value
-------------|------|-----------------|--------
Apple Inc    | 2022 | total_emissions | 1200.0
Apple Inc    | 2022 | renewable_energy| 5000.0
Apple Inc    | 2023 | total_emissions | 1150.0
Apple Inc    | 2023 | renewable_energy| 5500.0
Apple Inc    | 2024 | total_emissions | 1100.0
Apple Inc    | 2024 | renewable_energy| NULL
```

Frontend Pivot (Year → Column):
```
Apple Inc
Metric            | 2022    | 2023    | 2024
------------------|---------|---------|--------
Total Emissions   | 1200.0  | 1150.0  | 1100.0
Renewable Energy  | 5000.0  | 5500.0  | -
```

========================================
3. SECTOR COMPARISON
========================================

MetricFlow Query:
```bash
mf query \
  --metrics scope_1_emissions,water_consumption \
  --group-by company__sector,company__company_name,year \
  --where "company__sector = 'Technology' AND year = 2024" \
  --order-by -scope_1_emissions
```

Response:
```
sector     | company_name | year | metric_name         | value
-----------|--------------|------|---------------------|-------
Technology | Microsoft    | 2024 | scope_1_emissions   | 500.0
Technology | Microsoft    | 2024 | water_consumption   | 2000.0
Technology | Apple        | 2024 | scope_1_emissions   | 400.0
Technology | Apple        | 2024 | water_consumption   | NULL
Technology | Google       | 2024 | scope_1_emissions   | 350.0
Technology | Google       | 2024 | water_consumption   | 1800.0
```

Frontend Pivot (Company → Row):
```
Technology Sector - 2024

Company    | Scope 1    | Water
-----------|------------|----------
Microsoft  | 500.0      | 2000.0
Apple      | 400.0      | -
Google     | 350.0      | 1800.0
```

========================================
4. CUSTOM METRIC SELECTION (User chooses)
========================================

User selects metrics from dropdown:
- Scope 1 Emissions ✓
- Scope 3 Emissions ✓
- Employee Turnover ✓
- Waste Recycled ✓

MetricFlow Query (dynamic):
```bash
mf query \
  --metrics scope_1_emissions,scope_3_emissions,employee_turnover,waste_recycled \
  --group-by company__company_name,year \
  --where "company__company_name = 'Toyota' AND year = 2024"
```

Response:
```
company_name | year | metric_name         | value
-------------|------|---------------------|-------
Toyota       | 2024 | scope_1_emissions   | 800.0
Toyota       | 2024 | scope_3_emissions   | NULL
Toyota       | 2024 | employee_turnover   | 12.5
Toyota       | 2024 | waste_recycled      | 450.0
```

Frontend Display:
```
Toyota - 2024 Metrics

Environmental:
  Scope 1 Emissions: 800.0 tCO2e
  Scope 3 Emissions: Not Available

Social:
  Employee Turnover: 12.5%

Environmental:
  Waste Recycled: 450.0 tonnes
```

========================================
5. TRINO EQUIVALENT (for API)
========================================

Same logic in SQL:
```sql
SELECT 
    c.company_name,
    f.year,
    m.metric_name,
    f.normalized_value as value,
    u.standard_unit
FROM marts.fact_esg_metric f
JOIN marts.dim_company c ON f.company_id = c.company_id
JOIN marts.dim_metric m ON f.metric_id = m.metric_id
LEFT JOIN marts.dim_unit u ON f.unit_id = u.unit_id
WHERE c.company_name = 'Tesla Inc'
  AND f.year = 2024
  AND m.metric_name IN (
      'Scope 1 Emissions',
      'Scope 2 Emissions', 
      'Water Consumption',
      'Renewable Energy'
  );
```

========================================
6. API RESPONSE STRUCTURE (JSON)
========================================

```json
{
  "company": "Tesla Inc",
  "year": 2024,
  "metrics": [
    {
      "metric_id": "MET-00123",
      "metric_name": "Scope 1 Emissions",
      "metric_group": "emissions",
      "value": 120.5,
      "unit": "tCO2e",
      "data_source": "sustainability_report"
    },
    {
      "metric_id": "MET-00124",
      "metric_name": "Scope 2 Emissions",
      "metric_group": "emissions",
      "value": null,
      "unit": "tCO2e",
      "data_source": null
    },
    {
      "metric_id": "MET-00456",
      "metric_name": "Water Consumption",
      "metric_group": "water",
      "value": 30.0,
      "unit": "m3",
      "data_source": "cdp_disclosure"
    }
  ]
}
```

========================================
7. FRONTEND PIVOT LOGIC (Pseudo-code)
========================================

```javascript
// Backend returns long format
const data = [
  {company: "Tesla", year: 2024, metric: "Scope 1", value: 120},
  {company: "Tesla", year: 2024, metric: "Scope 2", value: null},
  {company: "Tesla", year: 2024, metric: "Water", value: 30}
];

// Pivot to wide format
const pivoted = data.reduce((acc, row) => {
  if (!acc[row.company]) acc[row.company] = {};
  if (!acc[row.company][row.year]) acc[row.company][row.year] = {};
  
  acc[row.company][row.year][row.metric] = row.value ?? "N/A";
  return acc;
}, {});

// Result:
{
  "Tesla": {
    "2024": {
      "Scope 1": 120,
      "Scope 2": "N/A",
      "Water": 30
    }
  }
}
```

========================================
8. WHY 400 METRICS IS NOT A PROBLEM
========================================

Backend:
- fact_esg_metric has all 400 metrics
- Query only selected metrics (5-20 typically)
- Filter: WHERE metric_name IN (...)

Frontend:
- User selects from dropdown (max 10-15)
- UI only renders selected metrics
- NULL values → show "N/A" or "-"

Example:
User dashboard shows:
  [x] Emissions
  [x] Energy  
  [x] Water
  [x] Waste
  [ ] Biodiversity (unchecked)
  [ ] Supply Chain (unchecked)
  
→ Query only 4 metric groups
→ Fast response, clean UI

========================================
9. PERFORMANCE OPTIMIZATION
========================================

Partitioning:
- fact_esg_metric partitioned by year
- Query: WHERE year = 2024 → single partition scan

Indexing (if using Postgres/MySQL):
- Index on (company_id, year, metric_id)
- Covering index for dashboard queries

Caching:
- Cache common queries (last 30 days)
- Invalidate on new data load

========================================
10. COMPLETE FLOW
========================================

1. User Action:
   - Select company: "Apple Inc"
   - Select year: 2024
   - Select metrics: [Emissions, Energy, Water]

2. Frontend Request:
   GET /api/metrics?company=Apple&year=2024&metrics=emissions,energy,water

3. Backend Query:
   mf query \
     --metrics total_emissions,renewable_energy,water_consumption \
     --group-by company__company_name,year \
     --where "company__company_name = 'Apple Inc' AND year = 2024"

4. Response (long):
   [{company: "Apple", year: 2024, metric: "emissions", value: 1000},
    {company: "Apple", year: 2024, metric: "energy", value: null},
    {company: "Apple", year: 2024, metric: "water", value: 500}]

5. Frontend Pivot:
   {
     "Apple Inc - 2024": {
       "Total Emissions": "1000 tCO2e",
       "Renewable Energy": "Not Available",
       "Water Consumption": "500 m3"
     }
   }

6. UI Render:
   ┌─────────────────────────────────┐
   │ Apple Inc - 2024 ESG Metrics    │
   ├─────────────────────────────────┤
   │ ✓ Total Emissions: 1000 tCO2e   │
   │ ✗ Renewable Energy: N/A         │
   │ ✓ Water Consumption: 500 m3     │
   └─────────────────────────────────┘