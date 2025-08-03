# Transportation Optimization with Snowflake Feature Store

A demonstration of using Snowflake's Feature Store and Model Registry for transportation cost optimization using linear programming. The model uses a **hybrid architecture** that automatically adapts to both programmatic Python usage and SQL-based execution.

## 🎯 Key Features

- **Hybrid Execution Modes**: 
  - **Python Mode**: Full feature store integration for local/SPCS development
  - **SQL Mode**: Pre-joined data for optimized UDF execution  
  - **Auto-Detection**: Automatically chooses the best mode based on execution context
- **Feature Store Integration**: Dynamic cost matrices calculated from transportation data in Snowflake
- **Real-time Optimization**: Transportation costs update automatically as underlying data changes  
- **Point-in-Time Lookups**: Historical cost matrix retrieval for analysis
- **MLOps Best Practices**: Model registration and deployment in Snowflake
- **Enterprise Ready**: No local files - everything lives in Snowflake

## 🏗️ Hybrid Architecture

The model automatically detects its execution environment and adapts:

```python
# Auto mode (recommended) - detects context automatically
model = create_snowflake_model(mode='auto')

# Force Python mode for development/testing
model = create_snowflake_model(mode='python')

# Force SQL mode for UDF simulation
model = create_snowflake_model(mode='sql')
```

**Data Flow:**
```
SQL Script (creates table + data)
       ↓
Transportation Data Table (Snowflake)
       ↓
Feature Store (cost_matrix_features)
       ↓
       ├─ Python Mode: Feature Store API ──┐
       └─ SQL Mode: Pre-joined SQL Data ───┤
                                          ↓
                        Transportation Model
                                          ↓
                     Optimized Transportation Plans
```

**Key Components:**
- **Transportation Data**: Raw data with fuel prices, distances, capacity factors
- **Feature Store**: Automated cost calculations using composite mathematical models  
- **Cost Matrix Features**: Aggregated costs per route with statistical measures
- **Hybrid Model**: Adapts execution mode based on context (Python API vs SQL UDF)

## 🚀 Quick Start (3 minutes)

### 1. Environment Setup

```bash
# Set Snowflake credentials
export SNOWFLAKE_ACCOUNT="your_account"
export SNOWFLAKE_USER="your_user"
export SNOWFLAKE_PASSWORD="your_password"
export SNOWFLAKE_DATABASE="your_database"
export SNOWFLAKE_SCHEMA="your_schema" 
export SNOWFLAKE_WAREHOUSE="your_warehouse"
export SNOWFLAKE_ROLE="your_role"
```

### 2. Create Transportation Data Table

Run this SQL script in Snowflake first:

```sql
-- Execute the contents of sql/setup_transportation_table.sql
-- Creates transportation_data table and populates with sample data
```

### 3. Setup Feature Store

```bash
# Creates feature view on top of transportation_data table
python main.py --setup-fs
```

**Expected Output:**
```
Setting up Transportation Feature Store...
✅ Created cost_matrix_features feature view
✅ Feature store setup complete!

Current cost matrix from feature store:
            Customer_1  Customer_2
warehouse                        
Warehouse_A      52.72       82.59
Warehouse_B     113.80       55.65
```

### 4. Test the Hybrid Model

```bash
# Test all execution modes (auto-detection, Python, SQL)
python main.py --test-override-fs

# Or test explicit Python mode with feature store
python main.py --test-fs
```

**Expected Output:**
```
Transportation LP Model - Hybrid Mode Demo
Testing both Python mode (with feature store) and SQL mode (pre-joined data)

============================================================
🔍 Test 1: Auto Mode (Python mode expected)
============================================================
✅ Auto mode test successful!
   Detected mode: python
   Cost source: feature_store

============================================================
🐍 Test 2: Explicit Python Mode
============================================================
✅ Python mode test successful!
   Mode: python
   Cost source: feature_store

============================================================
🗄️ Test 3: SQL Mode (simulating UDF execution)
============================================================
✅ SQL mode test successful!
   Mode: sql
   Cost source: sql_input

📊 Results Summary

Auto Mode Results:
  ✅ base_case: $832.72 (mode: python, source: feature_store)
  ✅ peak_season: $1041.72 (mode: python, source: feature_store)
  ❌ reduced_capacity: N/A (mode: python, source: feature_store)
```

### 5. Register Model (Optional)

```bash
# Register model for use in Snowflake SQL
python main.py --register
```

## 📊 Usage Examples

### Python API

```python
from models.snowflake_transportation_model import create_snowflake_model
import pandas as pd

# Create model (feature store required)
model = create_snowflake_model(config_template_file='./configs/constraints.json')

# Run optimization with dynamic costs from feature store
scenarios = pd.DataFrame([{
    'scenario_name': 'my_scenario',
    'warehouse_a_capacity': 120,
    'warehouse_b_capacity': 90,
    'customer_1_demand': 80,
    'customer_2_demand': 70
}])

results = model.predict(scenarios)
print(f"Optimal cost: ${results['optimal_cost'].iloc[0]:.2f}")
print(f"Cost source: {results['cost_matrix_source'].iloc[0]}")
```

### Point-in-Time Analysis

```python
from datetime import datetime, timedelta

# Look up costs as they were yesterday
yesterday = datetime.now() - timedelta(days=1)

scenarios = pd.DataFrame([{
    'scenario_name': 'historical_analysis',
    'warehouse_a_capacity': 100,
    'warehouse_b_capacity': 80,
    'customer_1_demand': 70,
    'customer_2_demand': 60,
    'feature_timestamp': yesterday.isoformat()
}])

results = model.predict(scenarios)
```

### Cost Overrides

```python
# Override specific costs while using feature store as base
scenarios = pd.DataFrame([{
    'scenario_name': 'what_if_scenario',
    'warehouse_a_capacity': 100,
    'warehouse_b_capacity': 80,
    'customer_1_demand': 70,
    'customer_2_demand': 60,
    'cost_a_to_1': 7.50,  # Override specific route cost
    'use_feature_store': False  # Required to enable overrides
}])

results = model.predict(scenarios)
```

### Snowflake SQL Usage

After model registration, use feature store views directly in SQL:

#### Option 1: Join with Feature Store Views (Recommended)

```sql
-- Join feature store data with your scenarios
WITH feature_data AS (
  SELECT 
    warehouse,
    customer,
    composite_cost,
    feature_timestamp
  FROM cost_matrix_features  -- Feature store view
),
scenarios AS (
  SELECT 
    'production_scenario' as scenario_name,
    120 as warehouse_a_capacity,
    90 as warehouse_b_capacity,
    80 as customer_1_demand,
    70 as customer_2_demand
),
cost_matrix AS (
  SELECT 
    s.*,
    MAX(CASE WHEN f.warehouse = 'Warehouse_A' AND f.customer = 'Customer_1' THEN f.composite_cost END) as cost_a_to_1,
    MAX(CASE WHEN f.warehouse = 'Warehouse_A' AND f.customer = 'Customer_2' THEN f.composite_cost END) as cost_a_to_2,
    MAX(CASE WHEN f.warehouse = 'Warehouse_B' AND f.customer = 'Customer_1' THEN f.composite_cost END) as cost_b_to_1,
    MAX(CASE WHEN f.warehouse = 'Warehouse_B' AND f.customer = 'Customer_2' THEN f.composite_cost END) as cost_b_to_2,
    MAX(f.feature_timestamp) as feature_timestamp
  FROM scenarios s
  CROSS JOIN feature_data f
  GROUP BY s.scenario_name, s.warehouse_a_capacity, s.warehouse_b_capacity, s.customer_1_demand, s.customer_2_demand
)
SELECT
    *,
    TRANSPORTATION_OPTIMIZER_FS ! "PREDICT"(
        scenario_name,
        warehouse_a_capacity,
        warehouse_b_capacity,
        customer_1_demand,
        customer_2_demand,
        cost_a_to_1,
        cost_a_to_2,
        cost_b_to_1,
        cost_b_to_2,
        feature_timestamp
    ) as optimization_result
FROM cost_matrix;
```

#### Option 2: Historical Point-in-Time Analysis

```sql
-- Get cost matrix as of a specific date
WITH historical_features AS (
  SELECT 
    warehouse,
    customer,
    composite_cost,
    feature_timestamp
  FROM cost_matrix_features
  WHERE DATE(feature_timestamp) = '2024-01-15'  -- Historical date
),
scenarios AS (
  SELECT 
    'historical_analysis' as scenario_name,
    100 as warehouse_a_capacity,
    80 as warehouse_b_capacity,
    70 as customer_1_demand,
    60 as customer_2_demand
),
cost_matrix AS (
  SELECT 
    s.*,
    MAX(CASE WHEN f.warehouse = 'Warehouse_A' AND f.customer = 'Customer_1' THEN f.composite_cost END) as cost_a_to_1,
    MAX(CASE WHEN f.warehouse = 'Warehouse_A' AND f.customer = 'Customer_2' THEN f.composite_cost END) as cost_a_to_2,
    MAX(CASE WHEN f.warehouse = 'Warehouse_B' AND f.customer = 'Customer_1' THEN f.composite_cost END) as cost_b_to_1,
    MAX(CASE WHEN f.warehouse = 'Warehouse_B' AND f.customer = 'Customer_2' THEN f.composite_cost END) as cost_b_to_2,
    MAX(f.feature_timestamp) as feature_timestamp
  FROM scenarios s
  CROSS JOIN historical_features f
  GROUP BY s.scenario_name, s.warehouse_a_capacity, s.warehouse_b_capacity, s.customer_1_demand, s.customer_2_demand
)
SELECT
    *,
    TRANSPORTATION_OPTIMIZER_FS ! "PREDICT"(
        scenario_name,
        warehouse_a_capacity,
        warehouse_b_capacity,
        customer_1_demand,
        customer_2_demand,
        cost_a_to_1,
        cost_a_to_2,
        cost_b_to_1,
        cost_b_to_2,
        feature_timestamp
    ) as optimization_result
FROM cost_matrix;
```

#### Option 3: Cost Override Scenarios

```sql
-- Use feature store as base and override specific costs
WITH base_features AS (
  SELECT 
    MAX(CASE WHEN warehouse = 'Warehouse_A' AND customer = 'Customer_1' THEN composite_cost END) as cost_a_to_1,
    MAX(CASE WHEN warehouse = 'Warehouse_A' AND customer = 'Customer_2' THEN composite_cost END) as cost_a_to_2,
    MAX(CASE WHEN warehouse = 'Warehouse_B' AND customer = 'Customer_1' THEN composite_cost END) as cost_b_to_1,
    MAX(CASE WHEN warehouse = 'Warehouse_B' AND customer = 'Customer_2' THEN composite_cost END) as cost_b_to_2,
    MAX(feature_timestamp) as feature_timestamp
  FROM cost_matrix_features
),
scenarios AS (
  SELECT 
    'what_if_scenario' as scenario_name,
    100 as warehouse_a_capacity,
    80 as warehouse_b_capacity,
    70 as customer_1_demand,
    60 as customer_2_demand,
    -- Override specific route cost for what-if analysis
    7.50 as override_cost_a_to_1
)
SELECT
    s.*,
    TRANSPORTATION_OPTIMIZER_FS ! "PREDICT"(
        s.scenario_name,
        s.warehouse_a_capacity,
        s.warehouse_b_capacity,
        s.customer_1_demand,
        s.customer_2_demand,
        s.override_cost_a_to_1,  -- Use override
        f.cost_a_to_2,           -- Use feature store
        f.cost_b_to_1,           -- Use feature store
        f.cost_b_to_2,           -- Use feature store
        f.feature_timestamp
    ) as optimization_result
FROM scenarios s
CROSS JOIN base_features f;
```

## 🛠️ Commands Reference

```bash
# Setup and Testing
python main.py --setup-fs          # Set up feature store (run after SQL script)
python main.py --test-override-fs  # Test model with feature store
python main.py --test-fs           # Test feature store integration

# Registration and Examples  
python main.py --register          # Register model with Snowflake
python main.py --example           # Run Snowflake integration example
```

## 🔧 Configuration

### Modify Problem Constraints

Edit `configs/constraints.json`:

```json
{
    "warehouses": {
        "Warehouse_A": {"capacity": 100},
        "Warehouse_B": {"capacity": 80}
    },
    "customers": {
        "Customer_1": {"demand": 70},
        "Customer_2": {"demand": 60}
    }
}
```

### Transportation Data Schema

The `transportation_data` table contains:

```sql
CREATE TABLE transportation_data (
    warehouse STRING,               -- Source warehouse
    customer STRING,                -- Destination customer  
    distance_km FLOAT,              -- Distance in kilometers
    fuel_price_per_liter FLOAT,     -- Current fuel prices
    vehicle_capacity_tons FLOAT,    -- Vehicle capacity
    base_rate_per_km FLOAT,         -- Base transportation rate
    travel_time_hours FLOAT,        -- Estimated travel time
    road_condition_factor FLOAT,    -- Road quality multiplier
    seasonal_factor FLOAT,          -- Seasonal demand multiplier
    priority_multiplier FLOAT,      -- Priority shipping multiplier
    last_updated TIMESTAMP          -- When data was last updated
);
```

## 🐍 Python API Usage Examples

### Auto Mode (Recommended)

```python
from models.snowflake_transportation_model import create_snowflake_model
import pandas as pd
from datetime import datetime

# Auto-detects execution environment (Python vs SQL)
model = create_snowflake_model(mode='auto')

scenarios = pd.DataFrame([{
    'scenario_name': 'test_scenario',
    'warehouse_a_capacity': 100,
    'warehouse_b_capacity': 80,
    'customer_1_demand': 70,
    'customer_2_demand': 60,
    'use_feature_store': True  # Use feature store in Python mode
}])

results = model.predict(scenarios)
print(f"Mode: {results.loc[0, 'execution_mode']}")
print(f"Cost source: {results.loc[0, 'cost_matrix_source']}")
```

### Explicit Python Mode with Feature Store

```python
# Force Python mode for feature store usage
model = create_snowflake_model(mode='python')

scenarios = pd.DataFrame([{
    'scenario_name': 'feature_store_test',
    'warehouse_a_capacity': 100,
    'warehouse_b_capacity': 80,
    'customer_1_demand': 70,
    'customer_2_demand': 60,
    'use_feature_store': True,
    'feature_timestamp': datetime.now()  # Point-in-time lookup
}])

results = model.predict(scenarios)
```

### Cost Matrix Overrides in Python Mode

```python
# Override specific costs while using feature store as base
scenarios = pd.DataFrame([{
    'scenario_name': 'override_test',
    'warehouse_a_capacity': 100,
    'warehouse_b_capacity': 80, 
    'customer_1_demand': 70,
    'customer_2_demand': 60,
    'use_feature_store': True,
    'cost_a_to_1': 15.0,  # Override this specific cost
    'cost_b_to_2': 12.0   # Override this specific cost
}])

results = model.predict(scenarios)
# Uses feature store costs except for overridden values
```

### SQL Mode Simulation

```python
# Simulate UDF execution with pre-joined data
model = create_snowflake_model(mode='sql')

scenarios = pd.DataFrame([{
    'scenario_name': 'sql_test',
    'warehouse_a_capacity': 100,
    'warehouse_b_capacity': 80,
    'customer_1_demand': 70,
    'customer_2_demand': 60,
    # All cost parameters required in SQL mode
    'cost_a_to_1': 10.0, 'cost_a_to_2': 12.0,
    'cost_b_to_1': 15.0, 'cost_b_to_2': 8.0
}])

results = model.predict(scenarios)
assert results.loc[0, 'execution_mode'] == 'sql'
```

## 📈 Feature Store Details

### Cost Calculation Formula

The feature store calculates composite costs using:

```sql
-- Composite cost combines multiple factors:
Cost = (distance_cost + fuel_cost + time_cost) × 
       capacity_factor × seasonal_factor × priority_multiplier

WHERE:
  distance_cost = distance_km × base_rate_per_km × road_condition_factor
  fuel_cost = (distance_km / fuel_efficiency) × fuel_price_per_liter  
  time_cost = travel_time_hours × 25 × 0.3
  capacity_factor = 10 / vehicle_capacity_tons
```

### Available Features

- **`composite_cost`**: Final aggregated cost per route
- **`avg_distance_cost`**: Average distance-based cost
- **`avg_fuel_cost`**: Average fuel cost component  
- **`avg_time_cost`**: Average time-based cost
- **`fuel_price_volatility`**: Standard deviation of fuel prices
- **`sample_count`**: Number of data points used

## 🎯 Key Benefits

✅ **Dynamic Costs** - No static files, costs update with data  
✅ **Real-time** - Feature store calculations happen automatically  
✅ **Scalable** - Snowflake handles computation and storage  
✅ **Auditable** - Point-in-time lookups for reproducibility  
✅ **Enterprise Ready** - Model registry integration for production  
✅ **No Maintenance** - Feature engineering handled by Snowflake  

## 🔍 Troubleshooting

### Common Issues

**"Feature store is required for this model" Error**
- Run the SQL script in Snowflake first: `sql/setup_transportation_table.sql`
- Then run: `python main.py --setup-fs`
- Verify Snowflake credentials and permissions

**Missing Feature Views**
- Run `python main.py --setup-fs` to recreate feature view
- Check that `transportation_data` table exists and contains data

**Connection Errors**
- Verify environment variables are set correctly
- Check Snowflake ML dependencies are installed
- Ensure ML role has feature store permissions

**Model Initialization Failures**
- Verify `transportation_data` table contains data
- Check feature view permissions
- Ensure feature store utilities import correctly

### Prerequisites

- ✅ Snowflake account with ML capabilities
- ✅ Python environment with Snowflake ML libraries
- ✅ Valid Snowflake credentials and permissions
- ✅ Execute `sql/setup_transportation_table.sql` first

## 🎓 Educational Value

This demo teaches:

1. **Feature Store Concepts** - Real-time feature engineering at scale
2. **MLOps Best Practices** - Model versioning, registration, deployment  
3. **Point-in-Time Correctness** - Historical feature consistency
4. **Enterprise ML** - Production-ready model deployment
5. **Dynamic Optimization** - Real-time cost matrix management

Perfect for demonstrating **modern ML infrastructure** and **feature store capabilities**! 🚀 