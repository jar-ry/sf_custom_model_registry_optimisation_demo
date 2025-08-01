# Transportation Optimization with Snowflake Feature Store

A transportation linear programming optimization model that demonstrates **Snowflake Feature Store** integration for dynamic cost matrix management.

## 🎯 What This Demo Shows

- **Feature Store Integration**: Dynamic cost matrices calculated from transportation data in Snowflake
- **Real-time Optimization**: Transportation costs update automatically as underlying data changes  
- **Point-in-Time Lookups**: Historical cost matrix retrieval for analysis
- **MLOps Best Practices**: Model registration and deployment in Snowflake
- **Enterprise Ready**: No local files - everything lives in Snowflake

## 🏗️ Architecture

```
SQL Script (creates table + data)
       ↓
Transportation Data Table (Snowflake)
       ↓
Feature Store (cost_matrix_features)
       ↓
Transportation Model (dynamic cost retrieval)
       ↓
Optimized Transportation Plans
```

**Key Components:**
- **Transportation Data**: Raw data with fuel prices, distances, capacity factors
- **Feature Store**: Automated cost calculations using composite mathematical models  
- **Cost Matrix Features**: Aggregated costs per route with statistical measures
- **Optimization Model**: Linear programming with dynamic cost retrieval

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

### 4. Test the Model

```bash
# Test optimization with feature store costs
python main.py --test-override-fs
```

**Expected Output:**
```
Transportation LP Model - Feature Store Demo
Testing 5 scenarios with feature store...

Results Summary:
✅ base_case: $832.72 (source: feature_store)
✅ peak_season: $1041.72 (source: feature_store)
✅ fuel_cost_spike: $1149.72 (source: feature_store)
❌ reduced_capacity: N/A (source: feature_store)
❌ impossible_demand: N/A (source: feature_store)
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

After model registration:

```sql
WITH mv AS MODEL "TRANSPORTATION_OPTIMIZER_FS" VERSION "V1_0"
SELECT
    *,
    mv ! "PREDICT"(
        SCENARIO_NAME,
        WAREHOUSE_A_CAPACITY,
        WAREHOUSE_B_CAPACITY,
        CUSTOMER_1_DEMAND,
        CUSTOMER_2_DEMAND,
        NULL, NULL, NULL, NULL,  -- No cost overrides needed
        TRUE,  -- use_feature_store
        NULL   -- feature_timestamp (use current)
    )
FROM (
    SELECT 
        'production_scenario' as SCENARIO_NAME,
        120 as WAREHOUSE_A_CAPACITY,
        90 as WAREHOUSE_B_CAPACITY,
        80 as CUSTOMER_1_DEMAND,
        70 as CUSTOMER_2_DEMAND
);
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