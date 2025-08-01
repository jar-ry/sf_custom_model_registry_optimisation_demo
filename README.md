# Transportation Linear Programming Demo

A comprehensive linear programming model for logistics and warehousing optimization using Python, featuring dynamic cost matrix generation and Snowflake integration.

## Problem Overview

This demo solves a transportation optimization problem with:
- **2 Warehouses** with capacity constraints
- **2 Customers** with demand requirements  
- **Dynamic Cost Matrix Generation** using mathematical models
- **Objective**: Minimize total transportation costs
- **Constraints**: 
  1. Warehouse capacity limits (≤ constraints)
  2. Customer demand fulfillment (≥ constraints)

## Project Structure

```
ml_registry_demo/
├── data/
│   ├── transportation_data.csv            # Enhanced transportation data with multiple factors
│   └── cost_matrix.csv                   # Generated cost matrix (created by cost generation)
├── configs/
│   └── constraints.json                  # Configurable constraint parameters
├── helper/
│   ├── model_utils.py                    # Cost matrix generation and utility functions
│   ├── register_with_snowflake.py        # Snowflake registration script
│   └── snowflake_utils.py               # Snowflake connection utilities
├── models/
│   ├── transportation_lp.py              # Main LP model implementation
│   └── snowflake_transportation_model.py # Snowflake model registry wrapper
├── main.py                              # Main entry point with command line interface
├── environment.yml                       # Conda environment file
├── COST_MATRIX_README.md                # Detailed cost matrix documentation
└── README.md                            # This file
```

## Quick Start

### Using Conda
```bash
# Create and activate the environment
conda env create -f environment.yml
conda activate ml_optimization_demo

# Test the model locally
python main.py --test

# Generate cost matrix from transportation data
python main.py --cost-matrix

# Register model with Snowflake (requires Snowflake credentials)
python main.py --register

# Run Snowflake integration example
python main.py --example
```

## Cost Matrix Generation

The project includes advanced cost matrix generation that processes realistic transportation data using mathematical models:

### Transportation Data Factors
The `data/transportation_data.csv` includes:
- Distance, fuel prices, vehicle capacity
- Travel time, road conditions, seasonal factors
- Priority multipliers and base rates

### Mathematical Models

**Time Model**: Cost based on travel time and operational factors
```
Cost = travel_time × hourly_rate × seasonal_factor × priority_multiplier
```

**Composite Model** (Recommended): Comprehensive model combining multiple factors
```
Cost = (distance_cost + fuel_cost + time_cost) × capacity_factor × seasonal_factor × priority_multiplier
```

### Usage
```bash
# Generate cost matrix using composite model
python main.py --cost-matrix
```

This creates `data/cost_matrix.csv` which is automatically used by the optimization model.

## Configuration

### Modify Constraints
Edit `configs/constraints.json` to change:
- Warehouse capacities
- Customer demands
- Add descriptions

### Modify Transportation Data
Edit `data/transportation_data.csv` to change:
- Transportation costs and factors
- Add more routes
- Adjust problem scenarios

## Sample Output

The model will show:
- Generated transportation cost matrix
- Constraint configuration
- Optimal shipment plan
- Total cost minimization
- Warehouse utilization rates
- Customer satisfaction metrics

Example cost matrix output:
```
customer     Customer_1  Customer_2
warehouse                          
Warehouse_A       53.30       84.20
Warehouse_B      116.37       56.86
```

## Snowflake Model Registry Integration

This project includes Snowflake model registry integration for enterprise deployment:

### Model Registration
```python
from models.snowflake_transportation_model import create_snowflake_model
from snowflake.ml.registry import Registry

# Create model instance with generated cost matrix
model = create_snowflake_model(
    cost_matrix_file='data/cost_matrix.csv',
    config_template_file='configs/constraints.json'
)

# Register with Snowflake (requires active session)
registry.log_model(
    model=model,
    model_name="transportation_optimizer",
    version_name="v1_0"
)
```

### Batch Optimization in Snowflake
```sql
-- Run optimization on multiple scenarios
WITH mv AS MODEL "TRANSPORTATION_OPTIMIZER" VERSION "V1_3"
SELECT
*,
mv ! "PREDICT"(
    SCENARIO_NAME,
    WAREHOUSE_A_CAPACITY,
    WAREHOUSE_B_CAPACITY,
    CUSTOMER_1_DEMAND,
    CUSTOMER_2_DEMAND,
    COST_A_TO_1,
    COST_A_TO_2,
    COST_B_TO_1,
    COST_B_TO_2
)
FROM
transportation_scenarios;
```

### Input/Output Format

**Input DataFrame columns:**
- `scenario_name`, `warehouse_a_capacity`, `warehouse_b_capacity`
- `customer_1_demand`, `customer_2_demand`
- `cost_a_to_1`, `cost_a_to_2`, `cost_b_to_1`, `cost_b_to_2` (optional overrides)

**Output DataFrame columns:**
- `scenario_name`, `optimal_cost`, `feasible`
- `shipment_a_to_1`, `shipment_a_to_2`, `shipment_b_to_1`, `shipment_b_to_2`
- `warehouse_a_utilization`, `warehouse_b_utilization`

## Command Line Interface

```bash
# Available commands
python main.py --test         # Test the model locally
python main.py --register     # Register model with Snowflake
python main.py --example      # Run Snowflake integration example
python main.py --cost-matrix  # Generate cost matrix from transportation data
```

## Key Features

1. **Dynamic Cost Matrix Generation**: Process real-world transportation factors
2. **Mathematical Model Options**: Time-based and composite cost models
3. **Flexible Data Processing**: Handle multiple data points per route with aggregation
4. **Snowflake Integration**: Enterprise-ready model deployment
5. **Command Line Interface**: Easy testing and operation
6. **Configurable Parameters**: JSON-based configuration management

## Use Cases

Perfect for demonstrating:
- Supply chain optimization with realistic cost modeling
- Resource allocation with dynamic pricing
- Cost minimization with multiple business factors
- Constraint satisfaction with enterprise deployment
- Linear programming concepts with real-world data
- **Enterprise model deployment and governance**
- **Batch optimization workflows in Snowflake**
- **Dynamic cost matrix generation from operational data**

## Files

- `helper/model_utils.py`: Core cost matrix generation functions
- `data/transportation_data.csv`: Enhanced transportation data with multiple factors
- `data/cost_matrix.csv`: Generated cost matrix for optimization
- `main.py`: Command line interface for all operations
- `COST_MATRIX_README.md`: Detailed documentation for cost matrix functionality 