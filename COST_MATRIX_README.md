# Transportation Cost Matrix Generation
## Overview

The cost matrix generation system transforms raw transportation data into optimized cost matrices that can be used by the transportation optimization model. It supports multiple mathematical approaches and aggregation methods.

## Data Structure

The `data/transportation_data.csv` file contains realistic transportation data with the following factors:

- **warehouse**: Source warehouse (Warehouse_A, Warehouse_B)
- **customer**: Destination customer (Customer_1, Customer_2)
- **distance_km**: Distance in kilometers
- **fuel_price_per_liter**: Current fuel prices
- **vehicle_capacity_tons**: Vehicle capacity
- **base_rate_per_km**: Base transportation rate
- **travel_time_hours**: Estimated travel time
- **road_condition_factor**: Road quality multiplier (1.0 = good, >1.0 = poor)
- **seasonal_factor**: Seasonal demand/pricing multiplier
- **priority_multiplier**: Priority shipping multiplier

## Mathematical Models
### 1. Time Model
Time-based cost calculation:
```
Cost = travel_time × hourly_rate × seasonal_factor × priority_multiplier
```

### 2. Composite Model (Recommended)
Comprehensive model combining all factors:
```
distance_cost = distance × base_rate × road_condition
fuel_cost = calculated fuel consumption cost
time_cost = travel_time × hourly_rate × 0.3
capacity_factor = 10 / vehicle_capacity
Cost = (distance_cost + fuel_cost + time_cost) × capacity_factor × seasonal_factor × priority_multiplier
```

## Aggregation Methods

When multiple data points exist for the same route, you can choose:

- **mean**: Average of all observations (default)
- **median**: Median value 
- **min**: Minimum cost (optimistic scenario)
- **max**: Maximum cost (pessimistic scenario)

## Usage

### Integration with Snowflake Model
```python
from models.snowflake_transportation_model import create_snowflake_model

# Use the generated cost matrix
model = create_snowflake_model(
    cost_matrix_file='data/cost_matrix.csv',
    config_template_file='./configs/constraints.json'
)
```

## Example Output

The composite model typically produces cost matrices like:

```
customer     Customer_1  Customer_2
warehouse                          
Warehouse_A       53.30       84.20
Warehouse_B      116.37       56.86
```

## Files
- `helper/model_utils.py`: Core cost matrix generation functions
- `data/transportation_data.csv`: Enhanced transportation data with multiple factors
- `data/cost_matrix.csv`: Generated cost matrix for Snowflake integration

This system provides a robust foundation for dynamic transportation cost modeling that adapts to changing business conditions and requirements. 