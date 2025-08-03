-- Transportation Data Table Setup for Feature Store Integration
-- This script creates and populates the transportation_data table in Snowflake

-- Create the transportation data table
CREATE OR REPLACE TABLE transportation_data (
    warehouse STRING,
    customer STRING,
    distance_km FLOAT,
    fuel_price_per_liter FLOAT,
    vehicle_capacity_tons FLOAT,
    base_rate_per_km FLOAT,
    travel_time_hours FLOAT,
    road_condition_factor FLOAT,
    seasonal_factor FLOAT,
    priority_multiplier FLOAT,
    last_updated TIMESTAMP
);

-- Grant necessary permissions (adjust role names as needed)
GRANT SELECT, INSERT, UPDATE ON transportation_data TO ROLE CUSTOM_ML_REGISTRY_DEMO_ROLE;

-- Insert sample transportation data
INSERT INTO transportation_data VALUES
    ('Warehouse_A', 'Customer_1', 45, 1.25, 10, 0.8, 0.9, 1.0, 1.0, 1.0, CURRENT_TIMESTAMP()),
    ('Warehouse_A', 'Customer_2', 72, 1.25, 10, 0.8, 1.4, 1.1, 1.0, 1.0, CURRENT_TIMESTAMP()),
    ('Warehouse_B', 'Customer_1', 68, 1.30, 8, 0.85, 1.3, 1.2, 1.0, 1.0, CURRENT_TIMESTAMP()),
    ('Warehouse_B', 'Customer_2', 38, 1.30, 8, 0.85, 0.8, 1.0, 1.0, 1.0, CURRENT_TIMESTAMP()),
    ('Warehouse_A', 'Customer_1', 45, 1.35, 10, 0.8, 0.9, 1.0, 1.1, 1.2, CURRENT_TIMESTAMP()),
    ('Warehouse_A', 'Customer_2', 72, 1.35, 10, 0.8, 1.4, 1.1, 1.1, 1.0, CURRENT_TIMESTAMP()),
    ('Warehouse_B', 'Customer_1', 68, 1.40, 8, 0.85, 1.3, 1.2, 1.1, 1.0, CURRENT_TIMESTAMP()),
    ('Warehouse_B', 'Customer_2', 38, 1.40, 8, 0.85, 0.8, 1.0, 1.1, 1.0, CURRENT_TIMESTAMP()),
    ('Warehouse_A', 'Customer_1', 45, 1.20, 12, 0.75, 0.8, 0.9, 0.9, 1.0, CURRENT_TIMESTAMP()),
    ('Warehouse_A', 'Customer_2', 72, 1.20, 12, 0.75, 1.2, 0.9, 0.9, 1.0, CURRENT_TIMESTAMP()),
    ('Warehouse_B', 'Customer_1', 68, 1.25, 10, 0.80, 1.1, 1.0, 0.9, 1.0, CURRENT_TIMESTAMP()),
    ('Warehouse_B', 'Customer_2', 38, 1.25, 10, 0.80, 0.7, 0.9, 0.9, 1.0, CURRENT_TIMESTAMP()),
    ('Warehouse_A', 'Customer_1', 45, 1.28, 10, 0.8, 1.0, 1.1, 1.05, 1.1, CURRENT_TIMESTAMP()),
    ('Warehouse_A', 'Customer_2', 72, 1.28, 10, 0.8, 1.5, 1.2, 1.05, 1.0, CURRENT_TIMESTAMP()),
    ('Warehouse_B', 'Customer_1', 68, 1.32, 8, 0.85, 1.4, 1.3, 1.05, 1.0, CURRENT_TIMESTAMP()),
    ('Warehouse_B', 'Customer_2', 38, 1.32, 8, 0.85, 0.9, 1.1, 1.05, 1.0, CURRENT_TIMESTAMP());

-- Show sample data with calculated costs
SELECT 
    warehouse,
    customer,
    distance_km,
    fuel_price_per_liter,
    seasonal_factor,
    -- Composite cost calculation
    ((distance_km * base_rate_per_km * road_condition_factor + 
      (distance_km / (8 * (vehicle_capacity_tons / 10) * (1 / road_condition_factor))) * fuel_price_per_liter +
      travel_time_hours * 25 * 0.3) * 
     (10 / vehicle_capacity_tons) * seasonal_factor * priority_multiplier) as calculated_cost,
    last_updated
FROM transportation_data
ORDER BY warehouse, customer, last_updated DESC
LIMIT 10;
