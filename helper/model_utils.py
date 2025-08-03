import os
import pandas as pd
import numpy as np
from helper.snowflake_utils import get_snowpark_session, get_snowflake_connection

def example_model_usage_in_snowflake():
    """
    Example of how to use the registered model in Snowflake for batch predictions.
    This would be run as SQL in Snowflake after registration.
    """
    sql_generate_scenarios = """
    -- Create a table with optimization scenarios
    CREATE OR REPLACE TABLE transportation_scenarios AS
    SELECT * FROM VALUES
        ('peak_season', 100, 80, 90, 80, 5, 8, 6, 4),
        ('off_season', 100, 80, 50, 40, 5, 8, 6, 4),
        ('fuel_price_hike', 100, 80, 70, 60, 7, 10, 8, 6),
        ('new_warehouse', 150, 80, 70, 60, 4, 7, 6, 4)
    AS t(scenario_name, warehouse_a_capacity, warehouse_b_capacity, 
        customer_1_demand, customer_2_demand, cost_a_to_1, cost_a_to_2, 
        cost_b_to_1, cost_b_to_2);
    """
    results = get_snowflake_connection().cursor().execute(sql_generate_scenarios)
    print(results)

    sql_batch_optimization = f"""
    -- Run batch optimization using the registered model
    WITH feature_costs AS (
    SELECT 
        MAX(CASE WHEN warehouse = 'Warehouse_A' AND customer = 'Customer_1' THEN composite_cost END) as cost_a_to_1,
        MAX(CASE WHEN warehouse = 'Warehouse_A' AND customer = 'Customer_2' THEN composite_cost END) as cost_a_to_2,
        MAX(CASE WHEN warehouse = 'Warehouse_B' AND customer = 'Customer_1' THEN composite_cost END) as cost_b_to_1,
        MAX(CASE WHEN warehouse = 'Warehouse_B' AND customer = 'Customer_2' THEN composite_cost END) as cost_b_to_2,
        MAX(feature_timestamp) as feature_timestamp
    FROM "COST_MATRIX_FEATURES$1.0"  -- This is your feature store view
    ),
    mv AS MODEL "TRANSPORTATION_OPTIMIZER_FS" VERSION "{os.getenv('MODEL_VERSION')}",
    scenarios AS (
    SELECT
        SCENARIO_NAME,
        WAREHOUSE_A_CAPACITY,
        WAREHOUSE_B_CAPACITY,
        CUSTOMER_1_DEMAND,
        CUSTOMER_2_DEMAND
        FROM TRANSPORTATION_SCENARIOS
    )
    SELECT
        mv ! "PREDICT"(
            s.scenario_name,
            s.warehouse_a_capacity,
            s.warehouse_b_capacity,
            s.customer_1_demand,
            s.customer_2_demand,
            f.cost_a_to_1,
            f.cost_a_to_2,
            f.cost_b_to_1,
            f.cost_b_to_2,
            f.feature_timestamp
        ) as result,
        -- Extract values from the JSON object
        result:optimal_cost::FLOAT AS optimal_cost,
        result:shipment_a_to_1::FLOAT AS shipment_a_to_1,
        result:shipment_a_to_2::FLOAT AS shipment_a_to_2,
        result:shipment_b_to_1::FLOAT AS shipment_b_to_1,
        result:shipment_b_to_2::FLOAT AS shipment_b_to_2,
        result:warehouse_a_utilization::FLOAT AS warehouse_a_utilization,
        result:warehouse_b_utilization::FLOAT AS warehouse_b_utilization,
        result:scenario_name::STRING AS result_scenario_name,
        result:feasible::BOOLEAN AS feasible,
    FROM scenarios s
    CROSS JOIN feature_costs f;
    """
    print(sql_batch_optimization)
    results = get_snowpark_session().sql(sql_batch_optimization).to_pandas()
    print(results)

def create_sample_scenarios_table():
    """
    Create sample scenarios for testing the model.
    """
    scenarios = pd.DataFrame([
        # Base case
        {
            'scenario_name': 'base_case',
            'warehouse_a_capacity': 100,
            'warehouse_b_capacity': 80,
            'customer_1_demand': 70,
            'customer_2_demand': 60,
            'cost_a_to_1': 5, 'cost_a_to_2': 8,
            'cost_b_to_1': 6, 'cost_b_to_2': 4
        },
        # High demand scenario
        {
            'scenario_name': 'peak_season',
            'warehouse_a_capacity': 100,
            'warehouse_b_capacity': 80,
            'customer_1_demand': 90,
            'customer_2_demand': 80,
            'cost_a_to_1': 5, 'cost_a_to_2': 8,
            'cost_b_to_1': 6, 'cost_b_to_2': 4
        },
        # Price volatility
        {
            'scenario_name': 'fuel_cost_spike',
            'warehouse_a_capacity': 100,
            'warehouse_b_capacity': 80,
            'customer_1_demand': 70,
            'customer_2_demand': 60,
            'cost_a_to_1': 7, 'cost_a_to_2': 10,
            'cost_b_to_1': 8, 'cost_b_to_2': 6
        },
        # Capacity constraints
        {
            'scenario_name': 'reduced_capacity',
            'warehouse_a_capacity': 60,
            'warehouse_b_capacity': 50,
            'customer_1_demand': 70,
            'customer_2_demand': 60,
            'cost_a_to_1': 5, 'cost_a_to_2': 8,
            'cost_b_to_1': 6, 'cost_b_to_2': 4
        },
        # Infeasible scenario  
        {
            'scenario_name': 'impossible_demand',
            'warehouse_a_capacity': 50,
            'warehouse_b_capacity': 40,
            'customer_1_demand': 70,
            'customer_2_demand': 60,
            'cost_a_to_1': 5, 'cost_a_to_2': 8,
            'cost_b_to_1': 6, 'cost_b_to_2': 4
        }
    ])
    
    return scenarios

