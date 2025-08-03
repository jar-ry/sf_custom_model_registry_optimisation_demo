"""
Example: Registering Transportation LP Model with Snowflake Model Registry
=========================================================================

This script demonstrates how to register your transportation optimization
model with Snowflake's model registry. Model uses feature store for cost matrices.
"""

import os
import pandas as pd
from models.snowflake_transportation_model import create_snowflake_model
from helper.snowflake_utils import get_model_registry, get_snowflake_connection
from datetime import datetime

def register_transportation_model():
    """
    Register the transportation LP model with Snowflake model registry.
    
    Prerequisites:
    1. Snowflake session established
    2. Model registry database/schema created
    3. Transportation feature store set up
    4. Appropriate permissions
    """
    
    # Step 1: Create the Snowflake-compatible model
    print("Creating Snowflake-compatible transportation model...")
    try:
        sf_model = create_snowflake_model(
            config_template_file='./configs/constraints.json'
        )
        print("✅ Model created successfully")
    except Exception as e:
        print(f"❌ Failed to create model: {e}")
        print("Make sure the feature store is set up first with 'python main.py --setup-fs'")
        return
    
    # Step 2: Test the model locally first
    print("Testing model locally...")
    
    # Use SQL mode test input for signature inference (matches how SQL will call the model)
    test_input = pd.DataFrame([{
        'scenario_name': 'registration_test',
        'warehouse_a_capacity': 100,
        'warehouse_b_capacity': 80,
        'customer_1_demand': 70,
        'customer_2_demand': 60,
        'cost_a_to_1': 10.0,  # Pre-joined cost parameters (required for SQL)
        'cost_a_to_2': 12.0,
        'cost_b_to_1': 15.0,
        'cost_b_to_2': 8.0,
        'feature_timestamp': datetime.now()
    }])
    
    print("*"*60)
    print("Testing model in SQL mode for signature inference...")
    print("*"*60)
    try:
        # Test with SQL mode first to ensure signature is captured correctly
        sql_model = create_snowflake_model(
            config_template_file='./configs/constraints.json',
            mode='sql'  # Force SQL mode for signature inference
        )
        test_result = sql_model.predict(test_input)
        print(f"✅ SQL mode test result: ${test_result.loc[0, 'optimal_cost']:.2f}")
        print(f"   Source: {test_result.loc[0, 'cost_matrix_source']}, Mode: {test_result.loc[0, 'execution_mode']}")
        
        # Use the SQL model for registration (since SQL will call it this way)
        sf_model = sql_model
        
    except Exception as e:
        print(f"❌ SQL mode test failed: {e}")
        print("🔄 Falling back to auto mode...")
        
        # Fallback to auto mode
        try:
            sf_model = create_snowflake_model(
                config_template_file='./configs/constraints.json'
            )
            test_result = sf_model.predict(test_input)
            print(f"✅ Auto mode test result: ${test_result.loc[0, 'optimal_cost']:.2f}")
            print(f"   Source: {test_result.loc[0, 'cost_matrix_source']}, Mode: {test_result.loc[0, 'execution_mode']}")
        except Exception as e2:
            print(f"❌ Both SQL and auto mode tests failed. Auto error: {e2}")
            return
    
    # Step 3: Connect to Snowflake model registry
    try:
        model_registry = get_model_registry()
        print("✅ Connected to model registry")
    except Exception as e:
        print(f"❌ Failed to connect to model registry: {e}")
        return
    
    # Step 4: Register the model
    print("Registering model with Snowflake...")
    
    current_dir = os.getcwd()
    models_path = os.path.join(current_dir, "models")

    try:
        model_version = model_registry.log_model(
            model=sf_model,
            model_name="TRANSPORTATION_OPTIMIZER_FS",  # Feature Store version
            version_name=f"{os.getenv('MODEL_VERSION')}",
            code_paths=[models_path],  # Use code_paths for local directories
            conda_dependencies=["pulp>=2.7.0"],  # Actual package dependencies
            sample_input_data=test_input,
            comment="Transportation optimization model with Snowflake Feature Store integration"
        )
        
        print(f"✅ Model registered successfully!")
        print(f"Model name: TRANSPORTATION_OPTIMIZER_FS")
        print(f"Version: {os.getenv('MODEL_VERSION')}") 
        print(f"Model version: {model_version}")
        
        print("\n📋 Usage in Snowflake SQL:")
        print(f"""
        -- Join with feature store views to get cost matrix
        WITH feature_costs AS (
        SELECT 
            MAX(CASE WHEN warehouse = 'Warehouse_A' AND customer = 'Customer_1' THEN composite_cost END) as cost_a_to_1,
            MAX(CASE WHEN warehouse = 'Warehouse_A' AND customer = 'Customer_2' THEN composite_cost END) as cost_a_to_2,
            MAX(CASE WHEN warehouse = 'Warehouse_B' AND customer = 'Customer_1' THEN composite_cost END) as cost_b_to_1,
            MAX(CASE WHEN warehouse = 'Warehouse_B' AND customer = 'Customer_2' THEN composite_cost END) as cost_b_to_2,
            MAX(feature_timestamp) as feature_timestamp
        FROM "COST_MATRIX_FEATURES$1.0"  -- This is your feature store view
        ),
        mv AS MODEL "TRANSPORTATION_OPTIMIZER_FS" VERSION "V1_6",
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
            s.*,
            f.*,
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
        """)
        
    except Exception as e:
        print(f"❌ Model registration failed: {e}")
