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
    test_input = pd.DataFrame([{
        'scenario_name': 'registration_test',
        'warehouse_a_capacity': 100,
        'warehouse_b_capacity': 80,
        'customer_1_demand': 70,
        'customer_2_demand': 60,
        'use_feature_store': True  # Use feature store for cost matrix
    }])
    
    try:
        test_result = sf_model.predict(test_input)
        print(f"Test result: ${test_result.loc[0, 'optimal_cost']:.2f}")
        print(f"Cost matrix source: {test_result.loc[0, 'cost_matrix_source']}")
    except Exception as e:
        print(f"❌ Model test failed: {e}")
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
            version_name="V1_0",
            conda_dependencies=[models_path],
            sample_input_data=test_input,
            comment="Transportation optimization model with Snowflake Feature Store integration"
        )
        
        print(f"✅ Model registered successfully!")
        print(f"Model name: TRANSPORTATION_OPTIMIZER_FS")
        print(f"Version: V1_0") 
        print(f"Model version: {model_version}")
        
        print("\n📋 Usage in Snowflake SQL:")
        print("""
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
        FROM transportation_scenarios;
        """)
        
    except Exception as e:
        print(f"❌ Model registration failed: {e}")


if __name__ == "__main__":
    register_transportation_model()

