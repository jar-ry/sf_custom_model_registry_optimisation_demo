"""
Example: Registering Transportation LP Model with Snowflake Model Registry
=========================================================================

This script demonstrates how to register your transportation optimization
model with Snowflake's model registry.
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
    3. Appropriate permissions
    """
    
    # Step 1: Create the Snowflake-compatible model
    print("Creating Snowflake-compatible transportation model...")
    sf_model = create_snowflake_model(
        config_template_file='./configs/constraints.json',
        cost_matrix_file='./data/cost_matrix.csv'
    )
    
    # Step 2: Test the model locally first
    print("Testing model locally...")
    test_input = pd.DataFrame([{
        'scenario_name': 'registration_test',
        'warehouse_a_capacity': 100,
        'warehouse_b_capacity': 80,
        'customer_1_demand': 70,
        'customer_2_demand': 60,
        'cost_a_to_1': 5,
        'cost_a_to_2': 8,
        'cost_b_to_1': 6,
        'cost_b_to_2': 4
    }])
    
    test_result = sf_model.predict(test_input)
    print(f"Test result: ${test_result.loc[0, 'optimal_cost']:.2f}")
    
    # Step 3: Connect to Snowflake model registry
    # Note: You need to establish a Snowflake session first
    model_registry = get_model_registry()
    
    # Step 4: Register the model
    print("Registering model with Snowflake...")
    
    current_dir = os.getcwd()
    models_path = os.path.join(current_dir, "models")

    model_version = model_registry.log_model(
        model=sf_model,
        model_name="transportation_optimizer",
        version_name="V1_3",
        comment="Transportation linear programming optimization model",
        sample_input_data=test_input,
        conda_dependencies=["pulp>=2.7.0"],
        code_paths=[models_path]
    )

    set_default_version_sql = """
    ALTER MODEL MLOPS_DATABASE.MLOPS_SCHEMA.transportation_optimizer
    SET DEFAULT_VERSION = 'V1_3';
    """

    results = get_snowflake_connection().cursor().execute(set_default_version_sql)
    print(results)
    
    print("Model registered successfully!")
    print("You can now use it for inference in Snowflake")
    
    return True

