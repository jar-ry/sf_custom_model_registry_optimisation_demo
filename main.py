from helper.register_with_snowflake import register_transportation_model
from helper.model_utils import create_sample_scenarios_table, example_model_usage_in_snowflake, generate_cost_matrix
from models.snowflake_transportation_model import create_snowflake_model

import os
from dotenv import load_dotenv
import argparse
import sys

def load_environment():
        """Load environment variables from .env file."""
        env_file = os.path.join(os.path.dirname(__file__), '.env')
        if os.path.exists(env_file):
            load_dotenv(env_file)
        else:
            print("No .env file found. Using system environment variables.")
    
def test_model():
    print("Transportation LP Model - Snowflake Integration Demo")
    
    # Test model creation and local inference
    model = create_snowflake_model(
        config_template_file='./configs/constraints.json',
        cost_matrix_file='./data/cost_matrix.csv'
    )
    scenarios = create_sample_scenarios_table()
    
    print(f"Testing {len(scenarios)} scenarios...")
    results = model.predict(scenarios)
    
    print("\nResults Summary:")
    for _, row in results.iterrows():
        feasible = "✅" if row['feasible'] else "❌"
        cost = f"${row['optimal_cost']:.2f}" if row['feasible'] else "N/A"
        print(f"{feasible} {row['scenario_name']}: {cost}")
    
    print(f"\nDetailed Results:")
    print(results[['scenario_name', 'optimal_cost', 'feasible', 
                   'warehouse_a_utilization', 'warehouse_b_utilization']])
    
if __name__ == "__main__":
    load_environment()
    
    # Using args to either test the model or register it with Snowflake or run example to use the model in Snowflake with 3 args
    # Check if the args are provided
    if len(sys.argv) == 1:
        print("No arguments provided. Please provide one of the following arguments:")
        print("--test: Test the model locally")
        print("--register: Register the model with Snowflake")
        print("--example: Run example to use the model in Snowflake")
        print("--cost-matrix: Test cost matrix generation from transportation data")
        print("--demo: Run complete cost matrix integration demo")
        sys.exit(1)
    
    # Parse the args
    parser = argparse.ArgumentParser(description='Transportation LP Model - Snowflake Integration Demo')
    parser.add_argument('--test', action='store_true', help='Test the model')
    parser.add_argument('--register', action='store_true', help='Register the model with Snowflake')
    parser.add_argument('--example', action='store_true', help='Run example to use the model in Snowflake')
    parser.add_argument('--cost-matrix', action='store_true', help='Test cost matrix generation from transportation data')
    args = parser.parse_args()  

    # Run the model with the args
    if args.test:
        test_model()
    elif args.register:
        register_transportation_model()
    elif args.example:
        example_model_usage_in_snowflake()
    elif args.cost_matrix:
        generate_cost_matrix()
    