from helper.register_with_snowflake import register_transportation_model
from helper.model_utils import create_sample_scenarios_table, example_model_usage_in_snowflake
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
    
def test_model_override_feature_store():
    print("Transportation LP Model - Feature Store Demo")
    
    # Test model creation with feature store only
    try:
        model = create_snowflake_model(
            config_template_file='./configs/constraints.json'
        )
        scenarios = create_sample_scenarios_table()
        
        print(f"Testing {len(scenarios)} scenarios with feature store...")
        results = model.predict(scenarios)
        
        print("\nResults Summary:")
        for _, row in results.iterrows():
            feasible = "✅" if row['feasible'] else "❌"
            cost = f"${row['optimal_cost']:.2f}" if row['feasible'] else "N/A"
            source = row.get('cost_matrix_source', 'unknown')
            print(f"{feasible} {row['scenario_name']}: {cost} (source: {source})")
        
        print(f"\nDetailed Results:")
        print(results[['scenario_name', 'optimal_cost', 'feasible', 
                       'warehouse_a_utilization', 'warehouse_b_utilization', 'cost_matrix_source']])
    
    except Exception as e:
        print(f"❌ Model test failed: {e}")
        print("Make sure to run 'python main.py --setup-fs' first to set up the feature store.")

def setup_feature_store():
    """Set up the transportation feature store with initial data."""
    print("Setting up Transportation Feature Store...")
    print("\n⚠️  PREREQUISITE: Make sure you've run the SQL script first:")
    print("   Execute sql/setup_transportation_table.sql in Snowflake to create the transportation_data table")
    print("")
    
    try:
        from helper.feature_store_utils import setup_transportation_feature_store
        fs_manager = setup_transportation_feature_store()
        print("✅ Feature store setup complete!")
        
        # Test feature store retrieval
        print("\nTesting feature store retrieval...")
        cost_matrix = fs_manager.get_latest_cost_matrix()
        print("Current cost matrix from feature store:")
        print(cost_matrix)
        
    except ImportError:
        print("❌ Feature store utilities not available. Please ensure Snowflake ML dependencies are installed.")
    except Exception as e:
        print(f"❌ Feature store setup failed: {e}")
        print("\n💡 Common fixes:")
        print("   1. Run sql/setup_transportation_table.sql in Snowflake first")
        print("   2. Verify your Snowflake credentials and permissions")
        print("   3. Ensure the transportation_data table exists in your schema")

def test_feature_store_model():
    """Test the model with feature store integration."""
    print("Testing Transportation Model with Feature Store Integration...")
    
    try:
        # Create model with feature store support
        model = create_snowflake_model(
            config_template_file='./configs/constraints.json'
        )
        
        # Create test scenarios with feature store usage
        scenarios = create_sample_scenarios_table()
        scenarios['use_feature_store'] = True  # Enable feature store lookup
        
        print(f"Testing {len(scenarios)} scenarios with feature store...")
        results = model.predict(scenarios)
        
        print("\nResults Summary:")
        for _, row in results.iterrows():
            feasible = "✅" if row['feasible'] else "❌"
            cost = f"${row['optimal_cost']:.2f}" if row['feasible'] else "N/A"
            source = row.get('cost_matrix_source', 'unknown')
            print(f"{feasible} {row['scenario_name']}: {cost} (source: {source})")
        
        print(f"\nFeature Store Usage Summary:")
        source_counts = results['cost_matrix_source'].value_counts()
        for source, count in source_counts.items():
            print(f"  {source}: {count} scenarios")
            
    except Exception as e:
        print(f"❌ Feature store model test failed: {e}")

if __name__ == "__main__":
    load_environment()
    
    # Check if the args are provided
    if len(sys.argv) == 1:
        print("No arguments provided. Please provide one of the following arguments:")
        print("--setup-fs: Set up the transportation feature store (REQUIRED FIRST)")
        print("--test-override-fs: Test the model and override the feature store")
        print("--test-fs: Test the model with feature store integration")
        print("--register: Register the model with Snowflake")
        print("--example: Run example to use the model in Snowflake")
        sys.exit(1)
    
    # Parse the args
    parser = argparse.ArgumentParser(description='Transportation LP Model - Feature Store Demo')
    parser.add_argument('--test-override-fs', action='store_true', help='Test the model and override the feature store')
    parser.add_argument('--register', action='store_true', help='Register the model with Snowflake')
    parser.add_argument('--example', action='store_true', help='Run example to use the model in Snowflake')
    parser.add_argument('--setup-fs', action='store_true', help='Set up the transportation feature store')
    parser.add_argument('--test-fs', action='store_true', help='Test the model with feature store integration')
    args = parser.parse_args()  

    # Run the model with the args
    if args.test_override_fs:
        test_model_override_feature_store()
    elif args.register:
        register_transportation_model()
    elif args.example:
        example_model_usage_in_snowflake()
    elif args.setup_fs:
        setup_feature_store()
    elif args.test_fs:
        test_feature_store_model()
    