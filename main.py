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
    print("Transportation LP Model - Hybrid Mode Demo")
    print("Testing both Python mode (with feature store) and SQL mode (pre-joined data)")
    
    try:
        # Test 1: Auto mode (should detect local environment → Python mode)
        print("\n" + "="*60)
        print("🔍 Test 1: Auto Mode (Python mode expected)")
        print("="*60)
        
        model_auto = create_snowflake_model(
            config_template_file='./configs/constraints.json',
            mode='auto'
        )
        
        scenarios_python = create_sample_scenarios_table()
        scenarios_python['use_feature_store'] = True  # Enable feature store lookup
        
        try:
            results_auto = model_auto.predict(scenarios_python)
            print("✅ Auto mode test successful!")
            print(f"   Detected mode: {results_auto.loc[0, 'execution_mode']}")
            print(f"   Cost source: {results_auto.loc[0, 'cost_matrix_source']}")
        except Exception as auto_error:
            print(f"⚠️ Auto mode failed: {auto_error}")
            results_auto = None
        
        # Test 2: Explicit Python mode
        print("\n" + "="*60)
        print("🐍 Test 2: Explicit Python Mode")
        print("="*60)
        
        model_python = create_snowflake_model(
            config_template_file='./configs/constraints.json',
            mode='python'
        )
        
        try:
            results_python = model_python.predict(scenarios_python)
            print("✅ Python mode test successful!")
            print(f"   Mode: {results_python.loc[0, 'execution_mode']}")
            print(f"   Cost source: {results_python.loc[0, 'cost_matrix_source']}")
        except Exception as python_error:
            print(f"⚠️ Python mode failed: {python_error}")
            results_python = None
        
        # Test 3: SQL mode (simulating UDF execution)
        print("\n" + "="*60)
        print("🗄️ Test 3: SQL Mode (simulating UDF execution)")
        print("="*60)
        
        model_sql = create_snowflake_model(
            config_template_file='./configs/constraints.json',
            mode='sql'
        )
        
        # Add cost matrix data for SQL mode
        scenarios_sql = create_sample_scenarios_table()
        scenarios_sql['cost_a_to_1'] = 10.0
        scenarios_sql['cost_a_to_2'] = 12.0
        scenarios_sql['cost_b_to_1'] = 15.0
        scenarios_sql['cost_b_to_2'] = 8.0
        
        try:
            results_sql = model_sql.predict(scenarios_sql)
            print("✅ SQL mode test successful!")
            print(f"   Mode: {results_sql.loc[0, 'execution_mode']}")
            print(f"   Cost source: {results_sql.loc[0, 'cost_matrix_source']}")
        except Exception as sql_error:
            print(f"❌ SQL mode failed: {sql_error}")
            results_sql = None
        
        # Display results summary
        print("\n" + "="*60)
        print("📊 Results Summary")
        print("="*60)
        
        successful_results = []
        if results_auto is not None:
            successful_results.append(("Auto", results_auto))
        if results_python is not None:
            successful_results.append(("Python", results_python))
        if results_sql is not None:
            successful_results.append(("SQL", results_sql))
        
        if successful_results:
            for test_name, results in successful_results:
                print(f"\n{test_name} Mode Results:")
                for _, row in results.iterrows():
                    feasible = "✅" if row['feasible'] else "❌"
                    cost = f"${row['optimal_cost']:.2f}" if row['feasible'] else "N/A"
                    mode = row['execution_mode']
                    source = row['cost_matrix_source']
                    print(f"  {feasible} {row['scenario_name']}: {cost} (mode: {mode}, source: {source})")
        else:
            print("❌ All tests failed. Please check feature store setup.")
    
    except Exception as e:
        print(f"❌ Test setup failed: {e}")
        print("Make sure to run 'python main.py --setup-fs' first to set up the feature store.")

def setup_feature_store():
    """Set up the transportation feature store with initial data."""
    print("Setting up Transportation Feature Store...")
    print("\n⚠️  PREREQUISITE: Make sure you've run the SQL script first:")
    print("   Execute sql/setup_transportation_table.sql in Snowflake to create the transportation_data table")
    print("")
    
    # Check execution environment
    try:
        from helper.snowflake_utils import is_running_in_snowflake
        env = "Snowflake" if is_running_in_snowflake() else "Local"
        print(f"🌍 Detected execution environment: {env}")
    except:
        print("🌍 Could not detect execution environment")
    
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
    """Test the model with feature store integration in Python mode."""
    print("Testing Transportation Model with Feature Store Integration (Python Mode)...")
    
    try:
        # Create model explicitly in Python mode for feature store testing
        model = create_snowflake_model(
            config_template_file='./configs/constraints.json',
            mode='python'  # Explicitly use Python mode
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
            mode = row['execution_mode']
            source = row['cost_matrix_source']
            print(f"{feasible} {row['scenario_name']}: {cost} (mode: {mode}, source: {source})")
        
        print(f"\nFeature Store Usage Summary:")
        mode_counts = results['execution_mode'].value_counts()
        source_counts = results['cost_matrix_source'].value_counts()
        print(f"Execution modes: {dict(mode_counts)}")
        print(f"Cost sources: {dict(source_counts)}")
        
        print(f"\nDetailed Results:")
        print(results[['scenario_name', 'optimal_cost', 'feasible', 
                       'warehouse_a_utilization', 'warehouse_b_utilization', 
                       'cost_matrix_source', 'execution_mode']])
        
    except Exception as e:
        print(f"❌ Feature store model test failed: {e}")
        print("💡 Common issues:")
        print("   1. Feature store not set up - run 'python main.py --setup-fs' first")
        print("   2. Transportation data table missing - run sql/setup_transportation_table.sql")
        print("   3. Snowflake credentials not configured")

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
    if args.setup_fs:
        setup_feature_store()
    elif args.test_fs:
        test_feature_store_model()
    elif args.test_override_fs:
        test_model_override_feature_store()
    elif args.register:
        register_transportation_model()
    elif args.example:
        example_model_usage_in_snowflake()