"""
Snowflake Custom Model Wrapper for Transportation Linear Programming
===================================================================

This module wraps the TransportationLP model to be compatible with 
Snowflake's model registry using the custom model framework.
Enhanced with Feature Store integration for dynamic cost matrix retrieval.
"""

import pickle
import pandas as pd
import json
import tempfile
import os
import logging
from typing import Dict, Any
from datetime import datetime
from snowflake.ml.model import custom_model

logger = logging.getLogger(__name__)

class SnowflakeTransportationModel(custom_model.CustomModel):
    """
    Snowflake custom model wrapper for transportation optimization.
    
    Hybrid model that supports both programmatic Python usage and SQL UDF execution:
    - 'python' mode: Full feature store integration for local/SPCS execution
    - 'sql' mode: Expects pre-joined cost matrix data for UDF execution
    - 'auto' mode: Automatically detects execution context (default)
    
    Input format varies by mode:
    
    Python Mode (local/SPCS):
    - scenario_name: Name of the optimization scenario
    - warehouse_a_capacity: (Optional) Override Warehouse A capacity  
    - warehouse_b_capacity: (Optional) Override Warehouse B capacity
    - customer_1_demand: (Optional) Override Customer 1 demand
    - customer_2_demand: (Optional) Override Customer 2 demand
    - cost_a_to_1: (Optional) Override cost from Warehouse A to Customer 1
    - cost_a_to_2: (Optional) Override cost from Warehouse A to Customer 2
    - cost_b_to_1: (Optional) Override cost from Warehouse B to Customer 1
    - cost_b_to_2: (Optional) Override cost from Warehouse B to Customer 2
    - use_feature_store: (Optional) Use feature store for cost matrix (default: True)
    - feature_timestamp: (Optional) Point-in-time timestamp for feature lookup
    
    SQL Mode (UDF execution):
    - scenario_name: Name of the optimization scenario
    - warehouse_a_capacity: (Optional) Override Warehouse A capacity  
    - warehouse_b_capacity: (Optional) Override Warehouse B capacity
    - customer_1_demand: (Optional) Override Customer 1 demand
    - customer_2_demand: (Optional) Override Customer 2 demand
    - cost_a_to_1: Cost from Warehouse A to Customer 1 (from feature store join)
    - cost_a_to_2: Cost from Warehouse A to Customer 2 (from feature store join)
    - cost_b_to_1: Cost from Warehouse B to Customer 1 (from feature store join)
    - cost_b_to_2: Cost from Warehouse B to Customer 2 (from feature store join)
    - feature_timestamp: (Optional) Timestamp used for feature lookup (for audit trail)
    
    Output format (consistent across modes):
    - scenario_name: Input scenario name
    - optimal_cost: Minimum total transportation cost
    - feasible: Whether a solution was found
    - shipment_a_to_1: Units shipped from A to Customer 1
    - shipment_a_to_2: Units shipped from A to Customer 2
    - shipment_b_to_1: Units shipped from B to Customer 1
    - shipment_b_to_2: Units shipped from B to Customer 2
    - warehouse_a_utilization: Percentage of Warehouse A capacity used
    - warehouse_b_utilization: Percentage of Warehouse B capacity used
    - cost_matrix_source: Source of cost matrix (feature_store, override, sql_input, etc.)
    - feature_timestamp: Timestamp used for feature lookup (if applicable)
    - execution_mode: Mode used for this prediction (python/sql)
    """
    
    def __init__(self, context: custom_model.ModelContext, mode: str = 'auto') -> None:
        """
        Initialize the transportation model with mode detection.
        
        Args:
            context: Snowflake model context
            mode: Execution mode - 'auto', 'python', or 'sql'
                  'auto': Automatically detect based on execution context
                  'python': Force feature store integration for programmatic usage
                  'sql': Force pre-joined data mode for UDF execution
        """
        super().__init__(context)
        
        # Load base configuration
        if 'config_template_file' in self.context.artifacts:
            config_file = self.context.artifacts['config_template_file']
        else:
            config_file = './configs/constraints.json'
        
        with open(config_file, 'r') as f:
            self.base_config = json.load(f)
        
        # Determine execution mode
        self.mode = self._determine_mode(mode)
        logger.info(f"🎯 Model initialized in '{self.mode}' mode")
        
        # Initialize feature store conditionally
        self.feature_store_manager = None
        self._feature_store_initialized = False
        self._initialization_error = None
        
        if self.mode == 'python':
            logger.info("🔧 Initializing feature store for Python mode...")
            self._ensure_feature_store()
        else:
            logger.info("📊 SQL mode - feature store will be bypassed (expecting pre-joined data)")
    
    def _determine_mode(self, requested_mode: str) -> str:
        """
        Determine the actual execution mode based on request and context.
        
        Args:
            requested_mode: User-requested mode ('auto', 'python', 'sql')
            
        Returns:
            Actual mode to use ('python' or 'sql')
        """
        if requested_mode in ['python', 'sql']:
            logger.info(f"🎛️ Using explicitly requested mode: {requested_mode}")
            return requested_mode
        
        # Auto-detection logic
        if self._is_udf_context():
            detected_mode = 'sql'
            logger.info("🔍 Auto-detected UDF execution context → SQL mode")
        else:
            detected_mode = 'python'
            logger.info("🔍 Auto-detected local/SPCS execution context → Python mode")
        
        return detected_mode
    
    def _is_udf_context(self) -> bool:
        """
        Detect if we're running in a UDF execution context.
        
        Key distinction:
        - Local environment: Can create sessions using credentials (even if no active session)
        - SPCS environment: Has active session available
        - UDF environment: Cannot create sessions AND no active session
        
        Returns:
            True if running as UDF, False if running locally/SPCS
        """
        try:
            import os
            
            # Method 1: Check for explicit UDF environment indicators
            # These are set by Snowflake specifically in UDF execution context
            udf_indicators = [
                'SNOWFLAKE_WAREHOUSE_ID',
                'SNOWFLAKE_CLUSTER_ID', 
                'SNOWFLAKE_SESSION_ID',
                'SNOWFLAKE_QUERY_ID'
            ]
            
            udf_env_vars = [var for var in udf_indicators if os.getenv(var)]
            if udf_env_vars:
                logger.info(f"🔍 UDF environment indicators found: {udf_env_vars} → SQL mode")
                return True
            
            # Method 2: Try to get active session (works in SPCS)
            try:
                from snowflake.snowpark.context import get_active_session
                session = get_active_session()
                
                # If we can get current database/schema, we're in SPCS or similar
                current_db = session.get_current_database()
                current_schema = session.get_current_schema()
                logger.info(f"🔍 Active session detected - DB: {current_db}, Schema: {current_schema} → Python mode")
                return False
                
            except ImportError:
                # get_active_session not available - likely local environment
                logger.info("🔍 get_active_session not available → Local environment → Python mode")
                return False
                
            except Exception as session_error:
                # No active session - need to distinguish between local and UDF
                logger.info(f"🔍 No active session: {session_error}")
                
                # Method 3: Try to determine if we can create a session (local vs UDF test)
                # Check if we have the basic credentials needed to create a session
                required_creds = ['SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USER']
                auth_creds = ['SNOWFLAKE_PASSWORD', 'SNOWFLAKE_PRIVATE_KEY_PATH']
                
                has_basic_creds = all(os.getenv(var) for var in required_creds)
                has_auth_creds = any(os.getenv(var) for var in auth_creds)
                
                if has_basic_creds and has_auth_creds:
                    logger.info("🔍 Credentials available for session creation → Local environment → Python mode")
                    return False
                else:
                    # No credentials available - could be UDF or misconfigured local
                    # Check for UDF-specific error patterns as additional signal
                    error_str = str(session_error).lower()
                    udf_error_indicators = [
                        'no default session is found',
                        'function execution',
                        'udf'
                    ]
                    
                    if any(indicator in error_str for indicator in udf_error_indicators):
                        logger.info(f"🔍 UDF-specific error pattern detected → SQL mode")
                        return True
                    else:
                        # Unclear situation - default to Python mode for better UX
                        logger.info(f"🔍 No credentials + unclear error → Defaulting to Python mode")
                        return False
                    
        except Exception as e:
            # If detection fails completely, default to Python mode
            logger.warning(f"🔍 Context detection failed: {e} → Defaulting to Python mode")
            return False
    
    def _ensure_feature_store(self):
        """Ensure feature store is initialized (for Python mode only)."""
        if self.mode != 'python':
            return  # Skip feature store initialization in SQL mode
            
        if self._feature_store_initialized:
            if self.feature_store_manager is None and self._initialization_error:
                raise self._initialization_error
            return
            
        try:
            self._init_feature_store()
            self._feature_store_initialized = True
            
            # Test feature store connectivity
            if self.feature_store_manager:
                logger.info("🧪 Testing feature store connectivity...")
                test_entity = self.feature_store_manager.route_entity
                logger.info(f"✅ Feature store test passed - entity: {test_entity.name}")
            
        except Exception as e:
            self._initialization_error = e
            self._feature_store_initialized = True
            logger.error(f"❌ Feature store initialization failed: {e}")
            raise RuntimeError(f"Could not initialize feature store: {e}") from e
    
    def _init_feature_store(self):
        """Initialize feature store manager."""
        try:
            from helper.feature_store_utils import TransportationFeatureStore
            from helper.snowflake_utils import is_running_in_snowflake
            
            # Check if we're running in Snowflake
            if is_running_in_snowflake():
                logger.info("🌍 Initializing feature store in Snowflake execution environment")
                try:
                    # Try to get active session and pass it explicitly
                    from snowflake.snowpark.context import get_active_session
                    active_session = get_active_session()
                    # Get current database and schema from session
                    current_db = active_session.get_current_database()
                    current_schema = active_session.get_current_schema()
                    logger.info(f"📍 Using session context: {current_db}.{current_schema}")
                    self.feature_store_manager = TransportationFeatureStore(current_db, current_schema)
                except Exception as e:
                    logger.warning(f"⚠️ Could not initialize with session context: {e}")
                    # Fall back to default initialization
                    self.feature_store_manager = TransportationFeatureStore()
            else:
                logger.info("🌍 Initializing feature store in local environment")
                self.feature_store_manager = TransportationFeatureStore()
                
        except ImportError as e:
            raise ImportError("Feature store utilities not available. Please ensure Snowflake ML dependencies are installed.") from e
        except Exception as e:
            logger.error(f"Failed to initialize feature store: {e}")
            raise RuntimeError(f"Could not initialize feature store: {e}") from e
    
    def _get_cost_matrix_from_feature_store(self, feature_timestamp: datetime = None) -> pd.DataFrame:
        """
        Retrieve cost matrix from feature store.
        
        Args:
            feature_timestamp: Point-in-time timestamp for feature lookup
            
        Returns:
            Cost matrix DataFrame
        """
        if self.feature_store_manager is None:
            raise ValueError("Feature store not available - ensure _ensure_feature_store() was called first")
        
        try:
            cost_matrix = self.feature_store_manager.get_latest_cost_matrix(feature_timestamp)
            return cost_matrix
        except Exception as e:
            logger.error(f"Failed to retrieve cost matrix from feature store: {e}")
            raise
    
    @custom_model.inference_api
    def predict(self, input: pd.DataFrame) -> pd.DataFrame:
        """
        Run transportation optimization for each scenario in the input.
        
        Behavior depends on execution mode:
        - Python mode: Uses feature store integration with optional overrides
        - SQL mode: Expects all cost matrix values as pre-joined inputs
        
        Args:
            input: DataFrame with scenario parameters (format depends on mode)
            
        Returns:
            DataFrame with optimization results
        """
        results = []
        
        for _, row in input.iterrows():
            scenario_name = "unknown"  # Initialize with default value
            try:
                # Extract scenario parameters
                scenario_name = row.get('scenario_name', f'scenario_{len(results)}')
                feature_timestamp = row.get('feature_timestamp', None)
                
                if feature_timestamp and isinstance(feature_timestamp, str):
                    feature_timestamp = datetime.fromisoformat(feature_timestamp)
                
                # Create dynamic configuration based on template
                config = json.loads(json.dumps(self.base_config))  # Deep copy
                
                # Override warehouse capacities if provided
                if 'warehouse_a_capacity' in row:
                    config['warehouses']['Warehouse_A']['capacity'] = int(row['warehouse_a_capacity'])
                if 'warehouse_b_capacity' in row:
                    config['warehouses']['Warehouse_B']['capacity'] = int(row['warehouse_b_capacity'])
                
                # Override customer demands if provided  
                if 'customer_1_demand' in row:
                    config['customers']['Customer_1']['demand'] = int(row['customer_1_demand'])
                if 'customer_2_demand' in row:
                    config['customers']['Customer_2']['demand'] = int(row['customer_2_demand'])
                
                # Get cost matrix based on execution mode
                logger.info(f"🎯 Processing scenario '{scenario_name}' in '{self.mode}' mode")
                if self.mode == 'python':
                    logger.info("🐍 Using Python mode for cost matrix retrieval")
                    cost_matrix, cost_matrix_source = self._get_cost_matrix_python_mode(row, feature_timestamp)
                else:  # self.mode == 'sql'
                    logger.info("🗄️ Using SQL mode for cost matrix retrieval")
                    cost_matrix, cost_matrix_source = self._get_cost_matrix_sql_mode(row)
                
                # Create temporary files for the LP model
                cost_data = []
                for warehouse in cost_matrix.index:
                    for customer in cost_matrix.columns:
                        cost_data.append({
                            'warehouse': warehouse,
                            'customer': customer,
                            'cost_per_unit': cost_matrix.loc[warehouse, customer]  # Changed from 'cost' to 'cost_per_unit'
                        })
                
                # Create temporary CSV for cost data
                with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
                    cost_df = pd.DataFrame(cost_data)
                    cost_df.to_csv(f.name, index=False)
                    temp_csv = f.name
                
                # Create temporary JSON for configuration
                with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                    json.dump(config, f)
                    temp_json = f.name
                
                try:
                    # Run the optimization
                    from models.transportation_lp import TransportationLP
                    lp_model = TransportationLP(
                        data_file=temp_csv,  # data_file parameter for cost data
                        config_file=temp_json  # config_file parameter for constraints
                    )
                    
                    # solve() returns boolean (True if optimal, False if not)
                    is_optimal = lp_model.solve()
                    
                    if is_optimal:
                        # Get detailed solution summary
                        solution = lp_model.get_solution_summary()
                        
                        result = {
                            'optimal_cost': solution['total_cost'],
                            'feasible': True,
                            'shipment_a_to_1': solution['shipments']['Warehouse_A']['Customer_1'],
                            'shipment_a_to_2': solution['shipments']['Warehouse_A']['Customer_2'], 
                            'shipment_b_to_1': solution['shipments']['Warehouse_B']['Customer_1'],
                            'shipment_b_to_2': solution['shipments']['Warehouse_B']['Customer_2'],
                            'warehouse_a_utilization': solution['warehouse_utilization']['Warehouse_A']['utilization_rate'] * 100,
                            'warehouse_b_utilization': solution['warehouse_utilization']['Warehouse_B']['utilization_rate'] * 100
                        }
                    else:
                        # Infeasible solution
                        result = {
                            'optimal_cost': None,
                            'feasible': False,
                            'shipment_a_to_1': 0,
                            'shipment_a_to_2': 0,
                            'shipment_b_to_1': 0,
                            'shipment_b_to_2': 0,
                            'warehouse_a_utilization': 0,
                            'warehouse_b_utilization': 0
                        }
                    
                    # Add scenario metadata to result
                    result['scenario_name'] = scenario_name
                    result['cost_matrix_source'] = cost_matrix_source
                    result['feature_timestamp'] = feature_timestamp
                    result['execution_mode'] = self.mode
                    
                finally:
                    # Clean up temporary files
                    try:
                        os.unlink(temp_csv)
                        os.unlink(temp_json)
                    except:
                        pass  # Ignore cleanup errors
                        
                results.append(result)
                
            except Exception as e:
                # Ensure scenario_name is never None for logging
                safe_scenario_name = scenario_name if scenario_name is not None else f"scenario_{len(results)}"
                logger.error(f"Error processing scenario {safe_scenario_name}: {e}")
                # Add error result
                error_result = {
                    'scenario_name': safe_scenario_name,
                    'optimal_cost': None,
                    'feasible': False,
                    'shipment_a_to_1': 0,
                    'shipment_a_to_2': 0,
                    'shipment_b_to_1': 0,
                    'shipment_b_to_2': 0,
                    'warehouse_a_utilization': 0,
                    'warehouse_b_utilization': 0,
                    'cost_matrix_source': "error",
                    'feature_timestamp': row.get('feature_timestamp', None),
                    'execution_mode': self.mode,
                    'error': str(e)
                }
                results.append(error_result)
        
        return pd.DataFrame(results)
    
    def _get_cost_matrix_python_mode(self, row: pd.Series, feature_timestamp: datetime = None) -> tuple[pd.DataFrame, str]:
        """
        Get cost matrix for Python mode with feature store integration.
        
        Args:
            row: Input row with scenario parameters
            feature_timestamp: Optional timestamp for point-in-time lookup
            
        Returns:
            Tuple of (cost_matrix_dataframe, source_description)
        """
        use_feature_store = row.get('use_feature_store', True)
        has_cost_overrides = any(col in row for col in ['cost_a_to_1', 'cost_a_to_2', 'cost_b_to_1', 'cost_b_to_2'])
        
        logger.info(f"🐍 Python mode cost retrieval: use_feature_store={use_feature_store}, has_cost_overrides={has_cost_overrides}")
        
        if has_cost_overrides and use_feature_store:
            # Use feature store as base with overrides
            logger.info("🔄 Using feature store as base with cost overrides")
            self._ensure_feature_store()
            base_cost_matrix = self._get_cost_matrix_from_feature_store(feature_timestamp)
            
            # Apply overrides
            if 'cost_a_to_1' in row:
                base_cost_matrix.loc['Warehouse_A', 'Customer_1'] = row['cost_a_to_1']
            if 'cost_a_to_2' in row:
                base_cost_matrix.loc['Warehouse_A', 'Customer_2'] = row['cost_a_to_2']
            if 'cost_b_to_1' in row:
                base_cost_matrix.loc['Warehouse_B', 'Customer_1'] = row['cost_b_to_1']
            if 'cost_b_to_2' in row:
                base_cost_matrix.loc['Warehouse_B', 'Customer_2'] = row['cost_b_to_2']
            
            return base_cost_matrix, "override"
            
        elif use_feature_store:
            # Use feature store only (no overrides)
            logger.info("🏪 Using feature store for cost matrix")
            self._ensure_feature_store()
            cost_matrix = self._get_cost_matrix_from_feature_store(feature_timestamp)
            return cost_matrix, "feature_store"
            
        else:
            # Use explicit overrides only (no feature store)
            logger.info("⚡ Using explicit cost overrides only")
            if not has_cost_overrides:
                raise ValueError("use_feature_store=False requires cost overrides. "
                               "Please provide cost_a_to_1, cost_a_to_2, cost_b_to_1, cost_b_to_2")
            
            cost_matrix = pd.DataFrame({
                'Customer_1': [row.get('cost_a_to_1', 10), row.get('cost_b_to_1', 15)],
                'Customer_2': [row.get('cost_a_to_2', 12), row.get('cost_b_to_2', 8)]
            }, index=['Warehouse_A', 'Warehouse_B'])
            
            return cost_matrix, "override_only"
    
    def _get_cost_matrix_sql_mode(self, row: pd.Series) -> tuple[pd.DataFrame, str]:
        """
        Get cost matrix for SQL mode expecting pre-joined data.
        
        Args:
            row: Input row with pre-joined cost matrix values
            
        Returns:
            Tuple of (cost_matrix_dataframe, source_description)
        """
        # Extract cost matrix from input (required in SQL mode)
        required_costs = ['cost_a_to_1', 'cost_a_to_2', 'cost_b_to_1', 'cost_b_to_2']
        missing_costs = [cost for cost in required_costs if cost not in row or pd.isna(row[cost])]
        
        if missing_costs:
            raise ValueError(f"SQL mode requires all cost parameters. Missing: {missing_costs}. "
                           f"Please join feature store views in SQL before calling the model.")
        
        # Create cost matrix from input parameters
        cost_matrix = pd.DataFrame({
            'Customer_1': [float(row['cost_a_to_1']), float(row['cost_b_to_1'])],
            'Customer_2': [float(row['cost_a_to_2']), float(row['cost_b_to_2'])]
        }, index=['Warehouse_A', 'Warehouse_B'])
        
        return cost_matrix, "sql_input"


def create_snowflake_model(config_template_file: str = None, mode: str = 'auto'):
    """
    Create and return a Snowflake-compatible transportation model.
    
    Args:
        config_template_file: Path to the constraints configuration file
        mode: Execution mode - 'auto', 'python', or 'sql'
              'auto': Automatically detect based on execution context (default)
              'python': Force feature store integration for programmatic usage
              'sql': Force pre-joined data mode for UDF execution
    
    Returns:
        SnowflakeTransportationModel: Configured model instance
        
    Examples:
        # Auto-detection (recommended)
        model = create_snowflake_model()
        
        # Force Python mode for local/SPCS usage
        model = create_snowflake_model(mode='python')
        
        # Force SQL mode for UDF testing
        model = create_snowflake_model(mode='sql')
    """
    # Create a mock context for local usage
    context = custom_model.ModelContext(artifacts={})
    if config_template_file:
        context.artifacts['config_template_file'] = config_template_file
    
    return SnowflakeTransportationModel(context, mode=mode)
