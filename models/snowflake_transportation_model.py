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
from typing import Dict, Any
from datetime import datetime
from snowflake.ml.model import custom_model
from models.transportation_lp import TransportationLP

class SnowflakeTransportationModel(custom_model.CustomModel):
    """
    Snowflake custom model wrapper for transportation optimization.
    
    Enhanced with Feature Store integration for dynamic cost matrix retrieval.
    Uses only feature store for cost matrices - no default cost matrix fallback.
    
    Input format (DataFrame columns):
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
    
    Note: If cost parameters are not provided, costs will be retrieved from feature store.
    
    Output format (DataFrame columns):
    - scenario_name: Input scenario name
    - optimal_cost: Minimum total transportation cost
    - feasible: Whether a solution was found
    - shipment_a_to_1: Units shipped from A to Customer 1
    - shipment_a_to_2: Units shipped from A to Customer 2
    - shipment_b_to_1: Units shipped from B to Customer 1
    - shipment_b_to_2: Units shipped from B to Customer 2
    - warehouse_a_utilization: Percentage of Warehouse A capacity used
    - warehouse_b_utilization: Percentage of Warehouse B capacity used
    - cost_matrix_source: Source of cost matrix (feature_store or override)
    - feature_timestamp: Timestamp used for feature lookup (if applicable)
    """
    
    def __init__(self, context: custom_model.ModelContext) -> None:
        super().__init__(context)
        # Load base configuration template
        if 'config_template_file' in self.context.artifacts:
            config_file = self.context.artifacts['config_template_file']
        else:
            # Default to the standard config file
            config_file = './configs/constraints.json'
        
        with open(config_file, 'r') as f:
            self.base_config = json.load(f)
            
        # Initialize feature store manager - required for this model
        self.feature_store_manager = None
        self._init_feature_store()
        
        if self.feature_store_manager is None:
            raise ValueError("Feature store is required for this model. Please ensure feature store is properly configured.")
    
    def _init_feature_store(self):
        """Initialize feature store manager."""
        try:
            from helper.feature_store_utils import TransportationFeatureStore
            self.feature_store_manager = TransportationFeatureStore()
        except ImportError as e:
            raise ImportError("Feature store utilities not available. Please ensure Snowflake ML dependencies are installed.") from e
        except Exception as e:
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
            raise ValueError("Feature store not available")
        
        cost_matrix = self.feature_store_manager.get_latest_cost_matrix(feature_timestamp)
        return cost_matrix

    @custom_model.inference_api
    def predict(self, input: pd.DataFrame) -> pd.DataFrame:
        """
        Run transportation optimization for each scenario in the input.
        
        Args:
            input: DataFrame with scenario parameters
            
        Returns:
            DataFrame with optimization results
        """
        results = []
        
        for _, row in input.iterrows():
            try:
                # Extract scenario parameters
                scenario_name = row.get('scenario_name', f'scenario_{len(results)}')
                use_feature_store = row.get('use_feature_store', True)
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
                
                # Determine cost matrix source
                cost_matrix_source = "feature_store"
                
                # Check if individual cost overrides are provided
                has_cost_overrides = any(col in row for col in ['cost_a_to_1', 'cost_a_to_2', 'cost_b_to_1', 'cost_b_to_2'])
                
                if has_cost_overrides and not use_feature_store:
                    # Use override costs only if feature store is explicitly disabled
                    cost_matrix = self._get_cost_matrix_from_feature_store(feature_timestamp)
                    # Apply overrides on top of feature store data
                    if 'cost_a_to_1' in row:
                        cost_matrix.loc['Warehouse_A', 'Customer_1'] = row['cost_a_to_1']
                    if 'cost_a_to_2' in row:
                        cost_matrix.loc['Warehouse_A', 'Customer_2'] = row['cost_a_to_2']
                    if 'cost_b_to_1' in row:
                        cost_matrix.loc['Warehouse_B', 'Customer_1'] = row['cost_b_to_1']
                    if 'cost_b_to_2' in row:
                        cost_matrix.loc['Warehouse_B', 'Customer_2'] = row['cost_b_to_2']
                    cost_matrix_source = "override"
                else:
                    # Always use feature store for cost matrix
                    cost_matrix = self._get_cost_matrix_from_feature_store(feature_timestamp)
                    cost_matrix_source = "feature_store"
                
                # Create temporary files for the LP model
                cost_data = []
                for warehouse in cost_matrix.index:
                    for customer in cost_matrix.columns:
                        cost_data.append({
                            'warehouse': warehouse,
                            'customer': customer, 
                            'cost_per_unit': cost_matrix.loc[warehouse, customer]
                        })
                
                # Create temporary CSV file
                with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
                    pd.DataFrame(cost_data).to_csv(f.name, index=False)
                    temp_csv = f.name
                
                # Create temporary JSON config file
                with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                    json.dump(config, f)
                    temp_json = f.name
                
                try:
                    # Run the optimization
                    model = TransportationLP(temp_csv, temp_json)
                    success = model.solve()
                    
                    if success:
                        summary = model.get_solution_summary()
                        
                        result = {
                            'scenario_name': scenario_name,
                            'optimal_cost': summary['total_cost'],
                            'feasible': True,
                            'shipment_a_to_1': summary['shipments']['Warehouse_A']['Customer_1'],
                            'shipment_a_to_2': summary['shipments']['Warehouse_A']['Customer_2'],
                            'shipment_b_to_1': summary['shipments']['Warehouse_B']['Customer_1'],
                            'shipment_b_to_2': summary['shipments']['Warehouse_B']['Customer_2'],
                            'warehouse_a_utilization': summary['warehouse_utilization']['Warehouse_A']['utilization_rate'],
                            'warehouse_b_utilization': summary['warehouse_utilization']['Warehouse_B']['utilization_rate'],
                            'cost_matrix_source': cost_matrix_source,
                            'feature_timestamp': feature_timestamp.isoformat() if feature_timestamp else None
                        }
                    else:
                        result = {
                            'scenario_name': scenario_name,
                            'optimal_cost': None,
                            'feasible': False,
                            'shipment_a_to_1': 0,
                            'shipment_a_to_2': 0,
                            'shipment_b_to_1': 0,
                            'shipment_b_to_2': 0,
                            'warehouse_a_utilization': 0,
                            'warehouse_b_utilization': 0,
                            'cost_matrix_source': cost_matrix_source,
                            'feature_timestamp': feature_timestamp.isoformat() if feature_timestamp else None
                        }
                
                finally:
                    # Clean up temporary files
                    if os.path.exists(temp_csv):
                        os.unlink(temp_csv)
                    if os.path.exists(temp_json):
                        os.unlink(temp_json)
                        
            except Exception as e:
                # Handle errors gracefully
                result = {
                    'scenario_name': scenario_name,
                    'optimal_cost': None,
                    'feasible': False,
                    'error': str(e),
                    'shipment_a_to_1': 0,
                    'shipment_a_to_2': 0,
                    'shipment_b_to_1': 0,
                    'shipment_b_to_2': 0,
                    'warehouse_a_utilization': 0,
                    'warehouse_b_utilization': 0,
                    'cost_matrix_source': "error",
                    'feature_timestamp': None
                }
            
            results.append(result)
        
        return pd.DataFrame(results)


def create_snowflake_model(config_template_file: str = None) -> SnowflakeTransportationModel:
    """
    Factory function to create a Snowflake-compatible transportation model.
    
    Args:
        config_template_file: Optional path to config template JSON file
                             (defaults to 'config/constraints.json')
        
    Returns:
        SnowflakeTransportationModel instance ready for registry
    """
    context_kwargs = {}
    
    if config_template_file and os.path.exists(config_template_file):
        context_kwargs['config_template_file'] = config_template_file
    
    model_context = custom_model.ModelContext(**context_kwargs)
    return SnowflakeTransportationModel(model_context)
