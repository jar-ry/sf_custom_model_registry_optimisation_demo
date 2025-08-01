"""
Snowflake Custom Model Wrapper for Transportation Linear Programming
===================================================================

This module wraps the TransportationLP model to be compatible with 
Snowflake's model registry using the custom model framework.
"""

import pickle
import pandas as pd
import json
import tempfile
import os
from typing import Dict, Any
from snowflake.ml.model import custom_model
from models.transportation_lp import TransportationLP
from helper.model_utils import generate_cost_matrix

class SnowflakeTransportationModel(custom_model.CustomModel):
    """
    Snowflake custom model wrapper for transportation optimization.
    
    This model uses the existing config template from config/constraints.json
    and allows runtime parameter overrides for different scenarios.
    
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
    
    Note: If parameters are not provided, defaults from config template are used.
    
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
        
        # Load any pre-trained components or configurations if needed
        if 'cost_matrix_file' in self.context.artifacts:
            self.default_cost_matrix = pd.read_csv(self.context.artifacts['cost_matrix_file'])
        else:
            # Default to generate cost matrix
            self.default_cost_matrix = generate_cost_matrix()
    
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
                
                # Create dynamic cost matrix if provided
                cost_matrix = self.default_cost_matrix.copy()
                if 'cost_a_to_1' in row:
                    cost_matrix.loc['Warehouse_A', 'Customer_1'] = row['cost_a_to_1']
                if 'cost_a_to_2' in row:
                    cost_matrix.loc['Warehouse_A', 'Customer_2'] = row['cost_a_to_2']
                if 'cost_b_to_1' in row:
                    cost_matrix.loc['Warehouse_B', 'Customer_1'] = row['cost_b_to_1']
                if 'cost_b_to_2' in row:
                    cost_matrix.loc['Warehouse_B', 'Customer_2'] = row['cost_b_to_2']
                
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
                            'warehouse_b_utilization': summary['warehouse_utilization']['Warehouse_B']['utilization_rate']
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
                            'warehouse_b_utilization': 0
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
                    'warehouse_b_utilization': 0
                }
            
            results.append(result)
        
        return pd.DataFrame(results)


def create_snowflake_model(
    cost_matrix_file: str = None, 
    config_template_file: str = None
) -> SnowflakeTransportationModel:
    """
    Factory function to create a Snowflake-compatible transportation model.
    
    Args:
        cost_matrix_file: Optional path to pickled cost matrix file
        config_template_file: Optional path to config template JSON file
                             (defaults to 'config/constraints.json')
        
    Returns:
        SnowflakeTransportationModel instance ready for registry
    """
    context_kwargs = {}
    
    if cost_matrix_file and os.path.exists(cost_matrix_file):
        context_kwargs['cost_matrix_file'] = cost_matrix_file
    
    if config_template_file and os.path.exists(config_template_file):
        context_kwargs['config_template_file'] = config_template_file
    
    model_context = custom_model.ModelContext(**context_kwargs)
    return SnowflakeTransportationModel(model_context)
