import os
import pandas as pd
import numpy as np
from helper.snowflake_utils import get_snowpark_session, get_snowflake_connection

def generate_cost_matrix_from_data(data_file='data/transportation_data.csv', model_type='composite', aggregation='mean'):
    """
    Process transportation data and generate cost matrix using mathematical models.
    
    Args:
        data_file (str): Path to the transportation data CSV file
        model_type (str): Type of cost model to use
            - 'time': Time-based cost model
            - 'composite': Comprehensive model combining all factors
        aggregation (str): How to aggregate multiple data points for same route
            - 'mean': Average of all observations
            - 'median': Median value
            - 'min': Minimum cost (optimistic)
            - 'max': Maximum cost (pessimistic)
    
    Returns:
        pd.DataFrame: Cost matrix with warehouses as rows and customers as columns
    """
    
    # Read the transportation data
    df = pd.read_csv(data_file)
    
    # Calculate costs using different mathematical models
    if model_type == 'time':
        # Time-based model: Cost = travel_time * hourly_rate * factors
        hourly_rate = 25  # Base hourly rate
        df['calculated_cost'] = (df['travel_time_hours'] * 
                               hourly_rate * 
                               df['seasonal_factor'] * 
                               df['priority_multiplier'])
    elif model_type == 'composite':
        # Comprehensive model combining distance, fuel, time, and other factors
        
        # Distance component
        distance_cost = df['distance_km'] * df['base_rate_per_km'] * df['road_condition_factor']
        
        # Fuel component  
        fuel_efficiency_base = 8
        fuel_efficiency = (fuel_efficiency_base * 
                          (df['vehicle_capacity_tons'] / 10) * 
                          (1 / df['road_condition_factor']))
        fuel_cost = (df['distance_km'] / fuel_efficiency) * df['fuel_price_per_liter']
        
        # Time component
        hourly_rate = 25
        time_cost = df['travel_time_hours'] * hourly_rate * 0.3  # Reduced weight for time
        
        # Vehicle capacity utilization factor (smaller vehicles cost more per unit)
        capacity_factor = 10 / df['vehicle_capacity_tons']  # Inverse relationship
        
        # Combine all components
        df['calculated_cost'] = ((distance_cost + fuel_cost + time_cost) * 
                               capacity_factor * 
                               df['seasonal_factor'] * 
                               df['priority_multiplier'])
    
    else:
        raise ValueError(f"Unknown model_type: {model_type}")
    
    # Aggregate costs by route (warehouse-customer pair)
    if aggregation == 'mean':
        cost_summary = df.groupby(['warehouse', 'customer'])['calculated_cost'].mean().reset_index()
    elif aggregation == 'median':
        cost_summary = df.groupby(['warehouse', 'customer'])['calculated_cost'].median().reset_index()
    elif aggregation == 'min':
        cost_summary = df.groupby(['warehouse', 'customer'])['calculated_cost'].min().reset_index()
    elif aggregation == 'max':
        cost_summary = df.groupby(['warehouse', 'customer'])['calculated_cost'].max().reset_index()
    else:
        raise ValueError(f"Unknown aggregation method: {aggregation}")
    
    # Create cost matrix
    cost_matrix = cost_summary.pivot(index='warehouse', columns='customer', values='calculated_cost')
    
    # Round to 2 decimal places for readability
    cost_matrix = cost_matrix.round(2)
    
    # Add some stats for analysis
    print(f"\nCost Matrix Generated using {model_type} model with {aggregation} aggregation:")
    print(f"Data points processed: {len(df)}")
    print(f"Unique routes: {len(cost_summary)}")
    print(f"Cost range: ${cost_matrix.min().min():.2f} - ${cost_matrix.max().max():.2f}")
    print("\nCost Matrix:")
    print(cost_matrix)
    
    return cost_matrix

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

    sql_batch_optimization = """
    -- Run batch optimization using the registered model
    WITH mv AS MODEL "TRANSPORTATION_OPTIMIZER" VERSION "V1_3"
    SELECT
    *,
    mv ! "PREDICT"(
        SCENARIO_NAME,
        WAREHOUSE_A_CAPACITY,
        WAREHOUSE_B_CAPACITY,
        CUSTOMER_1_DEMAND,
        CUSTOMER_2_DEMAND,
        COST_A_TO_1,
        COST_A_TO_2,
        COST_B_TO_1,
        COST_B_TO_2
    )
    FROM
    transportation_scenarios;
    """
    results = get_snowpark_session().sql(sql_batch_optimization).collect()
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

def generate_cost_matrix(model_type='composite', aggregation='mean', output_file='data/cost_matrix.csv'):
    """
    Generate and save a cost matrix in csv format for use with Snowflake model.
    
    Args:
        model_type (str): Type of cost model to use
        aggregation (str): How to aggregate multiple data points
        output_file (str): Path to save the pickled cost matrix
        
    Returns:
        pd.DataFrame: The generated cost matrix
    """
    
    # Generate the cost matrix
    cost_matrix = generate_cost_matrix_from_data(model_type=model_type, aggregation=aggregation)
    
    # Save to csv file
    cost_matrix.to_csv(output_file, index=False)
    
    print(f"\nCost matrix saved to: {output_file}")
    print(f"To use with Snowflake model, specify: cost_matrix_file='{output_file}'")
    
    return cost_matrix

