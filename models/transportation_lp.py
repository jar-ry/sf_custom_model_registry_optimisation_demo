"""
Transportation Linear Programming Model
=====================================

This module implements a simple transportation problem using linear programming.
The objective is to minimize transportation costs while satisfying:
1. Warehouse capacity constraints
2. Customer demand constraints

Problem Structure:
- Decision Variables: x_ij = units shipped from warehouse i to customer j
- Objective: Minimize total transportation cost
- Constraints: 
  * Warehouse capacity limits
  * Customer demand requirements
"""

import pandas as pd
import json
from pulp import *


class TransportationLP:
    def __init__(self, data_file, config_file):
        """
        Initialize the transportation LP model
        
        Args:
            data_file (str): Path to CSV file with transportation costs
            config_file (str): Path to JSON file with constraints
        """
        self.data_file = data_file
        self.config_file = config_file
        self.model = None
        self.variables = {}
        self.load_data()
        self.load_config()
    
    def load_data(self):
        """Load transportation cost data from CSV"""
        self.cost_data = pd.read_csv(self.data_file)
        # Get unique cost per route (remove duplicates for demo simplicity)
        self.cost_matrix = self.cost_data.drop_duplicates().pivot(
            index='warehouse', 
            columns='customer', 
            values='cost_per_unit'
        )
        print("Transportation Cost Matrix:")
        print(self.cost_matrix)
        print()
    
    def load_config(self):
        """Load constraint configuration from JSON"""
        with open(self.config_file, 'r') as f:
            self.config = json.load(f)
        
        print("Constraint Configuration:")
        print(f"Warehouse Capacities: {self.config['warehouses']}")
        print(f"Customer Demands: {self.config['customers']}")
        print()
    
    def create_model(self):
        """Create the linear programming model"""
        # Initialize the model
        self.model = LpProblem("Transportation_Optimization", LpMinimize)
        
        # Get warehouses and customers
        warehouses = list(self.cost_matrix.index)
        customers = list(self.cost_matrix.columns)
        
        # Create decision variables
        # x[i][j] = units shipped from warehouse i to customer j
        self.variables = {}
        for warehouse in warehouses:
            self.variables[warehouse] = {}
            for customer in customers:
                var_name = f"x_{warehouse}_{customer}"
                self.variables[warehouse][customer] = LpVariable(
                    var_name, 
                    lowBound=0, 
                    cat='Continuous'
                )
        
        # Objective function: Minimize total transportation cost
        objective = 0
        for warehouse in warehouses:
            for customer in customers:
                cost = self.cost_matrix.loc[warehouse, customer]
                objective += cost * self.variables[warehouse][customer]
        
        self.model += objective, "Total_Transportation_Cost"
        
        # Constraint 1: Warehouse capacity constraints
        print("Adding Constraint 1: Warehouse Capacity Limits")
        for warehouse in warehouses:
            capacity = self.config['warehouses'][warehouse]['capacity']
            constraint = 0
            for customer in customers:
                constraint += self.variables[warehouse][customer]
            
            self.model += constraint <= capacity, f"Capacity_{warehouse}"
            print(f"  {warehouse}: Total shipments <= {capacity}")
        
        print()
        
        # Constraint 2: Customer demand constraints  
        print("Adding Constraint 2: Customer Demand Requirements")
        for customer in customers:
            demand = self.config['customers'][customer]['demand']
            constraint = 0
            for warehouse in warehouses:
                constraint += self.variables[warehouse][customer]
            
            self.model += constraint >= demand, f"Demand_{customer}"
            print(f"  {customer}: Total deliveries >= {demand}")
        
        print()
    
    def solve(self):
        """Solve the linear programming model"""
        if self.model is None:
            self.create_model()
        
        print("Solving the Transportation LP Model...")
        print("=" * 50)
        
        # Solve the model
        self.model.solve()
        
        # Display results
        print(f"Status: {LpStatus[self.model.status]}")
        print(f"Optimal Total Cost: ${value(self.model.objective):.2f}")
        print()
        
        if self.model.status == LpStatusOptimal:
            print("Optimal Shipment Plan:")
            print("-" * 30)
            
            warehouses = list(self.cost_matrix.index)
            customers = list(self.cost_matrix.columns)
            
            for warehouse in warehouses:
                print(f"\n{warehouse}:")
                warehouse_total = 0
                for customer in customers:
                    quantity = self.variables[warehouse][customer].varValue
                    if quantity > 0:
                        cost = self.cost_matrix.loc[warehouse, customer]
                        total_cost = quantity * cost
                        print(f"  → {customer}: {quantity:.1f} units @ ${cost}/unit = ${total_cost:.2f}")
                        warehouse_total += quantity
                print(f"  Total from {warehouse}: {warehouse_total:.1f} units")
            
            print("\nCustomer Fulfillment:")
            print("-" * 20)
            for customer in customers:
                customer_total = 0
                for warehouse in warehouses:
                    customer_total += self.variables[warehouse][customer].varValue
                demand = self.config['customers'][customer]['demand']
                print(f"{customer}: {customer_total:.1f} units received (demand: {demand})")
        
        return self.model.status == LpStatusOptimal
    
    def get_solution_summary(self):
        """Return a summary of the solution"""
        if self.model is None or self.model.status != LpStatusOptimal:
            return None
        
        summary = {
            'total_cost': value(self.model.objective),
            'shipments': {},
            'warehouse_utilization': {},
            'customer_satisfaction': {}
        }
        
        warehouses = list(self.cost_matrix.index)
        customers = list(self.cost_matrix.columns)
        
        # Get shipment details
        for warehouse in warehouses:
            summary['shipments'][warehouse] = {}
            warehouse_total = 0
            for customer in customers:
                quantity = self.variables[warehouse][customer].varValue
                summary['shipments'][warehouse][customer] = quantity
                warehouse_total += quantity
            
            capacity = self.config['warehouses'][warehouse]['capacity']
            summary['warehouse_utilization'][warehouse] = {
                'used': warehouse_total,
                'capacity': capacity,
                'utilization_rate': warehouse_total / capacity
            }
        
        # Get customer satisfaction
        for customer in customers:
            customer_total = 0
            for warehouse in warehouses:
                customer_total += self.variables[warehouse][customer].varValue
            
            demand = self.config['customers'][customer]['demand']
            summary['customer_satisfaction'][customer] = {
                'received': customer_total,
                'demand': demand,
                'satisfaction_rate': customer_total / demand
            }
        
        return summary
