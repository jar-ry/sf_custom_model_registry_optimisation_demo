"""
Snowflake Feature Store Integration for Transportation Cost Matrix
================================================================

This module provides feature store integration for tracking cost matrix changes
over time using Snowflake's Feature Store. Simplified version for demo purposes.

NOTE: This implementation uses timestamps for point-in-time lookups and auditability.
For an even simpler demo without timestamps, you could modify the feature view to 
remove timestamp_col, but you'd lose historical analysis capabilities.
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import logging

from snowflake.ml.feature_store import FeatureStore, FeatureView, Entity
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, current_timestamp, to_timestamp
from snowflake.snowpark.types import StructType, StructField, StringType, FloatType, TimestampType, IntegerType

from helper.snowflake_utils import get_feature_store, get_snowpark_session, is_running_in_snowflake

logger = logging.getLogger(__name__)


class TransportationFeatureStore:
    """
    Manages transportation cost matrix features in Snowflake Feature Store.
    Simplified version for demo purposes.
    
    Uses timestamps for:
    - Point-in-time feature lookups
    - Auditability and reproducibility  
    - Historical analysis capabilities
    """
    
    def __init__(self, database: str = None, schema: str = None):
        """
        Initialize the Transportation Feature Store manager.
        
        Args:
            database: Snowflake database name (optional)
            schema: Snowflake schema name (optional)
        """
        # Get session first (handles both local and Snowflake environments)
        try:
            self.session = get_snowpark_session()
        except Exception as e:
            logger.error(f"Failed to get Snowpark session: {e}")
            if is_running_in_snowflake():
                # In Snowflake, try to get active session directly
                try:
                    from snowflake.snowpark.context import get_active_session
                    self.session = get_active_session()
                    logger.info("✅ Retrieved active session directly")
                except Exception as session_error:
                    raise RuntimeError(f"Cannot initialize feature store in Snowflake environment. "
                                     f"Session error: {session_error}") from session_error
            else:
                raise RuntimeError(f"Cannot initialize feature store in local environment. "
                                 f"Check your Snowflake credentials. Error: {e}") from e
        
        # Determine database and schema based on environment
        in_snowflake = is_running_in_snowflake()
        logger.info(f"🌍 Execution environment: {'Snowflake' if in_snowflake else 'Local'}")
        
        if database is None or schema is None:
            # Try to get from current session context (when running in Snowflake)
            try:
                current_db = self.session.get_current_database()
                current_schema = self.session.get_current_schema()
                self.database = database or current_db
                self.schema = schema or current_schema
                logger.info(f"📍 Using session context: {self.database}.{self.schema}")
            except Exception as e:
                logger.warning(f"⚠️ Could not get database/schema from session: {e}")
                # Fall back to environment variables (when running locally)
                self.database = database or os.getenv('SNOWFLAKE_DATABASE')
                self.schema = schema or os.getenv('SNOWFLAKE_SCHEMA')
                logger.info(f"📍 Using environment variables: {self.database}.{self.schema}")
        else:
            self.database = database
            self.schema = schema
            logger.info(f"📍 Using provided parameters: {self.database}.{self.schema}")
        
        # Validate database and schema
        if not self.database or not self.schema:
            raise ValueError(f"Database and schema must be specified. "
                           f"Got database='{self.database}', schema='{self.schema}'. "
                           f"Please provide them explicitly or set SNOWFLAKE_DATABASE and SNOWFLAKE_SCHEMA environment variables.")
        
        # Initialize feature store with detected/provided database and schema
        try:
            self.fs = get_feature_store(self.database, self.schema)
        except Exception as e:
            logger.error(f"Failed to initialize feature store: {e}")
            raise RuntimeError(f"Could not initialize feature store for {self.database}.{self.schema}: {e}") from e
        
        # Entity for route-based features
        self.route_entity = Entity(
            name="route",
            join_keys=["warehouse", "customer"],
            desc="Transportation route entity (warehouse-customer pair)"
        )
        
    def setup_feature_views(self):
        """
        Set up feature view for transportation cost matrix tracking.
        Simplified version with only the essential cost matrix feature view.
        """
        logger.info("Setting up transportation feature store...")
        
        # First, register the entity
        logger.info("Registering route entity...")
        self.fs.register_entity(self.route_entity)
        logger.info("✅ Route entity registered successfully")
        
        # Then create the feature view that depends on the entity
        logger.info("Setting up cost matrix feature view...")
        self._create_cost_matrix_fv()
        
        logger.info("✅ Feature view created successfully")
    
    def _create_cost_matrix_fv(self):
        """Create feature view for aggregated cost matrix."""
        
        # SQL for cost matrix calculation (reads directly from transportation_data table)
        cost_matrix_query = """
        SELECT 
            warehouse,
            customer,
            -- Distance-based costs
            AVG(distance_km * base_rate_per_km * road_condition_factor) as avg_distance_cost,
            
            -- Fuel costs
            AVG((distance_km / (8 * (vehicle_capacity_tons / 10) * (1 / road_condition_factor))) 
                * fuel_price_per_liter) as avg_fuel_cost,
            
            -- Time costs  
            AVG(travel_time_hours * 25 * 0.3) as avg_time_cost,
            
            -- Capacity factors
            AVG(10 / vehicle_capacity_tons) as avg_capacity_factor,
            
            -- Environmental factors
            AVG(seasonal_factor) as avg_seasonal_factor,
            AVG(priority_multiplier) as avg_priority_factor,
            
            -- Final composite cost
            AVG((distance_km * base_rate_per_km * road_condition_factor + 
                 (distance_km / (8 * (vehicle_capacity_tons / 10) * (1 / road_condition_factor))) * fuel_price_per_liter +
                 travel_time_hours * 25 * 0.3) * 
                (10 / vehicle_capacity_tons) * seasonal_factor * priority_multiplier) as composite_cost,
                
            -- Statistical measures
            STDDEV(fuel_price_per_liter) as fuel_price_volatility,
            MIN(fuel_price_per_liter) as min_fuel_price,
            MAX(fuel_price_per_liter) as max_fuel_price,
            
            COUNT(*) as sample_count,
            -- Timestamp for point-in-time lookups and auditability
            MAX(last_updated) as feature_timestamp
            
        FROM transportation_data 
        WHERE last_updated >= DATEADD('day', -30, CURRENT_TIMESTAMP())
        GROUP BY warehouse, customer
        """
        
        cost_matrix_df = self.session.sql(cost_matrix_query)
        
        cost_matrix_fv = FeatureView(
            name="cost_matrix_features",
            entities=[self.route_entity],
            feature_df=cost_matrix_df,
            # Timestamp column enables point-in-time lookups
            # Remove this line for a timestamp-free feature view (loses historical capabilities)
            timestamp_col="feature_timestamp",
            desc="Aggregated cost matrix features with statistical measures"
        )
        
        self.fs.register_feature_view(
            feature_view=cost_matrix_fv,
            version="1.0",
            overwrite=True
        )
        
        logger.info("✅ Created cost_matrix_features feature view")
    
    def get_latest_cost_matrix(self, feature_timestamp: Optional[datetime] = None) -> pd.DataFrame:
        """
        Get the latest cost matrix from feature store.
        
        Args:
            feature_timestamp: Optional timestamp for point-in-time lookup
                              If None, gets the latest available features
            
        Returns:
            DataFrame: Cost matrix with warehouses as rows, customers as columns
        """
        # Create entities dataframe for the route combinations we need
        entities_df = self.session.create_dataframe([
            ["Warehouse_A", "Customer_1"],
            ["Warehouse_A", "Customer_2"],
            ["Warehouse_B", "Customer_1"],
            ["Warehouse_B", "Customer_2"]
        ], schema=["warehouse", "customer"])
        # Get the feature view
        cost_matrix_fv = self.fs.get_feature_view("cost_matrix_features", "1.0")
        # Retrieve features using the feature store
        retrieved_features = self.fs.retrieve_feature_values(
            spine_df=entities_df,
            features=[cost_matrix_fv]
        )
        # Convert to pandas and create cost matrix
        cost_matrix_df = retrieved_features.to_pandas()
        
        # Create pivot table for cost matrix format
        cost_matrix = cost_matrix_df.pivot(
            index='WAREHOUSE', 
            columns='CUSTOMER', 
            values='COMPOSITE_COST'
        )
        
        return cost_matrix


def setup_transportation_feature_store(database: str = None, schema: str = None) -> TransportationFeatureStore:
    """
    Initialize and set up the transportation feature store.
    
    Note: This assumes the transportation_data table already exists in Snowflake.
    Run the SQL script sql/setup_transportation_table.sql first to create the table.
    
    Args:
        database: Snowflake database name
        schema: Snowflake schema name
        
    Returns:
        TransportationFeatureStore: Configured feature store manager
    """
    logger.info("Setting up Transportation Feature Store...")
    
    fs_manager = TransportationFeatureStore(database, schema)
    
    # Set up feature view (assumes transportation_data table exists)
    fs_manager.setup_feature_views()
    
    logger.info("✅ Transportation Feature Store setup complete!")
    return fs_manager
