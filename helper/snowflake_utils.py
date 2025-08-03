"""
Snowflake utilities for MLOps framework.

This module provides utilities for establishing Snowflake connections,
initializing Feature Store instances, and managing authentication.
"""

import os
import logging
from typing import Optional, Dict, Any
import snowflake.connector
from snowflake.snowpark import Session
from snowflake.ml.feature_store import FeatureStore, CreationMode
from snowflake.ml.registry import Registry
from snowflake.connector import DictCursor

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SnowflakeManager:
    """
    Manages Snowflake connections and ML services (Feature Store, Model Registry).
    
    Supports Container Session Inheritance:
    - When running in Snowflake containers, uses the current session context
    - When running locally, creates new connections with explicit authentication
    """
    
    def __init__(self):
        self._connection = None
        self._session = None
        self._feature_store = None
        self._model_registry = None    
    
    def get_connection(self) -> snowflake.connector.SnowflakeConnection:
        """
        Establish and return a Snowflake connection.
        
        Uses Container Session Inheritance when running in Snowflake containers,
        otherwise creates new connection with explicit authentication.
        
        Returns:
            snowflake.connector.SnowflakeConnection: Active Snowflake connection
        """
        if self._connection is None:
            # Local mode - explicit authentication required
            self._connection = self._create_explicit_connection()
        
        return self._connection
    
    def get_session(self) -> Session:
        """
        Establish and return a Snowpark Session for ML services.
        
        Handles both local and Snowflake execution environments:
        - In Snowflake: Uses the active session context
        - Locally: Creates new session with explicit authentication
        
        Returns:
            Session: Active Snowpark Session
        """
        if self._session is None:
            # First try to get active session (when running in Snowflake)
            try:
                from snowflake.snowpark.context import get_active_session
                self._session = get_active_session()
                logger.info("✅ Using active Snowflake session context")
                return self._session
            except ImportError:
                logger.info("🔧 Snowpark context not available, creating new session")
            except Exception as e:
                logger.info(f"🔧 No active session found ({type(e).__name__}: {e})")
                
                # Check if we're in a Snowflake execution environment
                # If so, we should not try to create a new session with credentials
                try:
                    # Additional check for Snowflake environment indicators
                    import os
                    if any(key.startswith('SNOWFLAKE_') for key in os.environ if 'WAREHOUSE' in key or 'CLUSTER' in key):
                        raise RuntimeError("Running in Snowflake environment but cannot access active session. "
                                         "This might indicate a session context issue.")
                except:
                    pass
                
            # Only create new session if we're running locally
            logger.info("🔧 Creating new Snowpark session for local execution")
            try:
                self._session = self._create_snowpark_session()
            except ValueError as e:
                if "No authentication method available" in str(e):
                    raise RuntimeError("Cannot create session: No authentication available. "
                                     "When running in Snowflake, ensure session context is available. "
                                     "When running locally, provide SNOWFLAKE_PASSWORD or SNOWFLAKE_PRIVATE_KEY_PATH.") from e
                raise
        
        return self._session
    
    def _create_snowpark_session(self) -> Session:
        """
        Create a new Snowpark Session for ML services.
        
        Returns:
            Session: New Snowpark Session
        """
        try:
            connection_params = {
                'user': os.getenv('SNOWFLAKE_USER'),
                'account': os.getenv('SNOWFLAKE_ACCOUNT'),
                'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
                'database': os.getenv('SNOWFLAKE_DATABASE'),
                'schema': os.getenv('SNOWFLAKE_SCHEMA'),
                'role': os.getenv('SNOWFLAKE_ROLE', 'MLOPS_ROLE')
            }
            
            # Check for key-pair authentication first (best practice)
            private_key_path = os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH')
            if private_key_path and os.path.exists(private_key_path):
                from cryptography.hazmat.primitives import serialization
                with open(private_key_path, 'rb') as key_file:
                    private_key = serialization.load_pem_private_key(
                        key_file.read(),
                        password=os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE', '').encode() or None,
                    )
                connection_params['private_key'] = private_key
                logger.info("🔐 Using key-pair authentication for Snowpark Session")
            else:
                # Fall back to password authentication
                password = os.getenv('SNOWFLAKE_PASSWORD')
                if not password:
                    raise ValueError("No authentication method available. Please provide either SNOWFLAKE_PASSWORD or SNOWFLAKE_PRIVATE_KEY_PATH")
                connection_params['password'] = password
                logger.info("🔐 Using password authentication for Snowpark Session")
            
            session = Session.builder.configs(connection_params).create()
            logger.info(f"✅ Created Snowpark Session: {connection_params['account']}")
            return session
            
        except Exception as e:
            logger.error(f"❌ Failed to create Snowpark Session: {str(e)}")
            raise
    
    def _create_explicit_connection(self) -> snowflake.connector.SnowflakeConnection:
        """
        Create a new Snowflake connection with explicit authentication.
        
        Returns:
            snowflake.connector.SnowflakeConnection: New authenticated connection
        """
        try:
            connection_params = {
                'user': os.getenv('SNOWFLAKE_USER'),
                'account': os.getenv('SNOWFLAKE_ACCOUNT'),
                'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
                'database': os.getenv('SNOWFLAKE_DATABASE'),
                'schema': os.getenv('SNOWFLAKE_SCHEMA'),
                'role': os.getenv('SNOWFLAKE_ROLE', 'MLOPS_ROLE')
            }
            
            # Check for key-pair authentication first (best practice)
            private_key_path = os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH')
            if private_key_path and os.path.exists(private_key_path):
                from cryptography.hazmat.primitives import serialization
                with open(private_key_path, 'rb') as key_file:
                    private_key = serialization.load_pem_private_key(
                        key_file.read(),
                        password=os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE', '').encode() or None,
                    )
                connection_params['private_key'] = private_key
                logger.info("🔐 Using key-pair authentication")
            else:
                # Fall back to password authentication
                password = os.getenv('SNOWFLAKE_PASSWORD')
                if not password:
                    raise ValueError("No authentication method available. Please provide either SNOWFLAKE_PASSWORD or SNOWFLAKE_PRIVATE_KEY_PATH")
                connection_params['password'] = password
                logger.info("🔐 Using password authentication")
            
            connection = snowflake.connector.connect(**connection_params)
            logger.info(f"✅ Connected to Snowflake: {connection_params['account']}")
            return connection
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to Snowflake: {str(e)}")
            raise
    
    def get_feature_store(self, database: Optional[str] = None, 
                         schema: Optional[str] = None) -> FeatureStore:
        """
        Initialize and return a Feature Store instance.
        
        Args:
            database: Optional database name (uses current/env if not provided)
            schema: Optional schema name (uses current/env if not provided)
            
        Returns:
            FeatureStore: Initialized Feature Store instance
        """
        if self._feature_store is None:
            # Use Snowpark Session for ML services
            session = self.get_session()
            
            # Determine database and schema
            fs_database = database
            fs_schema = schema
            
            if fs_database is None or fs_schema is None:
                # Try to get from session context first
                try:
                    if fs_database is None:
                        fs_database = session.get_current_database()
                    if fs_schema is None:
                        fs_schema = session.get_current_schema()
                    logger.info(f"🔧 Retrieved from session context: {fs_database}.{fs_schema}")
                except Exception as e:
                    logger.info(f"🔧 Could not get from session context ({type(e).__name__}), using environment variables")
                    # Fall back to environment variables
                    if fs_database is None:
                        fs_database = os.getenv('SNOWFLAKE_DATABASE')
                    if fs_schema is None:
                        fs_schema = os.getenv('SNOWFLAKE_SCHEMA')
                    logger.info(f"🔧 Retrieved from environment: {fs_database}.{fs_schema}")
            
            if not fs_database or not fs_schema:
                raise ValueError(f"Database and schema must be specified. Got database='{fs_database}', schema='{fs_schema}'")
                
            logger.info(f"🔧 Creating Feature Store in: {fs_database}.{fs_schema}")
            self._feature_store = FeatureStore(
                session=session,
                database=fs_database,
                name=fs_schema,
                default_warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
                creation_mode=CreationMode.CREATE_IF_NOT_EXIST
            )
            logger.info(f"✅ Initialized Feature Store: {fs_database}.{fs_schema}")
        
        return self._feature_store
    
    def get_model_registry(self, database: Optional[str] = None, 
                          schema: Optional[str] = None) -> Registry:
        """
        Initialize and return a Model Registry instance.
        
        Args:
            database: Optional database name (uses env var if not provided)
            schema: Optional schema name (uses env var if not provided)
            
        Returns:
            Registry: Initialized Model Registry instance
        """
        if self._model_registry is None:
            # Use Snowpark Session for ML services
            session = self.get_session()
            reg_database = database or os.getenv('SNOWFLAKE_DATABASE')
            reg_schema = schema or os.getenv('SNOWFLAKE_SCHEMA')
            logger.info(f"✅ Initializing Model Registry: {reg_database}.{reg_schema}")
            self._model_registry = Registry(
                session=session,
                database_name=reg_database,
                schema_name=reg_schema
            )
            logger.info(f"✅ Initialized Model Registry: {reg_database}.{reg_schema}")
        
        return self._model_registry
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Execute a SQL query and return results.
        
        Args:
            query: SQL query to execute
            params: Optional query parameters
            
        Returns:
            Dict containing query results
        """
        connection = self.get_connection()
        cursor = connection.cursor(DictCursor)
        
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            results = cursor.fetchall()
            logger.info(f"Query executed successfully, returned {len(results)} rows")
            return {
                'success': True,
                'data': results,
                'row_count': len(results)
            }
            
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'data': None
            }
        finally:
            cursor.close()
    
    def close_connection(self):
        """Close the Snowflake connection and session."""
        if self._session:
            self._session.close()
            self._session = None
            logger.info("Snowpark session closed")
            
        if self._connection:
            self._connection.close()
            self._connection = None
            logger.info("Snowflake connection closed")
            
        self._feature_store = None
        self._model_registry = None


# Global instance for easy access
snowflake_manager = SnowflakeManager()


def is_running_in_snowflake() -> bool:
    """
    Detect if code is running within Snowflake execution environment.
    
    Uses multiple detection methods:
    1. Active session availability
    2. Environment variables that indicate Snowflake context
    3. Python execution context indicators
    
    Returns:
        bool: True if running in Snowflake, False if running locally
    """
    # Method 1: Try to get active session
    try:
        from snowflake.snowpark.context import get_active_session
        get_active_session()
        return True
    except ImportError:
        pass
    except:
        pass
    
    # Method 2: Check for Snowflake-specific environment variables
    import os
    snowflake_env_indicators = [
        'SNOWFLAKE_WAREHOUSE_ID',
        'SNOWFLAKE_CLUSTER_ID', 
        'SNOWFLAKE_SESSION_ID',
        'SNOWFLAKE_QUERY_ID',
        'SNOWFLAKE_ACCOUNT_LOCATOR'
    ]
    
    if any(os.getenv(key) for key in snowflake_env_indicators):
        return True
        
    # Method 3: Check execution context (UDF, stored procedure, etc.)
    try:
        import sys
        # In Snowflake UDFs, the module path often contains snowflake-specific paths
        if any('snowflake' in str(path).lower() for path in sys.path):
            return True
    except:
        pass
        
    return False


def get_snowflake_connection() -> snowflake.connector.SnowflakeConnection:
    """Get the global Snowflake connection."""
    return snowflake_manager.get_connection()


def get_snowpark_session() -> Session:
    """Get the global Snowpark Session."""
    return snowflake_manager.get_session()


def get_feature_store(database: Optional[str] = None, 
                     schema: Optional[str] = None) -> FeatureStore:
    """Get the global Feature Store instance."""
    return snowflake_manager.get_feature_store(database, schema)


def get_model_registry(database: Optional[str] = None, 
                      schema: Optional[str] = None) -> Registry:
    """Get the global Model Registry instance."""
    return snowflake_manager.get_model_registry(database, schema) 