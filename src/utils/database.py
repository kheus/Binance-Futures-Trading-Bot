"""
Database connection and query utilities.
Provides a singleton connection pool and query execution methods.
"""
import psycopg2
from psycopg2 import pool
import logging
from typing import Optional, List, Dict, Any, Union, Tuple
import yaml
from pathlib import Path
from functools import wraps
import time
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log
)

logger = logging.getLogger(__name__)

class DatabaseManager:
    """Singleton class to manage database connections and queries."""
    _instance = None
    _pool = None
    _config_loaded = False
    _initialized = False
    
    # Default configuration
    _default_config = {
        "min_connections": 2,
        "max_connections": 20,
        "connect_timeout": 10,
        "retry_attempts": 3,
        "retry_wait": 1,
        "max_retry_wait": 10,
    }
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabaseManager, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not self._initialized:
            self._initialize()
            self._initialized = True
    
    def _initialize(self):
        """Initialize the database connection pool."""
        if not self._config_loaded:
            self._load_config()
        
        try:
            if self._pool is not None:
                self._pool.closeall()
                
            self._pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=self._config["min_connections"],
                maxconn=self._config["max_connections"],
                **self._db_params
            )
            logger.info("Database connection pool initialized")
        except Exception as e:
            logger.error(f"Failed to initialize database connection pool: {e}")
            raise
    
    def _load_config(self, config_path: Optional[str] = None):
        """Load database configuration from file."""
        if config_path is None:
            config_path = Path(__file__).parent.parent.parent / "config" / "db_config.yaml"
        
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            
            db_config = config.get("database", {}).get("postgresql", {})
            
            self._db_params = {
                "host": db_config.get("host", "localhost"),
                "database": db_config.get("database", "trading_bot"),
                "user": db_config.get("user", "postgres"),
                "password": db_config.get("password", ""),
                "port": db_config.get("port", 5432),
                "connect_timeout": self._config["connect_timeout"]
            }
            
            # Update config with any overrides
            self._config.update({
                k: v for k, v in db_config.items() 
                if k in self._config
            })
            
            self._config_loaded = True
            logger.debug("Database configuration loaded")
            
        except Exception as e:
            logger.error(f"Failed to load database configuration: {e}")
            raise
    
    def get_connection(self):
        """Get a database connection from the pool."""
        if not self._pool:
            self._initialize()
        
        try:
            conn = self._pool.getconn()
            conn.autocommit = False
            return conn
        except Exception as e:
            logger.error(f"Failed to get database connection: {e}")
            raise
    
    def release_connection(self, conn):
        """Release a connection back to the pool."""
        if conn:
            try:
                if not conn.closed:
                    conn.rollback()  # Always rollback any pending transactions
                    self._pool.putconn(conn)
            except Exception as e:
                logger.error(f"Error releasing connection: {e}")
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((psycopg2.OperationalError, psycopg2.InterfaceError)),
        reraise=True
    )
    def execute_query(
        self, 
        query: str, 
        params: Optional[Union[tuple, dict]] = None, 
        fetch: bool = False,
        return_last_id: bool = False
    ) -> Union[None, List[tuple], Any]:
        """
        Execute a database query with retry logic.
        
        Args:
            query: SQL query string
            params: Query parameters
            fetch: Whether to fetch results
            return_last_id: Whether to return the last inserted ID
            
        Returns:
            Query results or None
        """
        conn = None
        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            logger.debug(f"Executing query: {query[:100]}...")
            cursor.execute(query, params)
            
            if fetch:
                result = cursor.fetchall()
            elif return_last_id:
                cursor.execute("SELECT LASTVAL()")
                result = cursor.fetchone()[0]
            else:
                conn.commit()
                return None
                
            conn.commit()
            return result
            
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Query failed: {e}\nQuery: {query}\nParams: {params}")
            raise
            
        finally:
            if cursor:
                cursor.close()
            self.release_connection(conn)
    
    def execute_transaction(self, queries: List[Tuple[str, Union[tuple, dict]]]) -> bool:
        """
        Execute multiple queries in a single transaction.
        
        Args:
            queries: List of (query, params) tuples
            
        Returns:
            bool: True if all queries executed successfully
        """
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                for query, params in queries:
                    cursor.execute(query, params)
            conn.commit()
            return True
            
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Transaction failed: {e}")
            return False
            
        finally:
            self.release_connection(conn)

# Global instance
db = DatabaseManager()

# Backward compatibility
execute_query = db.execute_query
execute_transaction = db.execute_transaction
get_db_connection = db.get_connection
release_db_connection = db.release_connection
